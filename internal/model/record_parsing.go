package model

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"

	"github.com/google/uuid"
)

// ParseAttributesJSON parses JSON-aggregated EAV attributes into the record.
//
// Numbers are decoded as json.Number (#592): the token Postgres or DuckDB
// emitted reaches the record exactly, in ValueNumericRaw, beside the float64
// image every consumer of ValueNumeric reads. A decode into float64 rounded
// the token before any value-type rule could judge it (9007199254740993 read
// as 9007199254740992).
func ParseAttributesJSON(attrsJSON []byte, record *PersistentRecord) error {
	if len(attrsJSON) == 0 || string(attrsJSON) == "[]" {
		return nil
	}

	decoder := json.NewDecoder(bytes.NewReader(attrsJSON))
	decoder.UseNumber()
	var attributes []map[string]any
	if err := decoder.Decode(&attributes); err != nil {
		return fmt.Errorf("unmarshal attributes json: %w", err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("unmarshal attributes json: unexpected data after the attributes array")
	}

	record.OtherAttributes = make([]EAVRecord, 0, len(attributes))
	for _, attrObj := range attributes {
		attr, err := ParseEAVAttribute(attrObj)
		if err != nil {
			return fmt.Errorf("parse eav attribute: %w", err)
		}
		record.OtherAttributes = append(record.OtherAttributes, attr)
	}

	return nil
}

// ParseEAVAttribute converts a JSON object to an EAVRecord. Numbers may be
// float64 (a caller-built map) or json.Number (ParseAttributesJSON).
func ParseEAVAttribute(attrObj map[string]any) (EAVRecord, error) {
	schemaIDRaw, ok := jsonNumberField(attrObj["schema_id"])
	if !ok {
		return EAVRecord{}, fmt.Errorf("schema_id is missing or not a number: %v", attrObj["schema_id"])
	}
	attrIDRaw, ok := jsonNumberField(attrObj["attr_id"])
	if !ok {
		return EAVRecord{}, fmt.Errorf("attr_id is missing or not a number: %v", attrObj["attr_id"])
	}
	attr := EAVRecord{
		SchemaID: int16(schemaIDRaw),
		AttrID:   int16(attrIDRaw),
	}

	if rowIDStr, ok := attrObj["row_id"].(string); ok {
		if parsedUUID, err := uuid.Parse(rowIDStr); err == nil {
			attr.RowID = parsedUUID
		}
	}

	if indices, ok := attrObj["array_indices"].(string); ok {
		attr.ArrayIndices = indices
	}

	if valueText, ok := attrObj["value_text"].(string); ok {
		attr.ValueText = &valueText
	}
	if err := parseValueNumeric(attrObj["value_numeric"], &attr); err != nil {
		return EAVRecord{}, fmt.Errorf("attr_id %d of schema %d (row %s): %w", attr.AttrID, attr.SchemaID, attr.RowID, err)
	}

	return attr, nil
}

// jsonNumberField reads an id field. The ids are smallint columns, which
// both engines emit as plain integers; a float64 is a caller-built map.
func jsonNumberField(value any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case json.Number:
		f, err := v.Float64()
		return f, err == nil
	}
	return 0, false
}

// nonFiniteNumericTokens are the spellings Postgres JSON_BUILD_OBJECT gives
// a NUMERIC or DOUBLE PRECISION NaN or infinity: JSON strings, since JSON
// has no number for them.
var nonFiniteNumericTokens = map[string]float64{
	"NaN":       math.NaN(),
	"Infinity":  math.Inf(1),
	"-Infinity": math.Inf(-1),
}

// parseValueNumeric fills ValueNumeric and, for a decoded token,
// ValueNumericRaw. It preserves what was stored and applies no value-type
// rule (#592): a token past 2^53, a fraction, a magnitude past float64 (its
// image is the infinity ParseFloat rounds it to) and the non-finite strings
// all reach the transform layer, which judges them against the attribute's
// contract. Before, a non-finite string read as an absent attribute, and a
// token past float64 failed the whole row's decode.
func parseValueNumeric(value any, attr *EAVRecord) error {
	switch v := value.(type) {
	case nil:
		return nil
	case float64:
		attr.ValueNumeric = &v
		return nil
	case json.Number:
		image, err := strconv.ParseFloat(v.String(), 64)
		if err != nil && !errors.Is(err, strconv.ErrRange) {
			return fmt.Errorf("value_numeric token %q is not a number: %w", v.String(), err)
		}
		attr.ValueNumeric = &image
		attr.ValueNumericRaw = v.String()
		return nil
	case string:
		image, ok := nonFiniteNumericTokens[v]
		if !ok {
			return fmt.Errorf("value_numeric is the string %q, which names no number", v)
		}
		attr.ValueNumeric = &image
		attr.ValueNumericRaw = v
		return nil
	}
	return fmt.Errorf("value_numeric is a %T, not a number", value)
}

// CleanupEmptyMaps removes empty maps from the record to avoid nil-map checks
func CleanupEmptyMaps(record *PersistentRecord) {
	if len(record.TextItems) == 0 {
		record.TextItems = nil
	}
	if len(record.Int16Items) == 0 {
		record.Int16Items = nil
	}
	if len(record.Int32Items) == 0 {
		record.Int32Items = nil
	}
	if len(record.Int64Items) == 0 {
		record.Int64Items = nil
	}
	if len(record.Float64Items) == 0 {
		record.Float64Items = nil
	}
	if len(record.UUIDItems) == 0 {
		record.UUIDItems = nil
	}
}
