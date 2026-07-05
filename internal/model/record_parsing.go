package model

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// ParseAttributesJSON parses JSON-aggregated EAV attributes into the record
func ParseAttributesJSON(attrsJSON []byte, record *PersistentRecord) error {
	if len(attrsJSON) == 0 || string(attrsJSON) == "[]" {
		return nil
	}

	var attributes []map[string]any
	if err := json.Unmarshal(attrsJSON, &attributes); err != nil {
		return fmt.Errorf("unmarshal attributes json: %w", err)
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

// ParseEAVAttribute converts a JSON object to an EAVRecord
func ParseEAVAttribute(attrObj map[string]any) (EAVRecord, error) {
	schemaIDRaw, ok := attrObj["schema_id"].(float64)
	if !ok {
		return EAVRecord{}, fmt.Errorf("schema_id is missing or not a number: %v", attrObj["schema_id"])
	}
	attrIDRaw, ok := attrObj["attr_id"].(float64)
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
	if valueNumeric, ok := attrObj["value_numeric"].(float64); ok {
		attr.ValueNumeric = &valueNumeric
	}

	return attr, nil
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
