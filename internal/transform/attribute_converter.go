package transform

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/numutil"
	"github.com/lychee-technology/forma/internal/schemameta"
	"go.uber.org/zap"
)

// AttributeConverter provides conversion between model.EntityAttribute and model.EAVRecord
type AttributeConverter struct {
	registry forma.SchemaRegistry
}

// NewAttributeConverter creates a new AttributeConverter instance
func NewAttributeConverter(registry forma.SchemaRegistry) *AttributeConverter {
	return &AttributeConverter{
		registry: registry,
	}
}

// ToEAVRecord converts an model.EntityAttribute to an model.EAVRecord
func (c *AttributeConverter) ToEAVRecord(attr model.EntityAttribute, rowID uuid.UUID) (model.EAVRecord, error) {
	record := model.EAVRecord{
		SchemaID:     attr.SchemaID,
		RowID:        rowID,
		AttrID:       attr.AttrID,
		ArrayIndices: attr.ArrayIndices,
	}

	if attr.Value == nil {
		return record, nil
	}

	switch attr.ValueType {
	case forma.ValueTypeText:
		if strVal, ok := attr.Value.(string); ok {
			record.ValueText = &strVal
		} else {
			return record, fmt.Errorf("value type mismatch: expected string for text type")
		}

	case forma.ValueTypeSmallInt, forma.ValueTypeInteger, forma.ValueTypeBigInt, forma.ValueTypeNumeric:
		numVal, err := toFloat64ForEAV(attr.Value)
		if err != nil {
			return record, fmt.Errorf("convert to numeric: %w", err)
		}
		record.ValueNumeric = &numVal
		if attr.ValueType == forma.ValueTypeBigInt {
			if exact, ok := toInt64ExactForEAV(attr.Value); ok {
				record.ValueInt64 = &exact
			}
		}

	case forma.ValueTypeDate, forma.ValueTypeDateTime:
		timeVal, err := toTimeForEAV(attr.Value)
		if err != nil {
			return record, fmt.Errorf("convert to time: %w", err)
		}
		unixMillis := timeToUnixMillisFloat64(timeVal)
		record.ValueNumeric = &unixMillis
		exactMs := timeVal.UnixMilli()
		record.ValueInt64 = &exactMs

	case forma.ValueTypeUUID:
		if uuidVal, ok := attr.Value.(uuid.UUID); ok {
			strVal := uuidVal.String()
			record.ValueText = &strVal
		} else {
			return record, fmt.Errorf("value type mismatch: expected uuid.UUID for uuid type")
		}

	case forma.ValueTypeBool:
		boolVal, err := toBoolForEAV(attr.Value)
		if err != nil {
			return record, fmt.Errorf("convert to bool: %w", err)
		}
		floatBool := boolToFloat64(boolVal)
		record.ValueNumeric = &floatBool

	default:
		return record, fmt.Errorf("unsupported value type: %s", attr.ValueType)
	}

	return record, nil
}

// FromEAVRecord converts an model.EAVRecord to an model.EntityAttribute
func (c *AttributeConverter) FromEAVRecord(record model.EAVRecord, valueType forma.ValueType) (model.EntityAttribute, error) {
	attr := model.EntityAttribute{
		SchemaID:     record.SchemaID,
		RowID:        record.RowID,
		AttrID:       record.AttrID,
		ArrayIndices: record.ArrayIndices,
		ValueType:    valueType,
	}

	var err error
	attr.Value, err = extractValueFromEAVRecord(record, valueType)
	if err != nil {
		return attr, err
	}

	return attr, nil
}

// ToEAVRecords converts a slice of EntityAttributes to EAVRecords
func (c *AttributeConverter) ToEAVRecords(attributes []model.EntityAttribute, rowID uuid.UUID) ([]model.EAVRecord, error) {
	records := make([]model.EAVRecord, 0, len(attributes))
	for _, attr := range attributes {
		record, err := c.ToEAVRecord(attr, rowID)
		if err != nil {
			return nil, fmt.Errorf("convert attribute attrID=%d: %w", attr.AttrID, err)
		}
		records = append(records, record)
	}
	return records, nil
}

// FromEAVRecords converts a slice of EAVRecords to EntityAttributes
func (c *AttributeConverter) FromEAVRecords(records []model.EAVRecord) ([]model.EntityAttribute, error) {
	if len(records) == 0 {
		return []model.EntityAttribute{}, nil
	}

	// Get schema metadata to determine value types
	schemaID := records[0].SchemaID
	cache, idToName, err := schemameta.GetSchemaMetadata(c.registry, schemaID)
	if err != nil {
		return nil, err
	}

	presentAttrIndices := make(map[string]map[string]struct{}, len(records))

	attributes := make([]model.EntityAttribute, 0, len(records))
	for _, record := range records {
		attrName, ok := idToName[record.AttrID]
		if !ok {
			// Read-path unknown attribute IDs indicate metadata drift, not invalid user input.
			return nil, fmt.Errorf("unknown attribute id %d for schema %d (attribute not in metadata cache)", record.AttrID, record.SchemaID)
		}
		indexSet := presentAttrIndices[attrName]
		if indexSet == nil {
			indexSet = make(map[string]struct{})
			presentAttrIndices[attrName] = indexSet
		}
		indexSet[record.ArrayIndices] = struct{}{}

		meta := cache[attrName]
		vt := meta.ValueType
		if vt == forma.ValueTypeList {
			// List elements are stored one row per element; type each element
			// by the declared items type so downstream conversion (including
			// ToEAVRecord) sees a scalar type, never the container type (#204).
			vt = meta.EffectiveItemsType()
		}
		attr, err := c.FromEAVRecord(record, vt)
		if err != nil {
			return nil, fmt.Errorf("convert record attrID=%d: %w", record.AttrID, err)
		}
		attributes = append(attributes, attr)
	}

	missingRequired := make(map[int16]string)
	for attrName, metadata := range cache {
		switch metadata.EffectiveRequiredPolicy() {
		case forma.RequiredPolicyAlways:
			if isRequiredAttributeMissing(attrName, presentAttrIndices, true) {
				missingRequired[metadata.AttributeID] = attrName
			}
		case forma.RequiredPolicyIfParentPresent:
			if isRequiredAttributeMissing(attrName, presentAttrIndices, false) {
				missingRequired[metadata.AttributeID] = attrName
			}
		}
	}
	if len(missingRequired) > 0 {
		zap.S().Infow("missing EAV records for attrIDs.", "idToName", missingRequired)
		for missingAttrID, missingAttrName := range missingRequired {
			// Plain error, deliberately. FromEAVRecords is not write-only: the read
			// path rebuilds already-stored records through it
			// (persistent_record.go's FromPersistentRecord), so a persisted row
			// missing a required EAV row reaches here too. Wrapping
			// forma.ErrInvalidInput here — as an earlier #301 sweep did — made the
			// HTTP boundary answer that persisted-drift case with a verbatim 400,
			// inverting the split AGENTS.md and this repo's error-handling doc
			// draw: write validation carries the sentinel, read-path consistency
			// failures stay plain and operator-visible.
			//
			// The write path's 400 does not depend on this wrap. It has its own
			// write-only validator, validateRequiredAttributesFromInput
			// (transformer.go), which ToAttributes runs against the caller's input
			// before flattening and which does carry the sentinel.
			return nil, fmt.Errorf("missing required attribute '%s' (attrID=%d) in EAV records",
				missingAttrName, missingAttrID)
		}
	}

	return attributes, nil
}

// Helper functions for conversion

func toFloat64ForEAV(value any) (float64, error) {
	switch v := value.(type) {
	case *float64:
		return requiredFloat64FromPointer(v, "float64")
	case *float32:
		return requiredFloat64FromPointer(v, "float32")
	case *int:
		return requiredFloat64FromPointer(v, "int")
	case *int16:
		return requiredFloat64FromPointer(v, "int16")
	case *int32:
		return requiredFloat64FromPointer(v, "int32")
	case *int64:
		return requiredFloat64FromPointer(v, "int64")
	case string:
		return parseTrimmedFloat64(v)
	case *string:
		str, err := derefPointer(v, "string")
		if err != nil {
			return 0, err
		}
		return parseTrimmedFloat64(str)
	default:
		return numutil.Float64(value)
	}
}

func toTimeForEAV(value any) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case *time.Time:
		timeValue, err := derefPointer(v, "time")
		if err != nil {
			return time.Time{}, err
		}
		return timeValue, nil
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to time.Time", value)
	}
}

func toBoolForEAV(value any) (bool, error) {
	switch v := value.(type) {
	case string:
		return isTrueStringForEAV(v), nil
	case *string:
		str, err := derefPointer(v, "string")
		if err != nil {
			return false, err
		}
		return isTrueStringForEAV(str), nil
	case bool:
		return v, nil
	case *bool:
		booleanValue, err := derefPointer(v, "bool")
		if err != nil {
			return false, err
		}
		return booleanValue, nil
	case int:
		return boolFromPositive(v), nil
	case *int:
		num, err := derefPointer(v, "int")
		if err != nil {
			return false, err
		}
		return boolFromPositive(num), nil
	case int16:
		return boolFromPositive(v), nil
	case *int16:
		num, err := derefPointer(v, "int16")
		if err != nil {
			return false, err
		}
		return boolFromPositive(num), nil
	case int32:
		return boolFromNonZero(v), nil
	case *int32:
		num, err := derefPointer(v, "int32")
		if err != nil {
			return false, err
		}
		return boolFromPositive(num), nil
	case int64:
		return boolFromNonZero(v), nil
	case *int64:
		num, err := derefPointer(v, "int64")
		if err != nil {
			return false, err
		}
		return boolFromPositive(num), nil
	case float32:
		return float64ToBool(float64(v)), nil
	case *float32:
		num, err := derefPointer(v, "float32")
		if err != nil {
			return false, err
		}
		return float64ToBool(float64(num)), nil
	case float64:
		return float64ToBool(v), nil
	case *float64:
		num, err := derefPointer(v, "float64")
		if err != nil {
			return false, err
		}
		return float64ToBool(num), nil
	default:
		return false, fmt.Errorf("cannot convert %T to bool", value)
	}
}

func derefPointer[T any](value *T, typeName string) (T, error) {
	if value == nil {
		var zero T
		return zero, fmt.Errorf("nil %s pointer", typeName)
	}
	return *value, nil
}

func boolFromPositive[T ~int | ~int16 | ~int32 | ~int64](value T) bool {
	return value > 0
}

func boolFromNonZero[T ~int32 | ~int64](value T) bool {
	return value != 0
}

func isTrueStringForEAV(value string) bool {
	return strings.ToLower(value) == "true" || value == "1"
}

func parseTrimmedFloat64(value string) (float64, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, fmt.Errorf("empty string")
	}
	parsed, err := strconv.ParseFloat(trimmed, 64)
	if err != nil {
		return 0, fmt.Errorf("parse float: %w", err)
	}
	return parsed, nil
}

// shouldEnforceRequiredAttribute applies RequiredPolicyIfParentPresent semantics
// to an attribute using the observed EAV array-index context.
func shouldEnforceRequiredAttribute(attrName string, presentAttrIndices map[string]map[string]struct{}) bool {
	return isRequiredAttributeMissing(attrName, presentAttrIndices, false)
}

// isRequiredAttributeMissing reports whether a required attribute is missing.
//
// For nested attributes, the required check is contextual:
//   - RequiredPolicyAlways enforces the attribute even when its parent path is absent.
//   - RequiredPolicyIfParentPresent enforces the attribute only when its parent path
//     is present in the observed EAV records.
//   - Array-backed attributes must exist for every parent array index that is present.
func isRequiredAttributeMissing(attrName string, presentAttrIndices map[string]map[string]struct{}, enforceWhenParentMissing bool) bool {
	if indices, ok := presentAttrIndices[attrName]; ok && len(indices) > 0 {
		return parentIndexMissing(attrName, presentAttrIndices, indices, enforceWhenParentMissing)
	}

	parentPath, hasParent := attributeParentPath(attrName)
	if !hasParent {
		return true
	}

	parentIndices := collectParentIndices(parentPath, presentAttrIndices)
	if len(parentIndices) == 0 {
		return enforceWhenParentMissing
	}
	// The attribute is absent entirely while its parent context exists, so the
	// required attribute is missing for every observed parent context.
	return true
}

// parentIndexMissing verifies that a child attribute is present for every parent
// context that appears in the EAV records.
func parentIndexMissing(attrName string, presentAttrIndices map[string]map[string]struct{}, childIndices map[string]struct{}, enforceWhenParentMissing bool) bool {
	parentPath, hasParent := attributeParentPath(attrName)
	if !hasParent {
		return len(childIndices) == 0
	}

	parentIndices := collectParentIndices(parentPath, presentAttrIndices)
	if len(parentIndices) == 0 {
		// No parent context exists, so only RequiredPolicyAlways should fail here.
		return enforceWhenParentMissing && len(childIndices) == 0
	}
	if _, hasNonArrayChild := childIndices[""]; hasNonArrayChild {
		_, hasNonArrayParent := parentIndices[""]
		return !hasNonArrayParent
	}
	for idx := range parentIndices {
		if _, ok := childIndices[idx]; !ok {
			return true
		}
	}
	return false
}

// collectParentIndices gathers the array-index contexts that imply a parent path
// exists in the current EAV row. Descendant attributes contribute their observed
// indices so required children can be checked against the same contexts.
func collectParentIndices(parentPath string, presentAttrIndices map[string]map[string]struct{}) map[string]struct{} {
	parentIndices := make(map[string]struct{})
	prefix := parentPath + "."
	for presentAttrName, indexSet := range presentAttrIndices {
		if presentAttrName != parentPath && !strings.HasPrefix(presentAttrName, prefix) {
			continue
		}
		for idx := range indexSet {
			parentIndices[idx] = struct{}{}
		}
	}
	return parentIndices
}

func attributeParentPath(attrPath string) (string, bool) {
	lastDot := strings.LastIndex(attrPath, ".")
	if lastDot < 0 {
		return "", false
	}
	return attrPath[:lastDot], true
}

func toTime(value any) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case string:
		epoch, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return time.UnixMilli(epoch), nil
		}

		formats := []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02",
			"2006-01",
		}
		for _, format := range formats {
			if parsed, err := time.Parse(format, v); err == nil {
				return parsed, nil
			}
		}
		return time.Time{}, fmt.Errorf("unsupported time format: %s", v)
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to time.Time", value)
	}
}

func toBool(value any) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case string:
		return strconv.ParseBool(v)
	case int:
		return v != 0, nil
	case int64:
		return v != 0, nil
	case float64:
		return v != 0, nil
	case json.Number:
		f, err := v.Float64()
		if err != nil {
			return false, fmt.Errorf("cannot convert json.Number %q to bool: %w", v.String(), err)
		}
		return f != 0, nil
	default:
		return false, fmt.Errorf("cannot convert %T to bool", value)
	}
}

// ToFloat64Ok is an exported helper that behaves like the legacy optimizer helper:
// it returns (float64, bool) where bool indicates success.
func ToFloat64(v any) (float64, bool) {
	return numutil.ToFloat64(v)
}
