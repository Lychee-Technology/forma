package internal

import (
	"encoding/json"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

// AttributeConverter provides conversion between EntityAttribute and EAVRecord
type AttributeConverter struct {
	registry forma.SchemaRegistry
	*schemaMetadataCache
}

// NewAttributeConverter creates a new AttributeConverter instance
func NewAttributeConverter(registry forma.SchemaRegistry) *AttributeConverter {
	return &AttributeConverter{
		registry:            registry,
		schemaMetadataCache: newSchemaMetadataCache(registry),
	}
}

// ToEAVRecord converts an EntityAttribute to an EAVRecord
func (c *AttributeConverter) ToEAVRecord(attr EntityAttribute, rowID uuid.UUID) (EAVRecord, error) {
	record := EAVRecord{
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

	case forma.ValueTypeDate, forma.ValueTypeDateTime:
		timeVal, err := toTimeForEAV(attr.Value)
		if err != nil {
			return record, fmt.Errorf("convert to time: %w", err)
		}
		unixMillis := timeToUnixMillisFloat64(timeVal)
		record.ValueNumeric = &unixMillis

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

// FromEAVRecord converts an EAVRecord to an EntityAttribute
func (c *AttributeConverter) FromEAVRecord(record EAVRecord, valueType forma.ValueType) (EntityAttribute, error) {
	attr := EntityAttribute{
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
func (c *AttributeConverter) ToEAVRecords(attributes []EntityAttribute, rowID uuid.UUID) ([]EAVRecord, error) {
	records := make([]EAVRecord, 0, len(attributes))
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
func (c *AttributeConverter) FromEAVRecords(records []EAVRecord) ([]EntityAttribute, error) {
	if len(records) == 0 {
		return []EntityAttribute{}, nil
	}

	// Get schema metadata to determine value types
	schemaID := records[0].SchemaID
	cache, idToName, err := c.getSchemaMetadata(schemaID)
	if err != nil {
		return nil, err
	}

	copyIdToName := make(map[int16]string, len(idToName))
	maps.Copy(copyIdToName, idToName)

	attributes := make([]EntityAttribute, 0, len(records))
	for _, record := range records {
		attrName, ok := idToName[record.AttrID]
		if !ok {
			continue
		}

		meta := cache[attrName]
		attr, err := c.FromEAVRecord(record, meta.ValueType)
		if err != nil {
			return nil, fmt.Errorf("convert record attrID=%d: %w", record.AttrID, err)
		}
		attributes = append(attributes, attr)
		delete(copyIdToName, record.AttrID)
	}

	if len(copyIdToName) > 0 {
		zap.S().Infow("missing EAV records for attrIDs.", "idToName", copyIdToName)
		for missingAttrID, missingAttrName := range copyIdToName {
			metadata, ok := cache[missingAttrName]
			if !ok {
				zap.S().Warnw("missing attribute metadata for missing EAV record", "attrID", missingAttrID, "attrName", missingAttrName)
				continue
			}
			if metadata.Required {
				return nil, fmt.Errorf("missing required attribute '%s' (attrID=%d) in EAV records", missingAttrName, missingAttrID)
			}
		}
	}

	return attributes, nil
}

// Helper functions for conversion

func toFloat64ForEAV(value any) (float64, error) {
	switch v := value.(type) {
	case *float64:
		num, err := derefPointer(v, "float64")
		if err != nil {
			return 0, err
		}
		return toFloat64(num)
	case *float32:
		num, err := derefPointer(v, "float32")
		if err != nil {
			return 0, err
		}
		return toFloat64(num)
	case *int:
		num, err := derefPointer(v, "int")
		if err != nil {
			return 0, err
		}
		return toFloat64(num)
	case *int16:
		num, err := derefPointer(v, "int16")
		if err != nil {
			return 0, err
		}
		return toFloat64(num)
	case *int32:
		num, err := derefPointer(v, "int32")
		if err != nil {
			return 0, err
		}
		return toFloat64(num)
	case *int64:
		num, err := derefPointer(v, "int64")
		if err != nil {
			return 0, err
		}
		return toFloat64(num)
	case string:
		return parseTrimmedFloat64(v)
	case *string:
		str, err := derefPointer(v, "string")
		if err != nil {
			return 0, err
		}
		return parseTrimmedFloat64(str)
	default:
		return toFloat64(value)
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
		return v > 0, nil
	case *int:
		num, err := derefPointer(v, "int")
		if err != nil {
			return false, err
		}
		return num > 0, nil
	case int16:
		return v > 0, nil
	case *int16:
		num, err := derefPointer(v, "int")
		if err != nil {
			return false, err
		}
		return num > 0, nil
	case int32:
		return v != 0, nil
	case *int32:
		num, err := derefPointer(v, "int")
		if err != nil {
			return false, err
		}
		return num > 0, nil
	case int64:
		return v != 0, nil
	case *int64:
		num, err := derefPointer(v, "int")
		if err != nil {
			return false, err
		}
		return num > 0, nil
	case float32:
		return float64ToBool(float64(v)), nil
	case *float32:
		num, err := derefPointer(v, "int")
		if err != nil {
			return false, err
		}
		return float64ToBool(float64(num)), nil
	case float64:
		return float64ToBool(v), nil
	case *float64:
		num, err := derefPointer(v, "int")
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

func toFloat64(value any) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case json.Number:
		return v.Float64()
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", value)
	}
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
	default:
		return false, fmt.Errorf("cannot convert %T to bool", value)
	}
}

// ToFloat64Ok is an exported helper that behaves like the legacy optimizer helper:
// it returns (float64, bool) where bool indicates success.
func ToFloat64(v any) (float64, bool) {
	val, err := toFloat64(v)
	return val, err == nil
}

func extractValueFromEAVRecord(record EAVRecord, valueType forma.ValueType) (any, error) {
	switch valueType {
	case forma.ValueTypeText:
		if record.ValueText == nil {
			return nil, nil
		}
		return *record.ValueText, nil

	case forma.ValueTypeSmallInt:
		if record.ValueNumeric == nil {
			return nil, nil
		}
		return int16(*record.ValueNumeric), nil

	case forma.ValueTypeInteger:
		if record.ValueNumeric == nil {
			return nil, nil
		}
		return int32(*record.ValueNumeric), nil

	case forma.ValueTypeBigInt:
		if record.ValueNumeric == nil {
			return nil, nil
		}
		return int64(*record.ValueNumeric), nil

	case forma.ValueTypeNumeric:
		if record.ValueNumeric == nil {
			return nil, nil
		}
		return *record.ValueNumeric, nil

	case forma.ValueTypeDate, forma.ValueTypeDateTime:
		if record.ValueNumeric == nil {
			return nil, nil
		}
		timeVal := unixMillisFloat64ToTimeUTC(*record.ValueNumeric)
		return timeVal, nil

	case forma.ValueTypeUUID:
		if record.ValueText == nil {
			return nil, nil
		}
		uuidVal, err := uuid.Parse(*record.ValueText)
		if err != nil {
			return nil, fmt.Errorf("parse uuid: %w", err)
		}
		return uuidVal, nil

	case forma.ValueTypeBool:
		if record.ValueNumeric == nil {
			return nil, nil
		}
		return toBoolForEAV(record.ValueNumeric)

	default:
		// Fallback: try text first, then numeric
		if record.ValueText != nil {
			return *record.ValueText, nil
		}
		if record.ValueNumeric != nil {
			return *record.ValueNumeric, nil
		}
		return nil, nil
	}
}
