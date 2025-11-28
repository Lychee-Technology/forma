package internal

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
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
		unixMillis := float64(timeVal.UnixMilli())
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
		var floatBool float64
		if boolVal {
			floatBool = 1.0
		} else {
			floatBool = 0.0
		}
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

	attributes := make([]EntityAttribute, 0, len(records))
	for _, record := range records {
		attrName, ok := idToName[record.AttrID]
		if !ok {
			return nil, fmt.Errorf("attribute id %d not found for schema %d", record.AttrID, record.SchemaID)
		}

		meta := cache[attrName]
		attr, err := c.FromEAVRecord(record, meta.ValueType)
		if err != nil {
			return nil, fmt.Errorf("convert record attrID=%d: %w", record.AttrID, err)
		}
		attributes = append(attributes, attr)
	}

	return attributes, nil
}

// Helper functions for conversion

func toFloat64ForEAV(value any) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", value)
	}
}

func toTimeForEAV(value any) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case *time.Time:
		if v == nil {
			return time.Time{}, fmt.Errorf("nil time pointer")
		}
		return *v, nil
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to time.Time", value)
	}
}

func toBoolForEAV(value any) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case *bool:
		if v == nil {
			return false, fmt.Errorf("nil bool pointer")
		}
		return *v, nil
	default:
		return false, fmt.Errorf("cannot convert %T to bool", value)
	}
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
		timeVal := time.UnixMilli(int64(*record.ValueNumeric)).UTC()
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
		return *record.ValueNumeric > 0.5, nil

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
