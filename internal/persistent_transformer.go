package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

type persistentRecordTransformer struct {
	registry        forma.SchemaRegistry
	jsonTransformer Transformer
	*schemaMetadataCache
}

// NewPersistentRecordTransformer creates a new PersistentRecordTransformer instance
func NewPersistentRecordTransformer(registry forma.SchemaRegistry) PersistentRecordTransformer {
	return &persistentRecordTransformer{
		registry:            registry,
		jsonTransformer:     NewTransformer(registry),
		schemaMetadataCache: newSchemaMetadataCache(registry),
	}
}

func (t *persistentRecordTransformer) ToPersistentRecord(ctx context.Context, schemaID int16, rowID uuid.UUID, jsonData any) (*PersistentRecord, error) {
	if jsonData == nil {
		return nil, fmt.Errorf("jsonData cannot be nil")
	}

	// First convert to EntityAttributes using existing transformer logic
	entityAttributes, err := t.jsonTransformer.ToAttributes(ctx, schemaID, rowID, jsonData)
	if err != nil {
		return nil, fmt.Errorf("failed to convert to attributes: %w", err)
	}

	// Convert EntityAttributes to EAVRecords for database layer
	converter := NewAttributeConverter(t.registry)
	eavRecords, err := converter.ToEAVRecords(entityAttributes, rowID)
	if err != nil {
		return nil, fmt.Errorf("failed to convert to EAVRecords: %w", err)
	}

	// Get schema metadata
	cache, _, err := t.getSchemaMetadata(schemaID)
	if err != nil {
		return nil, err
	}

	// Initialize the persistent record
	record := &PersistentRecord{
		SchemaID:     schemaID,
		RowID:        rowID,
		TextItems:    make(map[string]string),
		Int16Items:   make(map[string]int16),
		Int32Items:   make(map[string]int32),
		Int64Items:   make(map[string]int64),
		Float64Items: make(map[string]float64),
		UpdatedAt:    time.Now().UnixMilli(),
	}

	// Set created_at if this is a new record
	record.CreatedAt = record.UpdatedAt

	// Process each EAV record
	for _, eavRecord := range eavRecords {
		// Find the attribute metadata
		var meta forma.AttributeMetadata
		var attrName string
		found := false
		for name, m := range cache {
			if m.AttributeID == eavRecord.AttrID {
				meta = m
				attrName = name
				found = true
				break
			}
		}

		if !found {
			return nil, fmt.Errorf("attribute ID %d not found in schema %d", eavRecord.AttrID, schemaID)
		}

		if meta.ColumnBinding != nil {
			if err := t.storeInMainColumn(record, eavRecord, meta.ColumnBinding); err != nil {
				return nil, fmt.Errorf("failed to store attribute %s in main column: %w", attrName, err)
			}
		} else {
			record.OtherAttributes = append(record.OtherAttributes, eavRecord)
		}
	}

	return record, nil
}

func (t *persistentRecordTransformer) FromPersistentRecord(ctx context.Context, record *PersistentRecord) (map[string]any, error) {
	if record == nil {
		return nil, fmt.Errorf("record cannot be nil")
	}

	// Get schema metadata
	cache, _, err := t.getSchemaMetadata(record.SchemaID)
	if err != nil {
		return nil, err
	}

	// Reconstruct attributes from main table columns
	attributes := make([]EAVRecord, 0)

	// Process each attribute in the cache to see if it has column binding
	for attrName, meta := range cache {
		if meta.ColumnBinding == nil {
			continue
		}

		attr, err := t.readFromMainColumn(record, attrName, meta, meta.ColumnBinding)
		if err != nil {
			return nil, fmt.Errorf("failed to read attribute %s from main column: %w", attrName, err)
		}
		if attr != nil {
			attributes = append(attributes, *attr)
		}
	}

	// Add EAV attributes
	attributes = append(attributes, record.OtherAttributes...)

	// Convert EAVRecords to EntityAttributes
	converter := NewAttributeConverter(t.registry)
	entityAttributes, err := converter.FromEAVRecords(attributes)
	if err != nil {
		return nil, fmt.Errorf("failed to convert EAVRecords to EntityAttributes: %w", err)
	}

	// Convert EntityAttributes back to JSON using existing transformer
	result, err := t.jsonTransformer.FromAttributes(ctx, entityAttributes)
	if err != nil {
		return nil, fmt.Errorf("failed to convert from attributes: %w", err)
	}

	return result, nil
}

func (t *persistentRecordTransformer) storeInMainColumn(record *PersistentRecord, attr EAVRecord, binding *forma.MainColumnBinding) error {
	columnName := string(binding.ColumnName)

	switch binding.Encoding {
	case forma.MainColumnEncodingUnixMs:
		// Date stored as Unix milliseconds in bigint column
		if attr.ValueNumeric != nil {
			record.Int64Items[columnName] = int64(*attr.ValueNumeric)
		}

	case forma.MainColumnEncodingBoolInt:
		// Bool stored as smallint (1/0)
		if attr.ValueNumeric != nil {
			if *attr.ValueNumeric > 0.5 {
				record.Int16Items[columnName] = 1
			} else {
				record.Int16Items[columnName] = 0
			}
		}

	case forma.MainColumnEncodingBoolText:
		// Bool stored as text ("1"/"0")
		if attr.ValueNumeric != nil {
			if *attr.ValueNumeric > 0.5 {
				record.TextItems[columnName] = "1"
			} else {
				record.TextItems[columnName] = "0"
			}
		}

	case forma.MainColumnEncodingISO8601:
		// Date stored as ISO 8601 string in text column
		if attr.ValueNumeric != nil {
			record.TextItems[columnName] = time.UnixMilli(int64(*attr.ValueNumeric)).UTC().Format(time.RFC3339)
		}

	case forma.MainColumnEncodingDefault:
		fallthrough
	default:
		// Default encoding based on column type
		switch binding.ColumnType() {
		case forma.MainColumnTypeText:
			if attr.ValueText != nil {
				record.TextItems[columnName] = *attr.ValueText
			}

		case forma.MainColumnTypeSmallint:
			if attr.ValueNumeric != nil {
				record.Int16Items[columnName] = int16(*attr.ValueNumeric)
			}

		case forma.MainColumnTypeInteger:
			if attr.ValueNumeric != nil {
				record.Int32Items[columnName] = int32(*attr.ValueNumeric)
			}

		case forma.MainColumnTypeBigint:
			if attr.ValueNumeric != nil {
				record.Int64Items[columnName] = int64(*attr.ValueNumeric)
			}

		case forma.MainColumnTypeDouble:
			if attr.ValueNumeric != nil {
				record.Float64Items[columnName] = *attr.ValueNumeric
			}

		default:
			return fmt.Errorf("unsupported column type: %s", binding.ColumnType())
		}
	}

	return nil
}

func (t *persistentRecordTransformer) readFromMainColumn(record *PersistentRecord, attrName string, meta forma.AttributeMetadata, binding *forma.MainColumnBinding) (*EAVRecord, error) {
	columnName := string(binding.ColumnName)

	attr := &EAVRecord{
		SchemaID:     record.SchemaID,
		RowID:        record.RowID,
		AttrID:       meta.AttributeID,
		ArrayIndices: "",
	}

	var hasValue bool

	switch binding.Encoding {
	case forma.MainColumnEncodingUnixMs:
		// Read Unix milliseconds from bigint column and convert to time
		if val, ok := record.Int64Items[columnName]; ok {
			t := float64(val)
			attr.ValueNumeric = &t
			hasValue = true
		}

	case forma.MainColumnEncodingBoolInt:
		// Read smallint (1/0) and convert to bool
		if val, ok := record.Int16Items[columnName]; ok {
			f := float64(val)
			attr.ValueNumeric = &f
			hasValue = true
		}

	case forma.MainColumnEncodingBoolText:
		// Read text ("1"/"0") and convert to bool
		if val, ok := record.TextItems[columnName]; ok {
			if val == "1" {
				b := 1.0
				attr.ValueNumeric = &b
			} else {
				b := 0.0
				attr.ValueNumeric = &b
			}
			hasValue = true
		}

	case forma.MainColumnEncodingISO8601:
		// Read ISO 8601 string from text column and convert to time
		if val, ok := record.TextItems[columnName]; ok {
			parsedTime, err := time.Parse(time.RFC3339, val)
			if err != nil {
				return nil, fmt.Errorf("failed to parse ISO 8601 date: %w", err)
			}
			unixMillis := float64(parsedTime.UnixMilli())
			attr.ValueNumeric = &unixMillis
			hasValue = true
		}

	case forma.MainColumnEncodingDefault:
		fallthrough
	default:
		// Default encoding based on column type and value type
		switch binding.ColumnType() {
		case forma.MainColumnTypeText:
			if val, ok := record.TextItems[columnName]; ok {
				attr.ValueText = &val
				hasValue = true
			}

		case forma.MainColumnTypeSmallint:
			if val, ok := record.Int16Items[columnName]; ok {
				f := float64(val)
				attr.ValueNumeric = &f
				hasValue = true
			}

		case forma.MainColumnTypeInteger:
			if val, ok := record.Int32Items[columnName]; ok {
				f := float64(val)
				attr.ValueNumeric = &f
				hasValue = true
			}

		case forma.MainColumnTypeBigint:
			if val, ok := record.Int64Items[columnName]; ok {
				f := float64(val)
				attr.ValueNumeric = &f
				hasValue = true
			}

		case forma.MainColumnTypeDouble:
			if val, ok := record.Float64Items[columnName]; ok {
				attr.ValueNumeric = &val
				hasValue = true
			}

		default:
			return nil, fmt.Errorf("unsupported column type: %s", binding.ColumnType())
		}
	}

	if !hasValue {
		return nil, nil
	}

	return attr, nil
}
