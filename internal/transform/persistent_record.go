package transform

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"
)

type persistentRecordTransformer struct {
	registry        forma.SchemaRegistry
	jsonTransformer *transformer
	relationRoots   RelationRootsLookup
}

// SetRelationRoots installs the relation-root lookup on this transformer and on
// every converter it builds, so the required-policy carve-out (#314/#315)
// applies on both the write and read halves of this type.
func (t *persistentRecordTransformer) SetRelationRoots(lookup RelationRootsLookup) {
	t.relationRoots = lookup
	t.jsonTransformer.SetRelationRoots(lookup)
}

// newConverter builds a converter carrying whatever relation-root lookup has
// been installed.
func (t *persistentRecordTransformer) newConverter() *AttributeConverter {
	converter := NewAttributeConverter(t.registry)
	converter.SetRelationRoots(t.relationRoots)
	return converter
}

// NewPersistentRecordTransformer creates a new PersistentRecordTransformer instance
func NewPersistentRecordTransformer(registry forma.SchemaRegistry) model.PersistentRecordTransformer {
	return &persistentRecordTransformer{
		registry:        registry,
		jsonTransformer: NewTransformer(registry),
	}
}

func (t *persistentRecordTransformer) ToPersistentRecord(ctx context.Context, schemaID int16, rowID uuid.UUID, jsonData any) (*model.PersistentRecord, error) {
	if jsonData == nil {
		return nil, fmt.Errorf("jsonData cannot be nil")
	}

	// First convert to EntityAttributes using existing transformer logic
	entityAttributes, err := t.jsonTransformer.ToAttributes(ctx, schemaID, rowID, jsonData)
	if err != nil {
		return nil, fmt.Errorf("failed to convert to attributes: %w", err)
	}

	// Convert EntityAttributes to EAVRecords for database layer
	converter := t.newConverter()
	eavRecords, err := converter.ToEAVRecords(entityAttributes, rowID)
	if err != nil {
		return nil, fmt.Errorf("failed to convert to EAVRecords: %w", err)
	}

	// Get schema metadata
	cache, _, err := schemameta.GetSchemaMetadata(t.registry, schemaID)
	if err != nil {
		return nil, err
	}

	// Initialize the persistent record
	record := &model.PersistentRecord{
		SchemaID:     schemaID,
		RowID:        rowID,
		TextItems:    make(map[string]string),
		Int16Items:   make(map[string]int16),
		Int32Items:   make(map[string]int32),
		Int64Items:   make(map[string]int64),
		UUIDItems:    make(map[string]uuid.UUID),
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
			// EAV storage keeps the float64 ValueNumeric contract (2^53
			// ceiling); clear the exact sidecar so the create-response echo
			// matches what eav_data actually persists (#205).
			eavRecord.ValueInt64 = nil
			record.OtherAttributes = append(record.OtherAttributes, eavRecord)
		}
	}

	return record, nil
}

func (t *persistentRecordTransformer) FromPersistentRecord(ctx context.Context, record *model.PersistentRecord) (map[string]any, error) {
	if record == nil {
		return nil, fmt.Errorf("record cannot be nil")
	}

	// Get schema metadata
	cache, _, err := schemameta.GetSchemaMetadata(t.registry, record.SchemaID)
	if err != nil {
		return nil, err
	}

	// Reconstruct attributes from main table columns
	attributes := make([]model.EAVRecord, 0)

	// Process each attribute in the cache to see if it has column binding
	for attrName, meta := range cache {
		if meta.ColumnBinding == nil {
			continue
		}

		attr, err := t.readFromMainColumn(record, meta, meta.ColumnBinding)
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
	converter := t.newConverter()
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

func (t *persistentRecordTransformer) storeInMainColumn(record *model.PersistentRecord, attr model.EAVRecord, binding *forma.MainColumnBinding) error {
	// System column bindings are read-only views: the record's own fields are
	// the source of truth and are set internally by code.
	if isSystemManagedColumn(binding.ColumnName) {
		return nil
	}

	// checkStorageFit enforces the funnel rule: the value must fit where it is
	// physically going (#459); an empty slot here means a caller bypassed the
	// funnel, and dropping the value silently would confirm to the client
	// something that was never written.
	stored, err := t.storeWithEncoding(record, attr, binding)
	if err != nil {
		return err
	}
	if !stored {
		return fmt.Errorf("no value to store in main column %s: attr id %d of schema %d (row %s) has an empty %s slot",
			binding.ColumnName, attr.AttrID, attr.SchemaID, attr.RowID, binding.ColumnType())
	}
	return nil
}

// storeWithEncoding dispatches on the binding's encoding, then on the column
// type for the default encoding. It reports whether a value was written so
// the caller can refuse an empty slot instead of dropping it (#459).
func (t *persistentRecordTransformer) storeWithEncoding(record *model.PersistentRecord, attr model.EAVRecord, binding *forma.MainColumnBinding) (bool, error) {
	columnName := string(binding.ColumnName)
	switch binding.Encoding {
	case forma.MainColumnEncodingUnixMs:
		// Date stored as Unix milliseconds in bigint column
		if attr.ValueInt64 != nil {
			record.Int64Items[columnName] = *attr.ValueInt64
			return true, nil
		}
		if attr.ValueNumeric != nil {
			record.Int64Items[columnName] = int64(*attr.ValueNumeric)
			return true, nil
		}
		return false, nil
	case forma.MainColumnEncodingBoolInt:
		// Bool stored as smallint (1/0)
		if attr.ValueNumeric == nil {
			return false, nil
		}
		record.Int16Items[columnName] = 0
		if float64ToBool(*attr.ValueNumeric) {
			record.Int16Items[columnName] = 1
		}
		return true, nil
	case forma.MainColumnEncodingBoolText:
		// Bool stored as text ("1"/"0")
		if attr.ValueNumeric == nil {
			return false, nil
		}
		record.TextItems[columnName] = "0"
		if float64ToBool(*attr.ValueNumeric) {
			record.TextItems[columnName] = "1"
		}
		return true, nil
	case forma.MainColumnEncodingISO8601:
		// Date stored as ISO 8601 string in text column
		if attr.ValueNumeric == nil {
			return false, nil
		}
		record.TextItems[columnName] = unixMillisFloat64ToTimeUTC(*attr.ValueNumeric).Format(time.RFC3339)
		return true, nil
	default:
		return t.storeWithDefaultEncoding(record, attr, binding)
	}
}

// storeWithDefaultEncoding writes the slot the column type consumes. The
// uuid branch cannot fail for a value that passed checkStorageFit; the
// parse stays as an invariant check.
func (t *persistentRecordTransformer) storeWithDefaultEncoding(record *model.PersistentRecord, attr model.EAVRecord, binding *forma.MainColumnBinding) (bool, error) {
	columnName := string(binding.ColumnName)
	switch binding.ColumnType() {
	case forma.MainColumnTypeText:
		if attr.ValueText == nil {
			return false, nil
		}
		record.TextItems[columnName] = *attr.ValueText
	case forma.MainColumnTypeSmallint:
		if attr.ValueNumeric == nil {
			return false, nil
		}
		record.Int16Items[columnName] = int16(*attr.ValueNumeric)
	case forma.MainColumnTypeInteger:
		if attr.ValueNumeric == nil {
			return false, nil
		}
		record.Int32Items[columnName] = int32(*attr.ValueNumeric)
	case forma.MainColumnTypeBigint:
		if attr.ValueInt64 != nil {
			record.Int64Items[columnName] = *attr.ValueInt64
			return true, nil
		}
		if attr.ValueNumeric == nil {
			return false, nil
		}
		record.Int64Items[columnName] = int64(*attr.ValueNumeric)
	case forma.MainColumnTypeDouble:
		if attr.ValueNumeric == nil {
			return false, nil
		}
		record.Float64Items[columnName] = *attr.ValueNumeric
	case forma.MainColumnTypeUUID:
		if attr.ValueText == nil {
			return false, nil
		}
		uuidValue, err := uuid.Parse(*attr.ValueText)
		if err != nil {
			return false, fmt.Errorf("failed to parse uuid: %w. schema id: %d, row id: %s, attr id: %d, array indices: %s, value: %s",
				err, attr.SchemaID, attr.RowID, attr.AttrID, attr.ArrayIndices, *attr.ValueText)
		}
		record.UUIDItems[columnName] = uuidValue
	default:
		return false, fmt.Errorf("unsupported column type: %s", binding.ColumnType())
	}
	return true, nil
}
