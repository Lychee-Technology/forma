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
			continue
		}
		storeInEAV(record, eavRecord)
	}

	return record, nil
}

func (t *persistentRecordTransformer) FromPersistentRecord(ctx context.Context, record *model.PersistentRecord) (map[string]any, error) {
	if record == nil {
		return nil, fmt.Errorf("record cannot be nil")
	}

	// Every error raised while rebuilding a row names the row (#405): the
	// EAV funnel renders it at FromEAVRecord, the hop that holds the record,
	// and the wraps here cover the hops that do not hold it on their own.
	cache, _, err := schemameta.GetSchemaMetadata(t.registry, record.SchemaID)
	if err != nil {
		return nil, fmt.Errorf("failed to load schema %d metadata for row %s: %w", record.SchemaID, record.RowID, err)
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
			return nil, fmt.Errorf("failed to read attribute %s of row %s from main column: %w", attrName, record.RowID, err)
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

	// Convert EntityAttributes back to JSON. FromAttributes names the
	// attribute but not the row (a malformed persisted array_indices fails
	// here, not in FromEAVRecord, which does not parse it), so the row is
	// added at this hop.
	result, err := t.jsonTransformer.FromAttributes(ctx, entityAttributes)
	if err != nil {
		return nil, fmt.Errorf("failed to convert attributes of row %s to JSON: %w", record.RowID, err)
	}

	return result, nil
}

// storeInEAV appends the record to the row's eav_data attributes. eav_data
// persists the float64 ValueNumeric image only (#205, 2^53 ceiling): the
// exact sidecar is memory-only and is cleared here so the create-response
// echo matches what is written. The funnels derive the image from the exact
// millis (setEpochMillis), so within 2^53 it is the logical value; the
// contract past that is #592.
func storeInEAV(record *model.PersistentRecord, attr model.EAVRecord) {
	attr.ValueInt64 = nil
	record.OtherAttributes = append(record.OtherAttributes, attr)
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

// storeWithEncoding places the value by (encoding, ColumnType()), the same
// pair checkBoundColumnFit keys on (#559). An explicit encoding names a
// rendering of the numeric slot — unix_ms and bool_smallint render a number,
// bool_text and iso8601 render text — and the column type names the map the
// rendering lands in; the default encoding writes the slot the column
// consumes. It reports whether a value was written so the caller can refuse
// an empty slot instead of dropping it (#459).
func (t *persistentRecordTransformer) storeWithEncoding(record *model.PersistentRecord, attr model.EAVRecord, binding *forma.MainColumnBinding) (bool, error) {
	switch binding.Encoding {
	case forma.MainColumnEncodingUnixMs:
		// Date as epoch millis; the exact sidecar wins where the column keeps it.
		if attr.ValueNumeric == nil {
			return false, nil
		}
		return storeNumericRendering(record, binding, *attr.ValueNumeric, attr.ValueInt64)
	case forma.MainColumnEncodingBoolInt:
		// Bool as 1/0
		if attr.ValueNumeric == nil {
			return false, nil
		}
		return storeNumericRendering(record, binding, boolToFloat64(float64ToBool(*attr.ValueNumeric)), nil)
	case forma.MainColumnEncodingBoolText:
		// Bool as "1"/"0"
		if attr.ValueNumeric == nil {
			return false, nil
		}
		text := "0"
		if float64ToBool(*attr.ValueNumeric) {
			text = "1"
		}
		return storeTextRendering(record, binding, text)
	case forma.MainColumnEncodingISO8601:
		// Date as an RFC3339 string at whole seconds within a four-digit
		// year. checkBoundColumnFit refuses a value the image cannot hold,
		// so reaching one here is a funnel bypass: refuse it rather than
		// truncate the caller's value or write an image the read path
		// cannot parse (#582).
		if !hasEpochMillis(&attr) {
			return false, nil
		}
		ms, err := exactEpochMillis(&attr)
		if err != nil {
			return false, fmt.Errorf("encoding %s cannot hold a slot in main column %s: %w", binding.Encoding, binding.ColumnName, err)
		}
		text, rule := iso8601Rendering(ms)
		if rule != "" {
			return false, fmt.Errorf("encoding %s %s and cannot hold value %d in main column %s",
				binding.Encoding, rule, ms, binding.ColumnName)
		}
		return storeTextRendering(record, binding, text)
	default:
		return t.storeWithDefaultEncoding(record, attr, binding)
	}
}

// storeNumericRendering writes a number into the map of the bound column's
// type: the integer maps at the column's width (checkIntegerFit has already
// bounded the value, so int16()/int32() cannot wrap), the double map, or the
// exact int64 sidecar for a bigint column when the caller has one. A text or
// uuid column cannot hold a number; checkBoundColumnFit refuses the pair, so
// reaching it here is a funnel bypass, reported as such.
func storeNumericRendering(record *model.PersistentRecord, binding *forma.MainColumnBinding, numeric float64, exact *int64) (bool, error) {
	columnName := string(binding.ColumnName)
	switch binding.ColumnType() {
	case forma.MainColumnTypeSmallint:
		record.Int16Items[columnName] = int16(numeric)
	case forma.MainColumnTypeInteger:
		record.Int32Items[columnName] = int32(numeric)
	case forma.MainColumnTypeBigint:
		if exact != nil {
			record.Int64Items[columnName] = *exact
			return true, nil
		}
		record.Int64Items[columnName] = int64(numeric)
	case forma.MainColumnTypeDouble:
		record.Float64Items[columnName] = numeric
	default:
		return false, errNoSlotForRendering(binding, "a numeric value")
	}
	return true, nil
}

// storeTextRendering writes a string into a text column; no other column
// type holds text, so anything else is the same funnel bypass as above.
func storeTextRendering(record *model.PersistentRecord, binding *forma.MainColumnBinding, text string) (bool, error) {
	if binding.ColumnType() != forma.MainColumnTypeText {
		return false, errNoSlotForRendering(binding, "a text value")
	}
	record.TextItems[string(binding.ColumnName)] = text
	return true, nil
}

// numericRenderingColumn reports the column types storeNumericRendering
// serializes into; checkBoundColumnFit admits unix_ms and bool_smallint on
// exactly these.
func numericRenderingColumn(colType forma.MainColumnType) bool {
	switch colType {
	case forma.MainColumnTypeSmallint, forma.MainColumnTypeInteger, forma.MainColumnTypeBigint, forma.MainColumnTypeDouble:
		return true
	}
	return false
}

func errNoSlotForRendering(binding *forma.MainColumnBinding, renders string) error {
	return fmt.Errorf("encoding %s renders %s that main column %s (%s) cannot hold",
		binding.Encoding, renders, binding.ColumnName, binding.ColumnType())
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
	case forma.MainColumnTypeSmallint, forma.MainColumnTypeInteger, forma.MainColumnTypeBigint, forma.MainColumnTypeDouble:
		if attr.ValueNumeric == nil {
			return false, nil
		}
		return storeNumericRendering(record, binding, *attr.ValueNumeric, attr.ValueInt64)
	default:
		return false, fmt.Errorf("unsupported column type: %s", binding.ColumnType())
	}
	return true, nil
}
