package transform

import (
	"fmt"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// readFromMainColumn reads an attribute value from the main table columns.
// It delegates to specialized functions based on column type.
func (t *persistentRecordTransformer) readFromMainColumn(record *model.PersistentRecord, meta forma.AttributeMetadata, binding *forma.MainColumnBinding) (*model.EAVRecord, error) {
	// First, check if this is a system column (RowID, SchemaID, timestamps)
	if attr := t.readFromSystemColumn(record, meta, binding); attr != nil {
		return attr, nil
	}

	// Then, try reading with special encoding
	attr, hasValue, err := t.readWithEncoding(record, meta, binding)
	if err != nil {
		return nil, err
	}
	if hasValue {
		return attr, nil
	}

	// Finally, try reading with default encoding
	attr, hasValue, err = t.readWithDefaultEncoding(record, meta, binding)
	if err != nil {
		return nil, err
	}
	if hasValue {
		return attr, nil
	}

	return nil, nil
}

// readFromSystemColumn handles reading from system columns (RowID, SchemaID, timestamps).
// Returns nil if the column is not a system column.
func (t *persistentRecordTransformer) readFromSystemColumn(record *model.PersistentRecord, meta forma.AttributeMetadata, binding *forma.MainColumnBinding) *model.EAVRecord {
	baseAttr := model.EAVRecord{
		SchemaID:     record.SchemaID,
		RowID:        record.RowID,
		AttrID:       meta.AttributeID,
		ArrayIndices: "",
	}

	switch binding.ColumnName {
	case forma.MainColumnRowID:
		rowIDStr := record.RowID.String()
		baseAttr.ValueText = &rowIDStr
		return &baseAttr

	case forma.MainColumnSchemaID:
		schemaIDFloat := float64(record.SchemaID)
		baseAttr.ValueNumeric = &schemaIDFloat
		return &baseAttr

	case forma.MainColumnCreatedAt:
		createdAtFloat := float64(record.CreatedAt)
		baseAttr.ValueNumeric = &createdAtFloat
		return &baseAttr

	case forma.MainColumnUpdatedAt:
		updatedAtFloat := float64(record.UpdatedAt)
		baseAttr.ValueNumeric = &updatedAtFloat
		return &baseAttr

	case forma.MainColumnDeletedAt:
		if record.DeletedAt == nil {
			return nil
		}
		deletedAtFloat := float64(*record.DeletedAt)
		baseAttr.ValueNumeric = &deletedAtFloat
		return &baseAttr
	}

	return nil
}

// readWithEncoding handles reading values with special encodings (UnixMs, BoolInt, BoolText, ISO8601).
// Returns (attr, hasValue, error).
func (t *persistentRecordTransformer) readWithEncoding(record *model.PersistentRecord, meta forma.AttributeMetadata, binding *forma.MainColumnBinding) (*model.EAVRecord, bool, error) {
	columnName := string(binding.ColumnName)

	attr := &model.EAVRecord{
		SchemaID:     record.SchemaID,
		RowID:        record.RowID,
		AttrID:       meta.AttributeID,
		ArrayIndices: "",
	}

	switch binding.Encoding {
	case forma.MainColumnEncodingUnixMs:
		// Read Unix milliseconds from bigint column and convert to time
		if val, ok := record.Int64Items[columnName]; ok {
			f := float64(val)
			exact := val
			attr.ValueNumeric = &f
			attr.ValueInt64 = &exact
			return attr, true, nil
		}

	case forma.MainColumnEncodingBoolInt:
		// Read smallint (1/0) and convert to bool
		if val, ok := record.Int16Items[columnName]; ok {
			f := float64(val)
			attr.ValueNumeric = &f
			return attr, true, nil
		}

	case forma.MainColumnEncodingBoolText:
		// Read text ("1"/"0") and convert to bool
		if val, ok := record.TextItems[columnName]; ok {
			parsed, err := boolFromBoolText(val)
			if err != nil {
				return nil, false, fmt.Errorf("column %s: %w", columnName, err)
			}
			b := boolToFloat64(parsed)
			attr.ValueNumeric = &b
			return attr, true, nil
		}

	case forma.MainColumnEncodingISO8601:
		// Read ISO 8601 string from text column and convert to time
		if val, ok := record.TextItems[columnName]; ok {
			parsedTime, err := time.Parse(time.RFC3339, val)
			if err != nil {
				return nil, false, fmt.Errorf("failed to parse ISO 8601 date: %w", err)
			}
			// The image has a four-digit year, so its millis are inside
			// the int64 range by construction; both slots carry them.
			setEpochMillis(attr, parsedTime.UnixMilli())
			return attr, true, nil
		}
	}

	return nil, false, nil
}

// readWithDefaultEncoding handles reading values with default encoding based on column type.
// Returns (attr, hasValue, error).
func (t *persistentRecordTransformer) readWithDefaultEncoding(record *model.PersistentRecord, meta forma.AttributeMetadata, binding *forma.MainColumnBinding) (*model.EAVRecord, bool, error) {
	columnName := string(binding.ColumnName)

	attr := &model.EAVRecord{
		SchemaID:     record.SchemaID,
		RowID:        record.RowID,
		AttrID:       meta.AttributeID,
		ArrayIndices: "",
	}

	switch binding.ColumnType() {
	case forma.MainColumnTypeUUID:
		if val, ok := record.UUIDItems[columnName]; ok {
			s := val.String()
			attr.ValueText = &s
			return attr, true, nil
		}
	case forma.MainColumnTypeText:
		if val, ok := record.TextItems[columnName]; ok {
			attr.ValueText = &val
			return attr, true, nil
		}

	case forma.MainColumnTypeSmallint:
		if val, ok := record.Int16Items[columnName]; ok {
			f := float64(val)
			attr.ValueNumeric = &f
			return attr, true, nil
		}

	case forma.MainColumnTypeInteger:
		if val, ok := record.Int32Items[columnName]; ok {
			f := float64(val)
			attr.ValueNumeric = &f
			return attr, true, nil
		}

	case forma.MainColumnTypeBigint:
		if val, ok := record.Int64Items[columnName]; ok {
			f := float64(val)
			exact := val
			attr.ValueNumeric = &f
			attr.ValueInt64 = &exact
			return attr, true, nil
		}

	case forma.MainColumnTypeDouble:
		if val, ok := record.Float64Items[columnName]; ok {
			attr.ValueNumeric = &val
			return attr, true, nil
		}

	default:
		return nil, false, fmt.Errorf("unsupported column type: %s", binding.ColumnType())
	}

	return nil, false, nil
}

// isSystemManagedColumn reports the main columns the record owns itself
// (row id, schema id, lifecycle timestamps). A binding to one of them is a
// read-only alias — storeInMainColumn skips it, and so does checkStorageFit,
// because the caller's value is never what gets written there.
func isSystemManagedColumn(col forma.MainColumn) bool {
	switch col {
	case forma.MainColumnRowID, forma.MainColumnSchemaID,
		forma.MainColumnCreatedAt, forma.MainColumnUpdatedAt, forma.MainColumnDeletedAt:
		return true
	}
	return false
}
