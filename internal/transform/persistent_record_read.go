package transform

import (
	"fmt"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/rfc3339"
)

// readFromMainColumn reads an attribute value from the main table columns.
// It delegates to specialized functions based on column type, and parses a
// text encoding's image at once (encodedText).
func (t *persistentRecordTransformer) readFromMainColumn(record *model.PersistentRecord, meta forma.AttributeMetadata, binding *forma.MainColumnBinding) (*model.EAVRecord, error) {
	attr, image, err := t.readMainColumnImage(record, meta, binding)
	if err != nil || image == nil {
		return attr, err
	}
	if err := image.parseInto(attr); err != nil {
		return nil, err
	}
	return attr, nil
}

// readMainColumnImage is readFromMainColumn without the parse of a text
// encoding's image: for a bool_text or rfc3339 column holding a value, it
// answers the record with no value slot set and the image the slots are
// parsed from. An update's merge base holds that image unparsed
// (fromPersistentRecord), so a malformed one fails only the updates that keep
// it, as an unreadable eav_data value does (#590, #938).
func (t *persistentRecordTransformer) readMainColumnImage(record *model.PersistentRecord, meta forma.AttributeMetadata, binding *forma.MainColumnBinding) (*model.EAVRecord, *encodedText, error) {
	// First, check if this is a system column (RowID, SchemaID, timestamps)
	if attr := t.readFromSystemColumn(record, meta, binding); attr != nil {
		return attr, nil, nil
	}

	// Then, try reading with special encoding
	if attr, image, hasValue := t.readWithEncoding(record, meta, binding); hasValue {
		return attr, image, nil
	}

	// Finally, try reading with default encoding
	attr, hasValue, err := t.readWithDefaultEncoding(record, meta, binding)
	if err != nil {
		return nil, nil, fmt.Errorf("read column %s with default encoding: %w", binding.ColumnName, err)
	}
	if hasValue {
		return attr, nil, nil
	}

	return nil, nil, nil
}

// encodedText is the stored image of a bound text column under an encoding
// that renders a number as text, bool_text or rfc3339, not yet parsed into
// the record's numeric slots.
type encodedText struct {
	encoding forma.MainColumnEncoding
	column   string
	text     string
}

// parseInto parses the image into attr's value slots, failing as the read
// path always has on an image the encoding does not store.
func (e *encodedText) parseInto(attr *model.EAVRecord) error {
	switch e.encoding {
	case forma.MainColumnEncodingBoolText:
		parsed, err := boolFromBoolText(e.text)
		if err != nil {
			return fmt.Errorf("decode %s encoding: column %s: %w", e.encoding, e.column, err)
		}
		b := boolToFloat64(parsed)
		attr.ValueNumeric = &b
	case forma.MainColumnEncodingRFC3339:
		parsedTime, err := rfc3339.Parse(e.text)
		if err != nil {
			return fmt.Errorf("decode %s encoding: column %s: parse ISO 8601 date: %w", e.encoding, e.column, err)
		}
		// The image has a four-digit year, so its millis are inside the
		// int64 range by construction; both slots carry them.
		setEpochMillis(attr, parsedTime.UnixMilli())
	default:
		return fmt.Errorf("decode %s encoding: column %s: the encoding renders no text image", e.encoding, e.column)
	}
	return nil
}

// readFromSystemColumn handles reading from system columns (RowID, SchemaID, timestamps).
// Returns nil if the column is not a system column. A numeric alias names its
// column in BoundColumn, which keeps it out of the integer-image refusal
// (checkIntegerImage): the value is the store's own, not a stored image.
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
		baseAttr.BoundColumn = string(binding.ColumnName)
		return &baseAttr

	case forma.MainColumnCreatedAt:
		createdAtFloat := float64(record.CreatedAt)
		baseAttr.ValueNumeric = &createdAtFloat
		baseAttr.BoundColumn = string(binding.ColumnName)
		return &baseAttr

	case forma.MainColumnUpdatedAt:
		updatedAtFloat := float64(record.UpdatedAt)
		baseAttr.ValueNumeric = &updatedAtFloat
		baseAttr.BoundColumn = string(binding.ColumnName)
		return &baseAttr

	case forma.MainColumnDeletedAt:
		if record.DeletedAt == nil {
			return nil
		}
		deletedAtFloat := float64(*record.DeletedAt)
		baseAttr.ValueNumeric = &deletedAtFloat
		baseAttr.BoundColumn = string(binding.ColumnName)
		return &baseAttr
	}

	return nil
}

// readWithEncoding handles reading values with special encodings (UnixMs,
// BoolInt, BoolText, RFC3339). A text encoding's image is answered unparsed
// beside a record with no value slot set (readMainColumnImage).
// Returns (attr, image, hasValue).
func (t *persistentRecordTransformer) readWithEncoding(record *model.PersistentRecord, meta forma.AttributeMetadata, binding *forma.MainColumnBinding) (*model.EAVRecord, *encodedText, bool) {
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
			return attr, nil, true
		}

	case forma.MainColumnEncodingBoolInt:
		// Read smallint (1/0) and convert to bool
		if val, ok := record.Int16Items[columnName]; ok {
			f := float64(val)
			attr.ValueNumeric = &f
			return attr, nil, true
		}

	case forma.MainColumnEncodingBoolText, forma.MainColumnEncodingRFC3339:
		// Text ("1"/"0", or an ISO 8601 string) parsed into the numeric slots
		if val, ok := record.TextItems[columnName]; ok {
			return attr, &encodedText{encoding: binding.Encoding, column: columnName, text: val}, true
		}
	}

	return nil, nil, false
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
			attr.BoundColumn = columnName
			return attr, true, nil
		}

	case forma.MainColumnTypeInteger:
		if val, ok := record.Int32Items[columnName]; ok {
			f := float64(val)
			attr.ValueNumeric = &f
			attr.BoundColumn = columnName
			return attr, true, nil
		}

	case forma.MainColumnTypeBigint:
		if val, ok := record.Int64Items[columnName]; ok {
			f := float64(val)
			exact := val
			attr.ValueNumeric = &f
			attr.ValueInt64 = &exact
			attr.BoundColumn = columnName
			return attr, true, nil
		}
		// A DuckDB-routed read of a numeric attribute past the int64 range
		// (2^63, the float64 of every int64 from 2^63-512 up) carries only
		// the float64 image; see federated.nullDuckDBBigint.
		if val, ok := record.Float64Items[columnName]; ok {
			attr.ValueNumeric = &val
			attr.BoundColumn = columnName
			return attr, true, nil
		}

	case forma.MainColumnTypeDouble:
		if val, ok := record.Float64Items[columnName]; ok {
			attr.ValueNumeric = &val
			attr.BoundColumn = columnName
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
// because the caller's value is never what gets written there. The set is
// model.IsSystemColumn's, which update-operator intake reads too (#674), so
// an operator is refused on exactly the bindings this skips.
func isSystemManagedColumn(col forma.MainColumn) bool {
	return model.IsSystemColumn(string(col))
}
