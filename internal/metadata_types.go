package internal

import "strings"

// ValueType represents supported attribute value types.
type ValueType string

const (
	ValueTypeText     ValueType = "text"
	ValueTypeSmallInt ValueType = "smallint"
	ValueTypeInteger  ValueType = "integer"
	ValueTypeBigInt   ValueType = "bigint"
	ValueTypeNumeric  ValueType = "numeric"  // double precision
	ValueTypeDate     ValueType = "date"     // for JSON attributes with format `date`
	ValueTypeDateTime ValueType = "datetime" // for JSON attributes with format `date-time`
	ValueTypeUUID     ValueType = "uuid"
	ValueTypeBool     ValueType = "bool"
)

// AttributeStorageLocation enumerates where the attribute physically resides.
type AttributeStorageLocation string

const (
	AttributeStorageLocationUnknown AttributeStorageLocation = ""
	AttributeStorageLocationMain    AttributeStorageLocation = "main"
	AttributeStorageLocationEAV     AttributeStorageLocation = "eav"
)

// AttributeFallbackKind describes lossy storage behaviors.
type AttributeFallbackKind string

const (
	AttributeFallbackKindNone            AttributeFallbackKind = "none"
	AttributeFallbackKindNumericToDouble AttributeFallbackKind = "numeric_to_double"
	AttributeFallbackKindDateToDouble    AttributeFallbackKind = "date_to_double"
	AttributeFallbackKindDateToText      AttributeFallbackKind = "date_to_text"
	AttributeFallbackKindBoolToDouble    AttributeFallbackKind = "bool_to_double"
	AttributeFallbackKindBoolToText      AttributeFallbackKind = "bool_to_text"
)

// AttributeFallbackMetadata captures whether predicate rewrites are required.
type AttributeFallbackMetadata struct {
	Enabled    bool                  `json:"enabled"`
	Kind       AttributeFallbackKind `json:"kind"`
	ColumnType MainColumnType        `json:"column_type"`
	Encoding   MainColumnEncoding    `json:"encoding"`
	Notes      string                `json:"notes"`
}

// AttributeStorageMetadata records where and how an attribute is stored.
type AttributeStorageMetadata struct {
	Location      AttributeStorageLocation  `json:"location"`
	ColumnBinding *MainColumnBinding        `json:"column_binding,omitempty"`
	Fallback      AttributeFallbackMetadata `json:"fallback"`
}

// MainColumnBinding describes how a schema attribute maps into a hot attribute column.
type MainColumnBinding struct {
	ColumnName MainColumn         `json:"col_name"`
	Encoding   MainColumnEncoding `json:"encoding,omitempty"`
}

// AttributeMetadata stores cached metadata from the attributes table.
type AttributeMetadata struct {
	AttributeName string                    `json:"attr_name"`  // attr_name, JSON Path
	AttributeID   int16                     `json:"attr_id"`    // attr_id
	ValueType     ValueType                 `json:"value_type"` // 'text', 'numeric', 'date', 'bool'
	Storage       *AttributeStorageMetadata `json:"storage,omitempty"`
}

// IsInsideArray infers if the attribute is inside an array based on its name.
func (m AttributeMetadata) IsInsideArray() bool {
	return strings.Contains(m.AttributeName, "[")
}

// SchemaAttributeCache is a mapping of attr_name -> metadata.
// Strongly recommended to populate per schema_id at application startup.
type SchemaAttributeCache map[string]AttributeMetadata

// SchemaMetadata aggregates attribute mappings for a schema version.
type SchemaMetadata struct {
	SchemaName    string              `json:"schema_name"`
	SchemaID      int16               `json:"schema_id"`
	SchemaVersion int                 `json:"schema_version"`
	Attributes    []AttributeMetadata `json:"attributes"`
}

func buildStorageMetadata(valueType ValueType, binding *MainColumnBinding) AttributeStorageMetadata {
	metadata := AttributeStorageMetadata{
		Location:      AttributeStorageLocationEAV,
		ColumnBinding: binding,
		Fallback:      AttributeFallbackMetadata{Kind: AttributeFallbackKindNone},
	}

	if binding == nil {
		return metadata
	}

	metadata.Location = AttributeStorageLocationMain
	metadata.Fallback = inferFallbackMetadata(valueType, binding)
	return metadata
}

func (m *MainColumnBinding) ColumnType() MainColumnType {
	name := strings.ToLower(string(m.ColumnName))
	switch {
	case strings.HasPrefix(name, "text"):
		return MainColumnTypeText
	case strings.HasPrefix(name, "smallint"):
		return MainColumnTypeSmallint
	case strings.HasPrefix(name, "integer"):
		return MainColumnTypeInteger
	case strings.HasPrefix(name, "bigint"):
		return MainColumnTypeBigint
	case strings.HasPrefix(name, "double"):
		return MainColumnTypeDouble
	case strings.HasPrefix(name, "uuid"):
		return MainColumnTypeUUID
	default:
		return MainColumnTypeText
	}
}

func inferFallbackMetadata(valueType ValueType, binding *MainColumnBinding) AttributeFallbackMetadata {
	fb := AttributeFallbackMetadata{
		Kind:       AttributeFallbackKindNone,
		ColumnType: MainColumnType(""),
		Encoding:   MainColumnEncodingDefault,
	}

	if binding == nil {
		return fb
	}

	columnType := binding.ColumnType()
	fb.ColumnType = columnType
	if binding.Encoding != "" {
		fb.Encoding = binding.Encoding
	}

	switch valueType {
	case ValueTypeNumeric:
		if columnType == MainColumnTypeDouble {
			fb.Enabled = true
			fb.Kind = AttributeFallbackKindNumericToDouble
			fb.Notes = "numeric stored as double precision"
		}
	case ValueTypeDate:
		switch columnType {
		case MainColumnTypeDouble:
			fb.Enabled = true
			fb.Kind = AttributeFallbackKindDateToDouble
			fb.Notes = "date stored as double precision"
		case MainColumnTypeText:
			fb.Enabled = true
			fb.Kind = AttributeFallbackKindDateToText
			if binding.Encoding == MainColumnEncodingISO8601 {
				fb.Notes = "date stored as ISO8601 text"
			} else {
				fb.Notes = "date stored as text"
			}
		}
	case ValueTypeBool:
		switch columnType {
		case MainColumnTypeDouble:
			fb.Enabled = true
			fb.Kind = AttributeFallbackKindBoolToDouble
			fb.Notes = "bool stored as double"
		case MainColumnTypeText:
			fb.Enabled = true
			fb.Kind = AttributeFallbackKindBoolToText
			fb.Notes = "bool stored as text"
		}
		if binding.Encoding == MainColumnEncodingBoolText {
			fb.Enabled = true
			fb.Kind = AttributeFallbackKindBoolToText
			fb.Notes = "bool stored as text encoding"
		}
	case ValueTypeText:
		// no-op; storing text anywhere else would be invalid today
	}

	return fb
}
