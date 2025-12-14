package forma

import (
	"strings"
)

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

// MainColumn represents column names in the main entity table.
type MainColumn string

const (
	MainColumnText01     MainColumn = "text_01"
	MainColumnText02     MainColumn = "text_02"
	MainColumnText03     MainColumn = "text_03"
	MainColumnText04     MainColumn = "text_04"
	MainColumnText05     MainColumn = "text_05"
	MainColumnText06     MainColumn = "text_06"
	MainColumnText07     MainColumn = "text_07"
	MainColumnText08     MainColumn = "text_08"
	MainColumnText09     MainColumn = "text_09"
	MainColumnText10     MainColumn = "text_10"
	MainColumnSmallint01 MainColumn = "smallint_01"
	MainColumnSmallint02 MainColumn = "smallint_02"
	MainColumnInteger01  MainColumn = "integer_01"
	MainColumnInteger02  MainColumn = "integer_02"
	MainColumnInteger03  MainColumn = "integer_03"
	MainColumnBigint01   MainColumn = "bigint_01"
	MainColumnBigint02   MainColumn = "bigint_02"
	MainColumnBigint03   MainColumn = "bigint_03"
	MainColumnBigint04   MainColumn = "bigint_04"
	MainColumnBigint05   MainColumn = "bigint_05"
	MainColumnDouble01   MainColumn = "double_01"
	MainColumnDouble02   MainColumn = "double_02"
	MainColumnDouble03   MainColumn = "double_03"
	MainColumnDouble04   MainColumn = "double_04"
	MainColumnDouble05   MainColumn = "double_05"
	MainColumnUUID01     MainColumn = "uuid_01"
	MainColumnUUID02     MainColumn = "uuid_02"
	MainColumnCreatedAt  MainColumn = "ltbase_created_at"
	MainColumnUpdatedAt  MainColumn = "ltbase_updated_at"
	MainColumnDeletedAt  MainColumn = "ltbase_deleted_at"
	MainColumnSchemaID   MainColumn = "ltbase_schema_id"
	MainColumnRowID      MainColumn = "ltbase_row_id"
)

// MainColumnType represents the data type of a main column.
type MainColumnType string

const (
	MainColumnTypeText     MainColumnType = "text"
	MainColumnTypeSmallint MainColumnType = "smallint"
	MainColumnTypeInteger  MainColumnType = "integer"
	MainColumnTypeBigint   MainColumnType = "bigint"
	MainColumnTypeDouble   MainColumnType = "double"
	MainColumnTypeUUID     MainColumnType = "uuid"
)

// MainColumnEncoding represents special encoding for main column values.
type MainColumnEncoding string

const (
	MainColumnEncodingDefault  MainColumnEncoding = "default"
	MainColumnEncodingBoolText MainColumnEncoding = "bool_text" // "true"/"false" string
	MainColumnEncodingUnixMs   MainColumnEncoding = "unix_ms"
	MainColumnEncodingBoolInt  MainColumnEncoding = "bool_smallint"
	MainColumnEncodingISO8601  MainColumnEncoding = "iso8601"
)

// MainColumnBinding describes how a schema attribute maps into a hot attribute column.
type MainColumnBinding struct {
	ColumnName MainColumn         `json:"col_name"`
	Encoding   MainColumnEncoding `json:"encoding,omitempty"`
}

// ColumnType derives the column type from the column name prefix.
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
	case (m.ColumnName == MainColumnCreatedAt || m.ColumnName == MainColumnUpdatedAt || m.ColumnName == MainColumnDeletedAt):
		return MainColumnTypeBigint
	case m.ColumnName == MainColumnSchemaID:
		return MainColumnTypeSmallint
	case m.ColumnName == MainColumnRowID:
		return MainColumnTypeUUID
	default:
		return MainColumnTypeText
	}
}

// AttributeMetadata stores cached metadata from the attributes table.
type AttributeMetadata struct {
	AttributeName string             `json:"attr_name"`  // attr_name, JSON Path
	AttributeID   int16              `json:"attr_id"`    // attr_id
	ValueType     ValueType          `json:"value_type"` // 'text', 'numeric', 'date', 'bool'
	ColumnBinding *MainColumnBinding `json:"column_binding,omitempty"`
}

// AttributeStorageLocation enumerates where the attribute physically resides.
type AttributeStorageLocation string

const (
	AttributeStorageLocationUnknown AttributeStorageLocation = ""
	AttributeStorageLocationMain    AttributeStorageLocation = "main"
	AttributeStorageLocationEAV     AttributeStorageLocation = "eav"
)

// IsInsideArray infers if the attribute is inside an array based on its name.
func (m AttributeMetadata) IsInsideArray() bool {
	return strings.Contains(m.AttributeName, "[")
}

// Location returns where the attribute is stored.
// If ColumnBinding is nil, the attribute is in EAV; otherwise in main table.
func (m AttributeMetadata) Location() AttributeStorageLocation {
	if m.ColumnBinding == nil {
		return AttributeStorageLocationEAV
	}
	return AttributeStorageLocationMain
}

// SchemaAttributeCache is a mapping of attr_name -> metadata.
// Strongly recommended to populate per schema_id at application startup.
type SchemaAttributeCache map[string]AttributeMetadata

// SchemaRegistry provides schema lookup operations.
// Implementations can load schemas from files, databases, or other sources.
type SchemaRegistry interface {
	// GetSchemaAttributeCacheByName retrieves schema ID and attribute cache by schema name
	GetSchemaAttributeCacheByName(name string) (int16, SchemaAttributeCache, error)
	// GetSchemaAttributeCacheByID retrieves schema name and attribute cache by schema ID
	GetSchemaAttributeCacheByID(id int16) (string, SchemaAttributeCache, error)
	// ListSchemas returns a list of all registered schema names

	GetSchemaByName(name string) (int16, JSONSchema, error)
	// GetSchemaAttributeCacheByID retrieves schema name and attribute cache by schema ID
	GetSchemaByID(id int16) (string, JSONSchema, error)
	ListSchemas() []string
}
