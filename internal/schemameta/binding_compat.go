package schemameta

import (
	"errors"
	"fmt"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// ErrUnknownMainColumn marks a binding whose col_name is not a column
// entity_main has (#557). validate-schema-consistency matches it to file the
// finding under its own category.
var ErrUnknownMainColumn = errors.New("unknown main column")

// ValidateColumnBinding reports whether an attribute's main-column binding
// names a column entity_main has (#557) and whether its valueType can
// round-trip through that column (#459). Both used to surface only at write
// time — an unknown column as the writer's "unsupported column", a pair
// outside the matrix below as a redacted 500 (text→uuid column: uuid.Parse
// failure) or a silent drop (text→smallint: empty numeric slot) — so
// registration refuses them up front, and validate-schema-consistency
// reports them across an already-deployed set.
//
// The column set is the runtime's (internal/model): the writer's allowlist,
// the read projection and the CDC column order all derive from it, so a
// name outside it cannot round-trip whatever ColumnType() infers from its
// prefix. "text_99" or "foo" classify as text and would pass the matrix
// alone. The match is exact: the writer's map lookup is case-sensitive.
//
//	valueType                        column type                         encoding
//	text                             text                                default
//	uuid                             uuid                                default
//	smallint/integer/bigint/numeric  smallint, integer, bigint, double   default (width enforced at write)
//	date/datetime                    bigint                              default or unix_ms (full int64 epoch-ms range)
//	date/datetime                    text                                iso8601 (whole seconds, years 0000–9999 enforced at write)
//
// An unbound date/datetime lives in eav_data.value_numeric as a float64
// image and keeps |epoch millis| <= 2^53; the funnel refuses the rest at
// write time (#582), so the per-destination ranges above are the contract.
//
//	bool                             smallint                            bool_smallint
//	bool                             text                                bool_text
//	list                             —                                   never bindable
//
// Width narrowing inside the numeric family is deliberately admitted:
// numeric→smallint is a shipped shape, and transform.checkStorageFit rejects
// any value that does not fit the column. System columns (ltbase_*) go through
// the same matrix via ColumnType(). A nil binding is always valid.
//
// The write path, the Postgres read path, the CDC export and the DuckDB
// federated reader (hot-leg projection and outer select, #555) round-trip
// every admitted pair.
func ValidateColumnBinding(attrName string, meta forma.AttributeMetadata) error {
	binding := meta.ColumnBinding
	if binding == nil {
		return nil
	}
	if !model.IsMainTableColumn(string(binding.ColumnName)) {
		return fmt.Errorf(
			"attribute %s (valueType %s) binds to %w %s: column_binding.col_name must be one of %s",
			attrName, meta.ValueType, ErrUnknownMainColumn, binding.ColumnName, model.EntityMainProjection)
	}
	encoding := binding.Encoding
	if encoding == "" {
		encoding = forma.MainColumnEncodingDefault
	}
	colType := binding.ColumnType()
	if bindingRoundTrips(meta.ValueType, colType, encoding) {
		return nil
	}
	return fmt.Errorf(
		"attribute %s (valueType %s) cannot round-trip through main column %s (%s column, encoding %s): %s",
		attrName, meta.ValueType, binding.ColumnName, colType, encoding, bindableColumnsFor(meta.ValueType, encoding))
}

func bindingRoundTrips(vt forma.ValueType, colType forma.MainColumnType, enc forma.MainColumnEncoding) bool {
	isTime := vt == forma.ValueTypeDate || vt == forma.ValueTypeDateTime
	switch enc {
	case forma.MainColumnEncodingDefault:
		switch vt {
		case forma.ValueTypeText:
			return colType == forma.MainColumnTypeText
		case forma.ValueTypeUUID:
			return colType == forma.MainColumnTypeUUID
		case forma.ValueTypeSmallInt, forma.ValueTypeInteger, forma.ValueTypeBigInt, forma.ValueTypeNumeric:
			switch colType {
			case forma.MainColumnTypeSmallint, forma.MainColumnTypeInteger, forma.MainColumnTypeBigint, forma.MainColumnTypeDouble:
				return true
			}
			return false
		case forma.ValueTypeDate, forma.ValueTypeDateTime:
			return colType == forma.MainColumnTypeBigint
		}
		return false
	case forma.MainColumnEncodingUnixMs:
		return isTime && colType == forma.MainColumnTypeBigint
	case forma.MainColumnEncodingISO8601:
		return isTime && colType == forma.MainColumnTypeText
	case forma.MainColumnEncodingBoolInt:
		return vt == forma.ValueTypeBool && colType == forma.MainColumnTypeSmallint
	case forma.MainColumnEncodingBoolText:
		return vt == forma.ValueTypeBool && colType == forma.MainColumnTypeText
	}
	return false
}

// bindableColumnsFor names the expected state for the error: what this
// valueType may bind to, or why it binds to nothing.
func bindableColumnsFor(vt forma.ValueType, enc forma.MainColumnEncoding) string {
	switch enc {
	case forma.MainColumnEncodingDefault, forma.MainColumnEncodingUnixMs, forma.MainColumnEncodingISO8601,
		forma.MainColumnEncodingBoolInt, forma.MainColumnEncodingBoolText:
	default:
		return fmt.Sprintf("unknown encoding %q; use default, unix_ms, iso8601, bool_smallint or bool_text", enc)
	}
	switch vt {
	case forma.ValueTypeText:
		return "text binds only to text columns (default encoding)"
	case forma.ValueTypeUUID:
		return "uuid binds only to uuid columns (default encoding)"
	case forma.ValueTypeSmallInt, forma.ValueTypeInteger, forma.ValueTypeBigInt, forma.ValueTypeNumeric:
		return string(vt) + " binds only to smallint, integer, bigint or double columns (default encoding; values must fit the column width)"
	case forma.ValueTypeDate, forma.ValueTypeDateTime:
		return string(vt) + " binds only to bigint columns (default or unix_ms encoding) or text columns (iso8601 encoding)"
	case forma.ValueTypeBool:
		return "bool binds only to smallint columns (bool_smallint encoding) or text columns (bool_text encoding)"
	case forma.ValueTypeList:
		return "list attributes store one element per eav_data row and have no scalar column form"
	}
	return fmt.Sprintf("unknown valueType %q has no main column form", vt)
}
