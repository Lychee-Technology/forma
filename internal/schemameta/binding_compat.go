package schemameta

import (
	"fmt"

	"github.com/lychee-technology/forma"
)

// ValidateColumnBinding reports whether an attribute's valueType can
// round-trip through its main-column binding (#459). A binding outside the
// matrix below used to surface only at write time — as a redacted 500
// (text→uuid column: uuid.Parse failure) or a silent drop (text→smallint:
// empty numeric slot) — so registration refuses it up front, and
// validate-schema-consistency reports it across an already-deployed set.
//
//	valueType                        column type                         encoding
//	text                             text                                default
//	uuid                             uuid                                default
//	smallint/integer/bigint/numeric  smallint, integer, bigint, double   default (width enforced at write)
//	date/datetime                    bigint                              default or unix_ms
//	date/datetime                    text                                iso8601
//	bool                             smallint                            bool_smallint
//	bool                             text                                bool_text
//	list                             —                                   never bindable
//
// Width narrowing inside the numeric family is deliberately admitted:
// numeric→smallint is a shipped shape, and transform.checkStorageFit rejects
// any value that does not fit the column. System columns (ltbase_*) go through
// the same matrix via ColumnType(). A nil binding is always valid.
//
// The write path, the Postgres read path and the CDC export round-trip every
// admitted pair. For date/datetime → text (iso8601) the DuckDB hot-leg
// projection is unverified; see #555.
func ValidateColumnBinding(attrName string, meta forma.AttributeMetadata) error {
	binding := meta.ColumnBinding
	if binding == nil {
		return nil
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
