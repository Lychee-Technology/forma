package sqlgen

import (
	"github.com/lychee-technology/forma"
)

// eavValueColumn returns the appropriate eav_data column for a given value type.
func eavValueColumn(vt forma.ValueType) string {
	switch vt {
	case forma.ValueTypeNumeric, forma.ValueTypeInteger, forma.ValueTypeBigInt,
		forma.ValueTypeSmallInt, forma.ValueTypeDate, forma.ValueTypeDateTime,
		forma.ValueTypeBool:
		return "value_numeric"
	default:
		return "value_text"
	}
}
