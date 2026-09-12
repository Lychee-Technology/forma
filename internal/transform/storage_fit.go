package transform

import (
	"fmt"
	"math"
	"strconv"

	"github.com/google/uuid"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// checkStorageFit is the single write-side fidelity rule shared by #384 and
// #459: a value must fit its physical destination — the bound main column
// when the attribute has one, its declared type otherwise. populateTypedValue
// has already typed the value for the declared valueType; this decides
// whether that typed value can be stored losslessly where it is going.
// Every error returned here is wrapped into forma.InvalidInputf by the
// caller: it describes the caller's value, so it is published.
//
// The declared-type check runs first even for bound attributes: the column
// may be wider than the declared type (smallint declared, integer column),
// and the declared type is what every DuckDB tier projects by (#384).
func checkStorageFit(attr *model.EAVRecord, meta forma.AttributeMetadata) error {
	if isNumericFamily(meta.ValueType) && attr.ValueNumeric != nil {
		if err := checkIntegerFit(attr, meta.ValueType, "declared type "+string(meta.ValueType)); err != nil {
			return err
		}
	}
	binding := meta.ColumnBinding
	if binding == nil || isSystemManagedColumn(binding.ColumnName) {
		return nil
	}
	return checkBoundColumnFit(attr, meta.ValueType, binding)
}

// checkBoundColumnFit mirrors storeInMainColumn's dispatch: the same
// (encoding, column type) decides which EAVRecord slot is serialized, so the
// slot must be populated. Every value serialized from the numeric slot into a
// smallint, integer or bigint column — a numeric-family value, epoch millis
// (date/datetime, default or unix_ms) or 0/1 (bool_smallint) — is then
// width-checked against the COLUMN's width, whatever the declared type or
// encoding: epoch millis (~1.7e12) overflow integer and smallint, and
// storeWithDefaultEncoding's int16()/int32() narrowing wraps (#459). The
// registration matrix refuses date→integer, but the funnel must not depend on
// registration: a deployed or programmatic registry can supply the binding.
func checkBoundColumnFit(attr *model.EAVRecord, vt forma.ValueType, binding *forma.MainColumnBinding) error {
	col := binding.ColumnName
	colType := binding.ColumnType()
	switch binding.Encoding {
	case forma.MainColumnEncodingBoolText, forma.MainColumnEncodingISO8601:
		// Text renderings of the numeric slot: nothing to width-check.
		if attr.ValueNumeric == nil {
			return errSlotMismatch(vt, col, "a date, datetime or bool value")
		}
		return nil
	case forma.MainColumnEncodingUnixMs, forma.MainColumnEncodingBoolInt:
		// Integer renderings of the numeric slot; width-checked below.
		if attr.ValueNumeric == nil {
			return errSlotMismatch(vt, col, "a date, datetime or bool value")
		}
	default:
		if err := checkDefaultEncodingSlot(attr, vt, col, colType); err != nil {
			return err
		}
	}
	if fitType, ok := columnFitType(colType); ok {
		return checkIntegerFit(attr, fitType, fmt.Sprintf("bound column %s (%s)", col, colType))
	}
	return nil
}

// checkDefaultEncodingSlot verifies that the slot storeWithDefaultEncoding
// serializes for colType is populated (and, for uuid columns, parseable).
// On success an integer or double column is guaranteed a non-nil numeric slot.
func checkDefaultEncodingSlot(attr *model.EAVRecord, vt forma.ValueType, col forma.MainColumn, colType forma.MainColumnType) error {
	switch colType {
	case forma.MainColumnTypeText:
		if attr.ValueText == nil {
			return errSlotMismatch(vt, col, "a text value")
		}
	case forma.MainColumnTypeUUID:
		if attr.ValueText == nil {
			return errSlotMismatch(vt, col, "a UUID value")
		}
		if _, err := uuid.Parse(*attr.ValueText); err != nil {
			return fmt.Errorf("%s value %q is not a UUID: bound column %s requires one", vt, *attr.ValueText, col)
		}
	case forma.MainColumnTypeSmallint, forma.MainColumnTypeInteger, forma.MainColumnTypeBigint, forma.MainColumnTypeDouble:
		if attr.ValueNumeric == nil {
			return errSlotMismatch(vt, col, "a numeric value")
		}
	default:
		return fmt.Errorf("attribute bound to column %s of unsupported type %s", col, colType)
	}
	return nil
}

func errSlotMismatch(vt forma.ValueType, col forma.MainColumn, expects string) error {
	return fmt.Errorf("%s value cannot be stored in main column %s, which stores %s: the attribute's valueType and column binding disagree", vt, col, expects)
}

// isNumericFamily reports the valueTypes whose magnitude is caller-chosen, so
// checkStorageFit runs the declared-type width check on them (numeric itself
// passes that check unconstrained, #205). date/datetime and bool also occupy
// the numeric slot (epoch millis, 0/1); their declared type carries no width,
// and only the bound column's width (checkBoundColumnFit) constrains them.
func isNumericFamily(vt forma.ValueType) bool {
	switch vt {
	case forma.ValueTypeSmallInt, forma.ValueTypeInteger, forma.ValueTypeBigInt, forma.ValueTypeNumeric:
		return true
	}
	return false
}

// columnFitType maps a narrow integer main column to the declared type whose
// range equals the column's storage width.
func columnFitType(colType forma.MainColumnType) (forma.ValueType, bool) {
	switch colType {
	case forma.MainColumnTypeSmallint:
		return forma.ValueTypeSmallInt, true
	case forma.MainColumnTypeInteger:
		return forma.ValueTypeInteger, true
	case forma.MainColumnTypeBigint:
		return forma.ValueTypeBigInt, true
	}
	return "", false
}

// checkIntegerFit rejects a value in the numeric slot that cannot fit the
// integer width vt names. dest labels the destination in the message
// ("declared type integer", "bound column smallint_01 (smallint)").
// eav_data.value_numeric is an unconstrained NUMERIC and Go's int16()/int32()
// conversions wrap, so this funnel is the only place the width can be
// enforced (#384, #459). numeric stays unconstrained (#205 owns its float64
// ceiling). The caller guarantees attr.ValueNumeric is non-nil.
//
// The check judges the slot the store consumes, not the caller's raw value:
// storeWithDefaultEncoding's bigint arm and the unix_ms arm write the exact
// ValueInt64 sidecar when it is populated and int64(*ValueNumeric) otherwise,
// and populateTypedValue fills the sidecar for declared bigint and
// date/datetime only. A numeric-declared 9223372036854775807 is therefore
// stored from its float64 image (2^63, which int64() wraps), so the image is
// what must fit; deriving an exact int64 from the raw value here would admit
// it (#459 review F1).
func checkIntegerFit(attr *model.EAVRecord, vt forma.ValueType, dest string) error {
	numVal := *attr.ValueNumeric
	var lo, hi float64
	switch vt {
	case forma.ValueTypeSmallInt:
		lo, hi = math.MinInt16, math.MaxInt16
	case forma.ValueTypeInteger:
		lo, hi = math.MinInt32, math.MaxInt32
	case forma.ValueTypeBigInt:
		// The exact sidecar is an int64 and fits by construction; it is also
		// how a declared bigint admits boundary literals like
		// "9223372036854775807", whose float64 image rounds up to 2^63 and
		// would fail the bound check below.
		if attr.ValueInt64 != nil {
			return nil
		}
		// Constant conversion: math.MinInt64 converts to exactly -2^63
		// (a valid value); math.MaxInt64 rounds up to exactly 2^63, so >=
		// rejects the first float64 that no longer fits.
		if numVal != math.Trunc(numVal) {
			return errNonIntegralFor(numVal, dest)
		}
		if numVal < math.MinInt64 || numVal >= math.MaxInt64 {
			return fmt.Errorf("value %s out of range for %s (allowed [-9223372036854775808, 9223372036854775807])", formatFitValue(numVal), dest)
		}
		return nil
	default:
		return nil
	}
	if numVal != math.Trunc(numVal) {
		return errNonIntegralFor(numVal, dest)
	}
	if numVal < lo || numVal > hi {
		return fmt.Errorf("value %s out of range for %s (allowed [%.0f, %.0f])", formatFitValue(numVal), dest, lo, hi)
	}
	return nil
}

func errNonIntegralFor(numVal float64, dest string) error {
	return fmt.Errorf("non-integral value %s does not fit %s (whole number required)", formatFitValue(numVal), dest)
}

// formatFitValue renders the rejected value in plain digits for the
// magnitudes an integer column can be asked to hold (epoch millis print as
// 1704067200000, not 1.7040672e+12). An integral value prints exactly, so a
// caller who sent 9223372036854775807 sees the 9223372036854775808 its
// float64 image became. Beyond 1e21 the shortest round-trip form keeps 1e300
// from becoming 301 digits.
func formatFitValue(numVal float64) string {
	if math.Abs(numVal) >= 1e21 {
		return strconv.FormatFloat(numVal, 'g', -1, 64)
	}
	if numVal == math.Trunc(numVal) {
		return strconv.FormatFloat(numVal, 'f', 0, 64)
	}
	return strconv.FormatFloat(numVal, 'f', -1, 64)
}
