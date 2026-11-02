package cdc

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"strconv"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgtype"
)

// stageRowDecoder holds one reusable pgx scan target per staged column and
// turns a scanned row into the Appender's values.
type stageRowDecoder struct {
	targets []any
	convert []func() driver.Value
	out     []driver.Value
}

func newStageRowDecoder(columns []stageColumn) *stageRowDecoder {
	d := &stageRowDecoder{
		targets: make([]any, len(columns)),
		convert: make([]func() driver.Value, len(columns)),
		out:     make([]driver.Value, len(columns)),
	}
	for i, c := range columns {
		d.targets[i], d.convert[i] = stageScanTarget(c.typ)
	}
	return d
}

// values returns the last scanned row; the slice is reused by the next row.
func (d *stageRowDecoder) values() []driver.Value {
	for i, convert := range d.convert {
		d.out[i] = convert()
	}
	return d.out
}

// stageScanTarget returns the pgx scan target for one staged type and the
// conversion of its scanned value to what the Appender takes; NULL stays
// nil throughout.
func stageScanTarget(typ stageColumnType) (any, func() driver.Value) {
	switch typ {
	case stageSmallint:
		v := &pgtype.Int2{}
		return v, func() driver.Value { return nullable(v.Valid, v.Int16) }
	case stageInteger:
		v := &pgtype.Int4{}
		return v, func() driver.Value { return nullable(v.Valid, v.Int32) }
	case stageBigint:
		v := &pgtype.Int8{}
		return v, func() driver.Value { return nullable(v.Valid, v.Int64) }
	case stageDouble:
		v := &stagedDouble{}
		return v, func() driver.Value { return nullable(v.valid, v.value) }
	case stageUUID:
		v := &pgtype.UUID{}
		return v, func() driver.Value { return nullable(v.Valid, duckdb.UUID(v.Bytes)) }
	default:
		v := &pgtype.Text{}
		return v, func() driver.Value { return nullable(v.Valid, v.String) }
	}
}

func nullable[T any](valid bool, value T) driver.Value {
	if !valid {
		return nil
	}
	return value
}

// stagedDouble decodes a double precision or NUMERIC column to DOUBLE, the
// way the DuckDB Postgres scanner did for the export before #785: a NUMERIC
// becomes the double nearest its decimal value, NaN and ±Infinity stay what
// they are, and a value past the double range becomes ±Infinity. pgx's own
// NUMERIC-to-float64 conversion refuses that last case.
type stagedDouble struct {
	value float64
	valid bool
}

// ScanFloat64 implements pgtype.Float64Scanner for double precision.
func (d *stagedDouble) ScanFloat64(v pgtype.Float8) error {
	d.value, d.valid = v.Float64, v.Valid
	return nil
}

// ScanNumeric implements pgtype.NumericScanner for NUMERIC.
func (d *stagedDouble) ScanNumeric(n pgtype.Numeric) error {
	f, err := numericToDouble(n)
	if err != nil {
		return err
	}
	d.value, d.valid = f, n.Valid
	return nil
}

func numericToDouble(n pgtype.Numeric) (float64, error) {
	switch {
	case !n.Valid:
		return 0, nil
	case n.NaN:
		return math.NaN(), nil
	case n.InfinityModifier == pgtype.Infinity:
		return math.Inf(1), nil
	case n.InfinityModifier == pgtype.NegativeInfinity:
		return math.Inf(-1), nil
	}
	digits := "0"
	if n.Int != nil {
		digits = n.Int.String()
	}
	text := digits + "e" + strconv.FormatInt(int64(n.Exp), 10)
	f, err := strconv.ParseFloat(text, 64)
	if err != nil && !errors.Is(err, strconv.ErrRange) {
		return 0, fmt.Errorf("decode numeric %s as double: %w", text, err)
	}
	// On ErrRange ParseFloat returns the correctly signed infinity (or zero
	// for an underflow), which is the scanner's answer too.
	return f, nil
}
