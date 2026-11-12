package cdc

import (
	"database/sql/driver"
	"fmt"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/lychee-technology/forma/internal/numutil"
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
		return fmt.Errorf("scan numeric: %w", err)
	}
	d.value, d.valid = f, n.Valid
	return nil
}

// numericToDouble is numutil.NumericToDouble, which the hot leg's sort keys
// share (sortkey): a hot key must compare as this export will write it.
func numericToDouble(n pgtype.Numeric) (float64, error) {
	f, err := numutil.NumericToDouble(n)
	if err != nil {
		return 0, fmt.Errorf("stage numeric: %w", err)
	}
	return f, nil
}
