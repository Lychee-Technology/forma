package sortkey

import (
	"bytes"
	"fmt"
	"math"
	"strings"

	"github.com/google/uuid"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// Column is one key of a page's order.
type Column struct {
	// Name is the column the key is read from: a system column of the
	// visible CTE, or the attribute's name.
	Name string
	Type ExportType
	Desc bool
	// NullReadsZero reads a NULL key as 0, as the keyset renderer reads
	// COALESCE(deleted_ts, 0). Set on deleted_ts only.
	NullReadsZero bool
}

// Order is the order of one page's parquet leg, key for key and direction
// for direction, row_id last. The comparator does not fix an order of its
// own: it is built for each pass from the ORDER BY the parquet leg renders
// (design.md §6.1, The page's order).
type Order struct {
	Columns []Column
}

// PageOrder returns the order the parquet leg renders for q's page: the
// keyset form's cursor columns when q carries an active cursor, else the
// offset form's ORDER BY. The cursor must be normalized (NormalizeCursor).
func PageOrder(q *model.FederatedAttributeQuery) (Order, error) {
	if q == nil {
		return OffsetOrder(nil)
	}
	if q.KeysetCursor.IsActive() {
		return KeysetOrder(q.KeysetCursor)
	}
	return OffsetOrder(q.AttributeOrders)
}

// OffsetOrder mirrors sqlgen's buildNonKeysetOrderBy: the resolved sort keys
// in order, or created_at DESC when there are none, then row_id ASC. A key
// with neither a column nor an attribute name renders nothing there and is
// skipped here too.
func OffsetOrder(orders []model.AttributeOrder) (Order, error) {
	var cols []Column
	for _, ao := range orders {
		name := ao.AttrName
		if name == "" {
			name = ao.ColumnName
		}
		if name == "" {
			continue
		}
		spec, err := orderSpec(ao)
		if err != nil {
			return Order{}, fmt.Errorf("offset order: %w", err)
		}
		cols = append(cols, Column{Name: name, Type: spec.typ, Desc: ao.Desc()})
	}
	if len(cols) == 0 {
		cols = []Column{{Name: ColumnCreatedAt, Type: BigInt, Desc: true}}
	}
	cols = append(cols, Column{Name: ColumnRowID, Type: UUID})
	return Order{Columns: cols}, nil
}

// KeysetOrder mirrors sqlgen's buildKeysetOrderBy: the cursor's columns, each
// in its own direction, row_id included in the direction the cursor gives
// it. The cursor must be normalized, so that every column carries its export
// type.
func KeysetOrder(cursor *model.KeysetCursor) (Order, error) {
	if !cursor.IsActive() {
		return Order{}, fmt.Errorf("keyset order: the cursor is not active, so the page renders the offset order")
	}
	cols := make([]Column, 0, len(cursor.Columns))
	for _, c := range cursor.Columns {
		typ, err := ParseExportType(c.ExportType)
		if err != nil {
			return Order{}, fmt.Errorf("keyset order: cursor column %q is not normalized: %w", c.Attribute, err)
		}
		_, sys, _ := systemColumn(c.Attribute)
		cols = append(cols, Column{
			Name:          c.Attribute,
			Type:          typ,
			Desc:          c.Direction == forma.SortOrderDesc,
			NullReadsZero: sys == ColumnDeletedTS,
		})
	}
	return Order{Columns: cols}, nil
}

// Compare orders two key tuples as the parquet leg's ORDER BY does, with
// NULLS LAST on every key in both directions. Each tuple holds one value per
// column, of the column's export type.
func (o Order) Compare(a, b []any) int {
	for i, col := range o.Columns {
		av, bv := col.read(a[i]), col.read(b[i])
		switch {
		case av == nil && bv == nil:
			continue
		case av == nil:
			return 1
		case bv == nil:
			return -1
		}
		c := compareValues(col.Type, av, bv)
		if col.Desc {
			c = -c
		}
		if c != 0 {
			return c
		}
	}
	return 0
}

// CheckKey reports whether key fits the order: one value per column, each
// nil or of its column's Go type. Derivation and normalization produce only
// such keys; the check is for a caller that builds one otherwise.
func (o Order) CheckKey(key []any) error {
	if len(key) != len(o.Columns) {
		return fmt.Errorf("sort key holds %d value(s) for an order of %d column(s)", len(key), len(o.Columns))
	}
	for i, col := range o.Columns {
		if key[i] != nil && !hasGoType(col.Type, key[i]) {
			return fmt.Errorf("sort key value %d for column %q is %T, not the Go type of its export type %s", i+1, col.Name, key[i], col.Type)
		}
	}
	return nil
}

func (col Column) read(v any) any {
	if v == nil && col.NullReadsZero {
		return int64(0)
	}
	return v
}

func hasGoType(t ExportType, v any) bool {
	switch v.(type) {
	case int64:
		return t.isInteger()
	case float64:
		return t == Double
	case string:
		return t == Varchar
	case bool:
		return t == Boolean
	case uuid.UUID:
		return t == UUID
	default:
		return false
	}
}

// compareValues compares two non-NULL values of type t in DuckDB's ascending
// order. Values of another Go type compare as equal; CheckKey refuses them.
func compareValues(t ExportType, a, b any) int {
	switch {
	case t.isInteger():
		x, _ := a.(int64)
		y, _ := b.(int64)
		return cmpOrdered(x, y)
	case t == Double:
		x, _ := a.(float64)
		y, _ := b.(float64)
		return CompareDouble(x, y)
	case t == Varchar:
		x, _ := a.(string)
		y, _ := b.(string)
		// Byte order: DuckDB's default binary collation.
		return strings.Compare(x, y)
	case t == Boolean:
		x, _ := a.(bool)
		y, _ := b.(bool)
		return cmpBool(x, y)
	case t == UUID:
		x, _ := a.(uuid.UUID)
		y, _ := b.(uuid.UUID)
		// DuckDB's UUID order is the order of the 16 bytes, which is the
		// order of the canonical text.
		return bytes.Compare(x[:], y[:])
	default:
		return 0
	}
}

// CompareDouble is DuckDB's DOUBLE order: NaN equals NaN and sorts above
// +Infinity, and -0 equals 0 (TestDuckDBDoubleOrder pins both against the
// engine).
func CompareDouble(x, y float64) int {
	xn, yn := math.IsNaN(x), math.IsNaN(y)
	switch {
	case xn && yn:
		return 0
	case xn:
		return 1
	case yn:
		return -1
	case x < y:
		return -1
	case x > y:
		return 1
	default:
		return 0
	}
}

func cmpOrdered[T int64 | string](x, y T) int {
	switch {
	case x < y:
		return -1
	case x > y:
		return 1
	default:
		return 0
	}
}

func cmpBool(x, y bool) int {
	switch {
	case x == y:
		return 0
	case !x:
		return -1
	default:
		return 1
	}
}
