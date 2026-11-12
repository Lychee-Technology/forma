package sortkey

import "fmt"

// Admits applies the keyset predicate the parquet leg renders
// (sqlgen.generateKeysetWhereClause) to one key tuple, for an after cursor
// whose boundary values are bounds: an OR over the cursor's columns, the
// i-th disjunct holding when every earlier key equals its boundary and the
// i-th key lies past its boundary in its column's direction (> ascending,
// < descending, as keysetComparisonOp chooses).
//
// The predicate is three-valued, as SQL's is: a NULL key makes its
// comparison unknown, the disjunct carrying it is not true, and the row is
// admitted only by a disjunct that is. So a NULL key is not "after" the
// cursor although the comparator sorts it last. Non-NULL operands compare by
// the comparator's value order, so a NaN boundary behaves as on DuckDB.
// deleted_ts reads NULL as 0, as the renderer's COALESCE does.
//
// The order must be the cursor's (KeysetOrder), and the bounds its
// normalized values (NormalizeCursor), none of them nil.
func (o Order) Admits(key, bounds []any) bool {
	for i := range o.Columns {
		if o.disjunct(i, key, bounds) {
			return true
		}
	}
	return false
}

// disjunct evaluates the i-th arm: equal on every column before i, and past
// the bound at column i. Unknown reads as not true.
func (o Order) disjunct(i int, key, bounds []any) bool {
	for j := 0; j < i; j++ {
		c, known := o.compareToBound(j, key, bounds)
		if !known || c != 0 {
			return false
		}
	}
	c, known := o.compareToBound(i, key, bounds)
	if !known {
		return false
	}
	if o.Columns[i].Desc {
		return c < 0
	}
	return c > 0
}

// compareToBound compares key[i] with bounds[i] in ascending value order;
// known is false when either operand is NULL.
func (o Order) compareToBound(i int, key, bounds []any) (int, bool) {
	col := o.Columns[i]
	k, b := col.read(key[i]), col.read(bounds[i])
	if k == nil || b == nil {
		return 0, false
	}
	return compareValues(col.Type, k, b), true
}

// CheckBounds reports whether bounds can serve as the cursor of this order:
// one non-nil value per column, of its column's Go type.
func (o Order) CheckBounds(bounds []any) error {
	if err := o.CheckKey(bounds); err != nil {
		return fmt.Errorf("keyset bounds: %w", err)
	}
	for i, b := range bounds {
		if b == nil {
			return fmt.Errorf("keyset bound %d for column %q is nil: a NULL bound admits no row on the parquet leg, and ValidateShape refuses it", i+1, o.Columns[i].Name)
		}
	}
	return nil
}
