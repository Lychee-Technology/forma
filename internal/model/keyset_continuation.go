package model

import (
	"fmt"

	"github.com/lychee-technology/forma"
)

// ValidateContinuation enforces that an active cursor CONTINUES the order the
// request resolved, rather than replacing it (#381 review).
//
// The DuckDB renderer builds the keyset ORDER BY from the cursor's columns and
// never reads AttributeOrders while a cursor is active, so before this rule a
// caller that sorted page one by `count ASC` and then sent a valid
// `created_at DESC, row_id ASC` cursor received a page ordered and filtered
// on created_at, answered successfully — the #354 silent-wrong-answer family.
//
// The rule has two halves, and the boundary between them is deliberate:
//
//   - AttributeOrders PRESENT: the cursor must be exactly those orders followed
//     by the row_id tiebreak. Column i must name orders[i].AttrName and carry
//     its direction; the trailing row_id must be ascending (or empty, which is
//     ascending), because that is the tiebreak the non-keyset ORDER BY appended
//     to page one (sqlgen.buildNonKeysetOrderBy). A longer or shorter cursor, a
//     different attribute, a flipped direction, or a row_id DESC tiebreak is
//     refused. The attribute name is compared byte-exactly: the sort key was
//     resolved against the schema, so its spelling is the canonical one, and a
//     cursor built from the same request carries the same spelling.
//
//   - AttributeOrders EMPTY: the cursor is honoured as written. The default
//     `created_at DESC, row_id ASC` is the renderer's fallback, not a caller
//     statement, and AttributeOrders cannot express a system-column order at
//     all — sort keys resolve against the schema, and created_at is not a
//     schema attribute. A cursor-only walk (an open first page bounded by a
//     far-future created_at, then cursors on created_at ASC or DESC with a
//     row_id tiebreak in either direction) is a complete order specification
//     with nothing to contradict, and refusing it would make every
//     created_at / ver_ts / deleted_ts cursor unreachable.
//
// An order without an AttrName cannot be continued and is refused by
// position: every production builder sets it (internal.buildAttributeOrders,
// the production e2e harness), and the one that does not (the federated
// benchmark) never combines orders with a cursor.
//
// A plain read-path error, like ValidateShape.
func (c *KeysetCursor) ValidateContinuation(orders []AttributeOrder) error {
	if !c.IsActive() || len(orders) == 0 {
		return nil
	}
	if len(c.Columns) != len(orders)+1 {
		return fmt.Errorf("keyset cursor carries %d column(s) but the request sorts on %d attribute(s): a cursor must continue the request's order — its columns are the sort attributes in order, then the row_id tiebreak — or the page would be ordered by the cursor instead of the sort it continues",
			len(c.Columns), len(orders))
	}
	for i, ao := range orders {
		if err := c.continuesOrder(i, ao); err != nil {
			return err
		}
	}
	tiebreak := c.Columns[len(c.Columns)-1]
	if tiebreak.Direction == forma.SortOrderDesc {
		return fmt.Errorf("keyset cursor tiebreak %q is descending but the sorted page it continues breaks ties on row_id ASC: a cursor must carry the same tiebreak direction as the order it continues, or rows tied on the sort key are paged in a different order than page one",
			tiebreak.Attribute)
	}
	return nil
}

// continuesOrder checks that cursor column i names sort key i with its
// direction. Positions are reported 1-based, alongside both names, because a
// caller reads the mismatch as "the second cursor column should be count".
func (c *KeysetCursor) continuesOrder(i int, ao AttributeOrder) error {
	col := c.Columns[i]
	if ao.AttrName == "" {
		return fmt.Errorf("keyset cursor cannot continue sort key %d: the resolved order carries no attribute name to match cursor column %q against", i+1, col.Attribute)
	}
	if col.Attribute != ao.AttrName {
		return fmt.Errorf("keyset cursor column %d is %q but the request sorts on %q at that position: a cursor must continue the request's order, or the page would be ordered and filtered on an attribute the request never sorted on",
			i+1, col.Attribute, ao.AttrName)
	}
	if (col.Direction == forma.SortOrderDesc) != ao.Desc() {
		return fmt.Errorf("keyset cursor column %d (%q) is %s but the request sorts %q %s: a cursor must continue the request's order, or the page would run against the direction of the page it continues",
			i+1, col.Attribute, directionWord(col.Direction), ao.AttrName, directionWord(ao.SortOrder))
	}
	return nil
}

// directionWord spells a sort order for an error message, reading the empty
// direction as ascending exactly as the renderer does.
func directionWord(dir forma.SortOrder) string {
	if dir == forma.SortOrderDesc {
		return "descending"
	}
	return "ascending"
}
