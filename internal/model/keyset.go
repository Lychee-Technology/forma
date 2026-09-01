package model

import (
	"fmt"

	"github.com/lychee-technology/forma"
)

type KeysetCursorMode string

const (
	KeysetCursorModeAfter  KeysetCursorMode = "after"
	KeysetCursorModeBefore KeysetCursorMode = "before"
)

type KeysetColumn struct {
	Attribute string
	Direction forma.SortOrder
}

type KeysetCursor struct {
	Columns []KeysetColumn
	Values  []interface{}
	Mode    KeysetCursorMode
}

// IsActive reports whether this cursor carries a continuation obligation.
// A nil cursor or one with no columns is the OPEN FIRST PAGE: nothing to
// filter, nothing to refuse.
//
// This predicate lives on the type rather than in internal/federated because
// the sites that decide whether the keyset clause is RENDERED live in
// internal/sqlgen (duckdb_template_renderer.go, duckdb_compiled_query.go) and
// cannot reach an unexported federated helper. They must never disagree with
// the sites that decide whether a cursor is HONOURED OR REFUSED (#381 item 9):
// a cursor the engine accepts but the renderer ignores answers an unfiltered
// page, which is the #354 silent-wrong-answer family.
func (c *KeysetCursor) IsActive() bool {
	return c != nil && len(c.Columns) > 0
}

// ValidateShape enforces the two cursor rules that need no knowledge of the
// physical schema, so they can be checked from any package:
//
//  1. Values must align with Columns one-for-one. A short or nil Values used
//     to bind SQL NULL for the unfilled arm; `col > NULL` is NULL, WHERE
//     treats that as not-true, and every disjunct carrying the arm drops out,
//     so the caller received a SILENTLY EMPTY PAGE rather than an error
//     (#381 item 7).
//  2. The final column must be row_id. The continuation predicate for the last
//     cursor key is a strict inequality, so a cursor ending on a non-unique key
//     excludes every row sharing that key's value at the page boundary — an
//     entire tie group is silently skipped (#183). row_id is the only
//     version-invariant unique column, so ending there gives the composite key
//     a total order.
//
// An inactive cursor is a no-op: the open first page carries no continuation
// obligation, and nothing renders from it.
//
// A plain read-path error, not an ErrInvalidInput-wrapped write-path
// validation carrier.
func (c *KeysetCursor) ValidateShape() error {
	if !c.IsActive() {
		return nil
	}
	if len(c.Values) != len(c.Columns) {
		return fmt.Errorf("keyset cursor carries %d column(s) but %d value(s): every cursor column needs exactly one boundary value, or the unfilled comparison binds SQL NULL and silently returns an empty page",
			len(c.Columns), len(c.Values))
	}
	last := c.Columns[len(c.Columns)-1].Attribute
	if last != "row_id" {
		return fmt.Errorf("keyset cursor final column is %q, expected \"row_id\": a cursor not ending on the unique row_id tiebreak silently skips every row tied on the composite key at the page boundary", last)
	}
	return nil
}
