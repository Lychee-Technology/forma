package model

import (
	"fmt"
	"reflect"
	"strings"

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

// ValidateShape enforces the cursor rules that need no knowledge of the
// physical schema, so they can be checked from any package:
//
//  1. Values must align with Columns one-for-one. A short or nil Values used
//     to bind SQL NULL for the unfilled arm; `col > NULL` is NULL, WHERE
//     treats that as not-true, and every disjunct carrying the arm drops out,
//     so the caller received a SILENTLY EMPTY PAGE rather than an error
//     (#381 item 7).
//
//  2. The final column must be row_id. The continuation predicate for the last
//     cursor key is a strict inequality, so a cursor ending on a non-unique key
//     excludes every row sharing that key's value at the page boundary — an
//     entire tie group is silently skipped (#183). row_id is the only
//     version-invariant unique column, so ending there gives the composite key
//     a total order.
//
//     The comparison is case-insensitive because DuckDB resolves an unquoted
//     identifier without regard to case: an emitted "ROW_ID" reaches the very
//     column "row_id" does, so it IS the tiebreak and refusing it would
//     contradict the case-insensitive system-column rule the federated
//     validator applies one layer up (#381). It is case-insensitive and
//     nothing more — a name that merely FOLDS onto row_id, like "row.id", is
//     not the tiebreak and is refused here, matching that validator's refusal
//     of any non-identity fold onto a system column.
//
//  3. Every boundary value must be non-nil. Alignment alone does not stop a
//     nil: it binds SQL NULL just as an unfilled arm does, and `row_id > NULL`
//     is unknown, so WHERE drops every disjunct carrying it. The caller gets a
//     silently empty or short page — item 1's failure with the count right.
//
//  4. Mode must be after. `keysetComparisonOp` computes
//     `mode == KeysetCursorModeAfter`, so ANY other value — the zero value most
//     of all — means BEFORE, and a cursor that lost its mode paginates
//     backwards while answering successfully.
//
//     Before is declared (KeysetCursorModeBefore) but refused too, until #513
//     lands the other half of it: the renderer flips the comparison operator
//     for a before cursor yet still fetches in the FORWARD order under the
//     LIMIT, so `key < 7 ORDER BY key ASC LIMIT 2` answers [1, 2] — the start
//     of the prior range — where a caller stepping backwards expects [5, 6].
//     A backward page needs the reversed order under the LIMIT and the
//     requested order restored outside it. Until then a before cursor is a
//     successfully-answered wrong page, and is refused for the same reason
//     an unset mode is.
//
//  5. Each column's Direction must be asc, desc, or empty. The renderer treats
//     everything that is not desc as ascending, so an unrecognised direction
//     paginates against its own ORDER BY. Empty is admitted because asc is the
//     documented default of this repository's sort surface
//     (internal.normalizeSortOrder), and the renderer already agrees with it.
//
// Rules 4 and 5 compare BYTE-EXACTLY, unlike rule 2's case-insensitive
// row_id. That is not an inconsistency: rule 2 governs a SQL identifier, which
// DuckDB resolves without regard to case, so "ROW_ID" IS row_id. A mode and a
// direction are Go constants that never reach SQL as text, and the renderer
// compares them exactly — admitting "DESC" here would hand the renderer a
// spelling it reads as ascending, reinstating the silent default this rule
// exists to remove. A future decode boundary that accepts caller spellings
// normalizes them before building the cursor, as normalizeSortOrder does.
//
// An inactive cursor is a no-op: the open first page carries no continuation
// obligation, and nothing renders from it — no values, no mode, no directions.
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
	if err := c.validateBoundaryValues(); err != nil {
		return err
	}
	if err := c.validateDirections(); err != nil {
		return err
	}
	if err := c.validateMode(); err != nil {
		return err
	}
	last := c.Columns[len(c.Columns)-1].Attribute
	if !strings.EqualFold(last, "row_id") {
		return fmt.Errorf("keyset cursor final column is %q, expected \"row_id\": a cursor not ending on the unique row_id tiebreak silently skips every row tied on the composite key at the page boundary", last)
	}
	return nil
}

// validateBoundaryValues refuses a nil boundary (rule 3). Positions are
// reported 1-based alongside the column name, because a caller assembling a
// cursor from a row reads it as "the value for created_at", not as Values[0].
func (c *KeysetCursor) validateBoundaryValues() error {
	for i, v := range c.Values {
		if !isNilBoundary(v) {
			continue
		}
		return fmt.Errorf("keyset cursor value %d (for column %q) is nil: a nil boundary binds SQL NULL, and a comparison against NULL is unknown rather than false, so every row tied at the page boundary is silently dropped instead of the cursor being refused",
			i+1, c.Columns[i].Attribute)
	}
	return nil
}

// isNilBoundary reports whether a boundary value binds SQL NULL. A typed nil
// — (*string)(nil) in an interface — is not == nil but binds NULL exactly as
// an untyped one does, and a cursor decoded into typed fields is precisely
// where one appears, so the reflective check is the one that guards the
// failure rather than the syntax.
func isNilBoundary(v interface{}) bool {
	if v == nil {
		return true
	}
	switch rv := reflect.ValueOf(v); rv.Kind() {
	case reflect.Ptr, reflect.Interface, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		return rv.IsNil()
	default:
		return false
	}
}

// validateDirections refuses a direction the renderer would read as ascending
// without being told to (rule 5).
func (c *KeysetCursor) validateDirections() error {
	for _, col := range c.Columns {
		switch col.Direction {
		case "", forma.SortOrderAsc, forma.SortOrderDesc:
			continue
		default:
			return fmt.Errorf("keyset cursor column %q has direction %q, expected \"asc\", \"desc\" or empty (asc): the renderer reads every other value as ascending, so the cursor would page against its own ORDER BY and answer successfully",
				col.Attribute, col.Direction)
		}
	}
	return nil
}

// validateMode refuses every mode but after (rule 4): an unknown one, the
// zero value the renderer reads as before, and before itself, whose backward
// window the renderer does not build yet (#513).
func (c *KeysetCursor) validateMode() error {
	switch c.Mode {
	case KeysetCursorModeAfter:
		return nil
	case KeysetCursorModeBefore:
		return fmt.Errorf("keyset cursor mode is \"before\", which is not supported yet (#513): the renderer flips the comparison but fetches in the forward order under the LIMIT, so a before page would answer the start of the prior range rather than the page preceding the cursor")
	default:
		return fmt.Errorf("keyset cursor mode is %q, expected \"after\": the renderer reads every other value — an unset mode above all — as \"before\", so a cursor that lost its mode paginates backwards and still answers successfully",
			c.Mode)
	}
}
