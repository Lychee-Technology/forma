package sqlgen

import (
	"fmt"

	"github.com/lychee-technology/forma/internal/conditionexpr"
)

// Render spells the payload as an eav_data subquery correlated to the
// anchor's schema and row expressions, drawing each placeholder from next
// in Args order (the attribute id first). It is the one spelling the
// pg-eav emitter and the hybrid builder's EAV branch share.
//
// A null test is a presence test: a scalar is present when its row holds a
// value, a list whenever any of its rows exists, the empty-list marker
// included (#204), so is_null answers exactly the attributes the read path
// returns as absent (#646 D6).
func (p PgEavLeafPayload) Render(eavTable, schemaExpr, rowExpr string, next func() string) string {
	head := fmt.Sprintf("SELECT 1 FROM %s x WHERE x.schema_id = %s AND x.row_id = %s AND %s",
		eavTable, schemaExpr, rowExpr, p.RenderOnRow("x", next))
	if p.NullTest == conditionexpr.SQLOpIsNull {
		return "NOT EXISTS (" + head + ")"
	}
	return "EXISTS (" + head + ")"
}

// MatchesOnRow reports whether the payload holds exactly for the entities
// with an eav_data row that RenderOnRow selects: every leaf but is_null,
// whose NOT EXISTS holds where no such row exists.
func (p PgEavLeafPayload) MatchesOnRow() bool {
	return p.Err == nil && p.NullTest != conditionexpr.SQLOpIsNull
}

// RenderOnRow spells the payload's test of one eav_data row aliased alias,
// the body Render's EXISTS asks of some row of the entity, drawing each
// placeholder from next in Args order. For is_null it spells the presence
// test Render negates.
func (p PgEavLeafPayload) RenderOnRow(alias string, next func() string) string {
	match := alias + ".attr_id = " + next()
	switch {
	case p.NullTest != "":
		if !p.List {
			match += " AND " + alias + "." + p.ValueColumn + " IS NOT NULL"
		}
		return match
	case p.Set != nil:
		return match + " AND " + p.Set.RenderPg(p.ComparisonLHS(alias), next())
	case p.Now != nil:
		return match + " AND " + p.Now.RenderComparison(p.ComparisonLHS(alias), p.SQLOp, next())
	}
	return fmt.Sprintf("%s AND %s %s %s", match, p.ComparisonLHS(alias), p.SQLOp, next())
}

// Args returns the binds of Render in placeholder order.
func (p PgEavLeafPayload) Args() []any {
	switch {
	case p.NullTest != "":
		return []any{p.AttrID}
	case p.Set != nil:
		return []any{p.AttrID, p.Set.Values}
	case p.Now != nil:
		return []any{p.AttrID, p.Now.OffsetMs}
	}
	return []any{p.AttrID, p.Value}
}

// RenderMain spells the main-branch predicate over an already-qualified
// column, drawing placeholders from next, and returns its binds in order.
func (p HybridLeafPayload) RenderMain(column string, next func() string) (string, []any) {
	switch {
	case p.MainBool != nil:
		return p.MainBool.Render(column, next), p.MainBool.Args()
	case p.MainNullTest != "":
		return column + " " + p.MainNullTest, nil
	case p.MainSet != nil:
		return p.MainSet.renderMain(column, next)
	case p.MainNow != nil:
		return p.MainNow.RenderComparison(column, p.MainSQLOp, next()), p.MainNow.Args()
	}
	rhs := next()
	if p.MainCast != "" {
		rhs += "::" + p.MainCast
	}
	if p.MainByteOrder {
		column = inByteOrder(column)
	}
	return fmt.Sprintf("%s %s %s", column, p.MainSQLOp, rhs), []any{p.MainValue}
}
