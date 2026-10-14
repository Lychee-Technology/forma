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
	head := fmt.Sprintf("SELECT 1 FROM %s x WHERE x.schema_id = %s AND x.row_id = %s AND x.attr_id = %s",
		eavTable, schemaExpr, rowExpr, next())
	switch {
	case p.NullTest != "":
		if !p.List {
			head += " AND x." + p.ValueColumn + " IS NOT NULL"
		}
		if p.NullTest == conditionexpr.SQLOpIsNull {
			return "NOT EXISTS (" + head + ")"
		}
		return "EXISTS (" + head + ")"
	case p.Set != nil:
		return "EXISTS (" + head + " AND " + p.Set.RenderPg(p.ComparisonLHS("x"), next()) + ")"
	case p.Now != nil:
		return "EXISTS (" + head + " AND " + p.Now.RenderComparison(p.ComparisonLHS("x"), p.SQLOp, next()) + ")"
	}
	return fmt.Sprintf("EXISTS (%s AND %s %s %s)", head, p.ComparisonLHS("x"), p.SQLOp, next())
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
		return p.MainSet.RenderPg(p.MainSet.mainLHS(column), next()), []any{p.MainSet.Values}
	case p.MainNow != nil:
		return p.MainNow.RenderComparison(column, p.MainSQLOp, next()), p.MainNow.Args()
	}
	rhs := next()
	if p.MainCast != "" {
		rhs += "::" + p.MainCast
	}
	return fmt.Sprintf("%s %s %s", column, p.MainSQLOp, rhs), []any{p.MainValue}
}
