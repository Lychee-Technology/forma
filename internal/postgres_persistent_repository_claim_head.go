package internal

import (
	"fmt"
	"strings"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/lychee-technology/forma"
)

// The group-head predicate of a claim (#653). A row m is its group's head
// when it matches Live and no row e of its group that matches Live precedes
// it by the head order, then row ID. Both the candidate select and the
// re-check anchor on entity_main as m, so they apply one predicate.
//
// A key column compares as e.c = m.c OR both null rather than IS NOT
// DISTINCT FROM: the two agree, but PostgreSQL cannot use an index for IS
// NOT DISTINCT FROM, and the NOT EXISTS then reads the schema's whole range
// for every candidate. With the equality, a declared index on the key
// columns serves it. Order columns sort with nulls last in either
// direction, as a query's Sort does.

// renderHybrid renders cond with Query's renderer anchored on entity_main
// as alias, numbering its placeholders after argCounter, and answers the
// clause, its args and the last placeholder number used.
func (r *DBPersistentRecordRepository) renderHybrid(tables model.StorageTables, schemaID int16, cond forma.Condition, argCounter int, alias string) (string, []any, int, error) {
	builder := &hybridConditionBuilder{
		r:                    r,
		eavTable:             sanitizeIdentifier(tables.EAVData),
		mainTable:            sanitizeIdentifier(tables.EntityMain),
		schemaID:             schemaID,
		argCounter:           argCounter,
		useMainTableAsAnchor: true,
		mainAlias:            alias,
	}
	builder.initCache()
	clause, args, err := builder.build(cond)
	if err != nil {
		return "", nil, 0, err
	}
	return clause, args, builder.argCounter, nil
}

// headPredicate renders head on alias m, its placeholders numbered after
// argCounter, and answers the clause, its args and the last placeholder
// number used. Live is rendered once for m and once for e with the same
// placeholders, so its values are bound once.
func (r *DBPersistentRecordRepository) headPredicate(tables model.StorageTables, schemaID int16, head *model.GroupHead, argCounter int) (string, []any, int, error) {
	live, args, next, err := r.renderHybrid(tables, schemaID, head.Live, argCounter, "m")
	if err != nil {
		return "", nil, 0, fmt.Errorf("render the group live condition: %w", err)
	}
	earlierLive, _, _, err := r.renderHybrid(tables, schemaID, head.Live, argCounter, "e")
	if err != nil {
		return "", nil, 0, fmt.Errorf("render the group live condition on earlier rows: %w", err)
	}
	parts := make([]string, 0, len(head.KeyColumns)+3)
	parts = append(parts, "e.ltbase_schema_id = m.ltbase_schema_id")
	for _, column := range head.KeyColumns {
		parts = append(parts, sameValue(sanitizeIdentifier(column)))
	}
	parts = append(parts, headPrecedes(head.Orders), "("+earlierLive+")")
	clause := fmt.Sprintf("(%s) AND NOT EXISTS (SELECT 1 FROM %s e WHERE %s)",
		live, sanitizeIdentifier(tables.EntityMain), strings.Join(parts, " AND "))
	return clause, args, next, nil
}

// sameValue says e and m hold equal values in column, a null equal to a
// null.
func sameValue(column string) string {
	return fmt.Sprintf("(e.%[1]s = m.%[1]s OR (e.%[1]s IS NULL AND m.%[1]s IS NULL))", column)
}

// headPrecedes says e comes before m by orders, then row ID ascending.
func headPrecedes(orders []model.AttributeOrder) string {
	clause := "e.ltbase_row_id < m.ltbase_row_id"
	for i := len(orders) - 1; i >= 0; i-- {
		column := sanitizeIdentifier(orders[i].ColumnName)
		before := "<"
		if orders[i].SortOrder == forma.SortOrderDesc {
			before = ">"
		}
		clause = fmt.Sprintf("(e.%[1]s %[2]s m.%[1]s OR (e.%[1]s IS NOT NULL AND m.%[1]s IS NULL) OR (%[3]s AND %[4]s))",
			column, before, sameValue(column), clause)
	}
	return clause
}

// RenderClaimEligibility renders a group claim's re-check: condition and
// the head predicate, anchored on m, placeholders numbered from $3 after a
// statement's schema ID and row IDs. It sends no statement.
func (b *txBoundRepository) RenderClaimEligibility(tables model.StorageTables, schemaID int16, condition forma.Condition, head *model.GroupHead) (*renderedGuard, error) {
	clause, args, next, err := b.repo.renderHybrid(tables, schemaID, condition, 2, "m")
	if err != nil {
		return nil, fmt.Errorf("render claim condition: %w", err)
	}
	headClause, headArgs, _, err := b.repo.headPredicate(tables, schemaID, head, next)
	if err != nil {
		return nil, fmt.Errorf("render claim group head: %w", err)
	}
	return &renderedGuard{clause: "(" + clause + ") AND " + headClause, args: append(args, headArgs...)}, nil
}
