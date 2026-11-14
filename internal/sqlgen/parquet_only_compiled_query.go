package sqlgen

import (
	"fmt"
	"strings"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// ParquetWindow is the run of the parquet leg's ordered matches one pgx pass
// reads (design.md §6.1, *Pagination and totals*). The parquet-only
// template binds it, so the statement text, and with it the compiled plan,
// does not depend on the hot match count.
type ParquetWindow struct {
	// Offset is the first parquet rank the window holds. Always 0 for a
	// keyset page: the cursor, not a rank, starts the page.
	Offset int64
	// Limit is the number of parquet rows the window holds.
	Limit int64
}

// OffsetPageParquetWindow returns the parquet window an offset page
// [offset, offset+limit) can draw from, given hotMatches hot keys. A parquet
// row of rank p lands at merged position p plus the number of hot keys
// before it, between 0 and hotMatches, so only the ranks
// [max(0, offset-hotMatches), offset+limit) can reach the page: at most
// hotMatches+limit rows.
func OffsetPageParquetWindow(offset, limit, hotMatches int) (ParquetWindow, error) {
	if offset < 0 || limit < 0 || hotMatches < 0 {
		return ParquetWindow{}, fmt.Errorf(
			"parquet window for offset %d, limit %d and %d hot matches: each must be non-negative",
			offset, limit, hotMatches)
	}
	start := max(0, offset-hotMatches)
	return ParquetWindow{Offset: int64(start), Limit: int64(offset + limit - start)}, nil
}

// KeysetPageParquetWindow returns the parquet window of a keyset page: the
// first limit parquet rows after the cursor. The merge takes limit rows of
// the two legs together, so the parquet leg never contributes more.
func KeysetPageParquetWindow(limit int) (ParquetWindow, error) {
	if limit < 0 {
		return ParquetWindow{}, fmt.Errorf("parquet window for keyset limit %d: the limit must be non-negative", limit)
	}
	return ParquetWindow{Limit: int64(limit)}, nil
}

// ParquetOnlyCompiledQuery is a rendered ParquetOnlyQueryTemplateDuckDB
// skeleton for one query shape and render scope. Nothing per request renders
// into it: the dirty set is a staged table, and the condition operands, the
// keyset boundary and the window are binds. One skeleton therefore serves
// every request of its shape, whatever the dirty set's size or the hot match
// count.
type ParquetOnlyCompiledQuery struct {
	Skeleton string
	// HasKeyset records the page form the skeleton renders: LIMIT ? for a
	// keyset page, LIMIT ? OFFSET ? for an offset page. Bind refuses a
	// request of the other form, whose binds would not line up.
	HasKeyset bool
}

// parquetOnlyProjectionKeys are the projection params the parquet-only
// template reads.
var parquetOnlyProjectionKeys = []string{"OuterSelect", "S3SourceSelect"}

// CompileParquetOnlyQuery renders the parquet-only skeleton for q's shape.
// params carries the schema projection and the parquet scan inputs
// (DuckDBS3Paths or S3_PATHS, the cold column sets, EmptyParquetScan); no
// PostgreSQL param is read. dual supplies the
// DuckDB logical clause; its PostgreSQL clauses are not rendered, because Go
// serves the hot rows.
func CompileParquetOnlyQuery(params map[string]any, q *model.FederatedAttributeQuery, dual *DualClauses) (*ParquetOnlyCompiledQuery, error) {
	if q == nil {
		return nil, fmt.Errorf("compile parquet-only DuckDB query: the query is nil")
	}
	m := make(map[string]any, len(params)+8)
	for k, v := range params {
		m[k] = v
	}
	logical := "1=1"
	if dual != nil {
		logical = defaultIfEmpty(dual.DuckClause, logical)
	}
	m["LOGICAL_WHERE_CLAUSE"] = logical

	if err := injectDuckDBTemplateParams(m, q, dual); err != nil {
		return nil, fmt.Errorf("compile parquet-only DuckDB query: %w", err)
	}
	if err := requireParquetOnlyProjection(m); err != nil {
		return nil, fmt.Errorf("compile parquet-only DuckDB query: %w", err)
	}
	if err := requireS3Paths(m); err != nil {
		return nil, fmt.Errorf("compile parquet-only DuckDB query: %w", err)
	}
	// Bind re-derives the keyset args from the request's cursor.
	delete(m, "KEYSET_ARGS")
	hasKeyset := q.KeysetCursor.IsActive()
	m["ORDER_BY"] = parquetOnlyOrderBy(q)
	m["SORT_KEY_SELECT"] = parquetOnlySortKeySelect(q)

	sql, tplArgs, err := RenderSQLTemplate(ParquetOnlyQueryTemplateDuckDB, m)
	if err != nil {
		return nil, fmt.Errorf("compile parquet-only DuckDB query: %w", err)
	}
	if len(tplArgs) > 0 {
		// The window binds last, so a template-collected arg would shift it.
		return nil, fmt.Errorf("compile parquet-only DuckDB query: the template collected %d args, want none", len(tplArgs))
	}
	return &ParquetOnlyCompiledQuery{Skeleton: sql, HasKeyset: hasKeyset}, nil
}

// Bind returns the executable SQL and its args for a request of the
// skeleton's shape. The args follow the placeholders' order: the DuckDB
// condition args for the pushdown semijoin, again for the filter on the dedup
// winner, the keyset boundary, then the window.
func (c *ParquetOnlyCompiledQuery) Bind(q *model.FederatedAttributeQuery, dual DualClauses, window ParquetWindow) (string, []any, error) {
	keyset := q != nil && q.KeysetCursor.IsActive()
	if keyset != c.HasKeyset {
		return "", nil, fmt.Errorf("bind parquet-only DuckDB query: the request is a keyset page: %t, the skeleton: %t", keyset, c.HasKeyset)
	}
	if window.Offset < 0 || window.Limit < 0 {
		return "", nil, fmt.Errorf("bind parquet-only DuckDB query: window offset %d and limit %d must be non-negative", window.Offset, window.Limit)
	}
	args := make([]any, 0, 2*len(dual.DuckArgs)+4)
	args = append(args, dual.DuckArgs...)
	args = append(args, dual.DuckArgs...)
	if !keyset {
		return c.Skeleton, append(args, window.Limit, window.Offset), nil
	}
	if window.Offset != 0 {
		return "", nil, fmt.Errorf("bind parquet-only DuckDB query: a keyset page's window starts at its cursor, not at offset %d", window.Offset)
	}
	_, keysetArgs, err := generateKeysetWhereClause(q.KeysetCursor)
	if err != nil {
		return "", nil, fmt.Errorf("bind parquet-only DuckDB query keyset args: %w", err)
	}
	args = append(args, keysetArgs...)
	return c.Skeleton, append(args, window.Limit), nil
}

// BuildParquetOnlyQuery compiles and binds in one call, for a caller without
// a plan cache.
func BuildParquetOnlyQuery(params map[string]any, q *model.FederatedAttributeQuery, dual DualClauses, window ParquetWindow) (string, []any, error) {
	compiled, err := CompileParquetOnlyQuery(params, q, &dual)
	if err != nil {
		return "", nil, err
	}
	return compiled.Bind(q, dual, window)
}

// parquetOnlyOrderBy renders the page's order, the one the retired attach
// template rendered (buildKeysetOrderBy or buildNonKeysetOrderBy), with NULLS LAST
// spelled out on every term: the Go comparator puts NULL last in both
// directions, and DuckDB's default_null_order setting could otherwise move
// the parquet leg's NULLs without changing the statement.
func parquetOnlyOrderBy(q *model.FederatedAttributeQuery) string {
	terms := parquetOnlyOrderTerms(q)
	out := make([]string, len(terms))
	for i, term := range terms {
		out[i] = term.expr + " " + term.dir + " NULLS LAST"
	}
	return strings.Join(out, ", ")
}

// SortKeyColumn names the column that carries the i-th key of the page's
// order in the parquet-only result, after current_page. The keys come in the
// order of the ORDER BY terms, so the i-th is the i-th column of the Go order
// (sortkey.PageOrder) built from the same request. Go reads the column by
// position, and the statement never references it by name (see
// parquetOnlyOrderTerms), so an attribute of the same name cannot take its
// place.
func SortKeyColumn(i int) string { return fmt.Sprintf("forma_sort_key_%d", i) }

// parquetOnlySortKeySelect projects each ORDER BY term's expression, without
// its direction, as SortKeyColumn(i). The merge compares these values with
// the hot keys, so they are the exact expressions the statement orders by:
// COALESCE(visible.deleted_ts, 0) included.
func parquetOnlySortKeySelect(q *model.FederatedAttributeQuery) string {
	terms := parquetOnlyOrderTerms(q)
	cols := make([]string, len(terms))
	for i, term := range terms {
		cols[i] = fmt.Sprintf("%s AS %s", term.expr, SortKeyColumn(i))
	}
	return strings.Join(cols, ",\n  ")
}

// parquetOnlyVisible is the relation the parquet-only statement's final
// SELECT reads, which qualifies every column its order reads.
const parquetOnlyVisible = "visible"

// parquetOrderTerm is one ORDER BY term of the parquet-only statement: the
// expression the rows order by, which is also the sort key the row carries,
// and its direction.
type parquetOrderTerm struct {
	expr string
	dir  string
}

// parquetOnlyOrderTerms is the page's order: the cursor's columns under an
// active cursor (keysetOrderTerms' order), the request's sort keys otherwise
// (nonKeysetOrderTerms' order).
//
// Two things differ from the order the other templates render, both for the
// merge. Every term reads a column of visible, qualified as visible.<column>,
// never an unqualified name: in ORDER BY, and in a select list after it is
// defined, DuckDB binds an unqualified name to an output alias before an
// input column, so an attribute named like an alias of the result (a
// SortKeyColumn, ltbase_row_id, total_records) would order by that alias
// instead. And an attribute bound to an entity_main column reads its visible
// column, the parquet column at the attribute's export type
// (sqlgen.DuckDBNullScanType), not the outer select's storage alias of the
// bound column: the alias holds the column's storage image (SMALLINT 1/0 for
// a bool_smallint bool, VARCHAR '1'/'0' for bool_text, RFC 3339 text for an
// rfc3339 date), while the key the hot leg derives, and the Go comparator
// reads, has the export type (sortkey.AttributeType).
func parquetOnlyOrderTerms(q *model.FederatedAttributeQuery) []parquetOrderTerm {
	if q.KeysetCursor.IsActive() {
		terms := make([]parquetOrderTerm, 0, len(q.KeysetCursor.Columns))
		for _, col := range q.KeysetCursor.Columns {
			expr := visibleColumnRef(ParquetAttrColumn(col.Attribute))
			if DuckDBEqualFold(col.Attribute, keysetDeletedTS) {
				expr = fmt.Sprintf("COALESCE(%s, 0)", expr)
			}
			terms = append(terms, parquetOrderTerm{expr: expr, dir: orderDirection(col.Direction)})
		}
		return terms
	}
	var terms []parquetOrderTerm
	for _, ao := range q.AttributeOrders {
		var expr string
		switch {
		case ao.AttrName != "":
			expr = visibleColumnRef(ParquetAttrColumn(ao.AttrName))
		case ao.IsMainColumn():
			// No attribute name to read the visible column by: the bound
			// column's output alias, as nonKeysetOrderTerms renders it.
			expr = ao.ColumnName
		default:
			// Unresolvable; nonKeysetOrderTerms skips it too.
			continue
		}
		terms = append(terms, parquetOrderTerm{expr: expr, dir: orderDirection(ao.SortOrder)})
	}
	if len(terms) == 0 {
		terms = []parquetOrderTerm{{expr: visibleColumnRef("created_at"), dir: "DESC"}}
	}
	return append(terms, parquetOrderTerm{expr: visibleColumnRef("row_id"), dir: "ASC"})
}

// visibleColumnRef qualifies a column of the visible CTE.
func visibleColumnRef(column string) string {
	return parquetOnlyVisible + "." + column
}

// orderDirection renders a sort direction as the other order builders do:
// DESC for forma.SortOrderDesc, ASC otherwise.
func orderDirection(dir forma.SortOrder) string {
	if dir == forma.SortOrderDesc {
		return "DESC"
	}
	return "ASC"
}

// requireParquetOnlyProjection fails a render missing a projection param the
// parquet-only template reads, naming each absent key.
func requireParquetOnlyProjection(params map[string]any) error {
	var missing []string
	for _, key := range parquetOnlyProjectionKeys {
		if _, ok := params[key]; !ok {
			missing = append(missing, key)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf("parquet-only DuckDB template render is missing schema projection params [%s]; derive them via BuildSchemaProjection or BuildBenchmarkProjections", strings.Join(missing, ", "))
}
