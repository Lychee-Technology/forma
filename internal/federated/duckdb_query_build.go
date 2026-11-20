package federated

import (
	"context"
	"fmt"
	"strconv"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// SQL construction shared by the parquet leg: the dual clauses and schema
// projections, the dispatched query, and the parquet scan params and scope.

// translateDuckDBQuery renders q's condition into the dual clauses and writes
// the schema projections into sqlParams. Its refusals depend only on the query
// and the schema's metadata: a filter literal the attribute cannot take is
// invalid input, and a schema with no usable metadata fails with
// ErrSchemaMetadataCacheRequired. Both are non-degradable. A read refused
// before it builds (a cached corrupt verdict, #722) runs this too, so it
// raises them as the build would rather than degrading past them.
func (e *DBFederatedQueryEngine) translateDuckDBQuery(
	ctx context.Context, eavTable string, q *model.FederatedAttributeQuery,
	cache forma.SchemaAttributeCache, sqlParams map[string]any,
) (sqlgen.DualClauses, error) {
	paramIndex := 0
	dc, err := sqlgen.ToDualClauses(q.Condition, sqlutil.SanitizeIdentifier(eavTable), q.SchemaID, cache, &paramIndex)
	if err != nil {
		return sqlgen.DualClauses{}, fmt.Errorf("to dual clauses: %w", err)
	}
	if _, err := e.injectSchemaProjections(ctx, sqlParams, q.SchemaID, cache); err != nil {
		return sqlgen.DualClauses{}, err
	}
	return dc, nil
}

// dispatchedQuery returns a copy of q whose Limit, Offset and AttributeOrders
// are the arguments the DuckDB seam was called with.
//
// The retired attach template rendered LIMIT, OFFSET and the non-keyset ORDER
// BY from the QUERY OBJECT (sqlgen.injectDuckDBTemplateParams read q.Limit,
// q.Offset, q.AttributeOrders), and the parquet-only statement still derives
// its order and window from it; the call arguments only reached the template
// map under "Limit"/"Offset"/"PageSize"/"SortKeys", which it never read. So a caller
// that normalised or clamped its limit but passed q unchanged had its clamp
// silently ignored: the keyset coordinator (ExecuteFederatedKeysetQuery, then
// the keyset branch of ExecuteFederatedPaginatedQuery) rendered LIMIT 0 for a
// zero q.Limit and an over-MaxRows q.Limit verbatim, with only its in-memory
// slice honouring the clamp (#381 review), and
// computeFederatedCount hand-rolled the very copy made here (#181).
//
// Made at this function, the single point every pass and render flows
// through, so the rendered skeleton, the shape hash and the plan scope all
// see the query that was dispatched. The explicit arguments
// are the pagination contract of every seam; engine.Query passes q's own
// fields and is unaffected. q itself is never mutated — the caller may still
// be reading it (recordTranslation, the keyset page slice).
func dispatchedQuery(q *model.FederatedAttributeQuery, limit, offset int, attributeOrders []model.AttributeOrder) *model.FederatedAttributeQuery {
	if q == nil {
		return nil
	}
	page := *q
	page.Limit = limit
	page.Offset = offset
	page.AttributeOrders = attributeOrders
	return &page
}

// applyParquetScanParams sets the params the renderer derives the parquet
// scan source from. Both templates read the parquet tiers through it.
func applyParquetScanParams(sqlParams map[string]any, parquetPaths []string, cold coldScanSet) {
	if len(parquetPaths) > 0 {
		sqlParams["DuckDBS3Paths"] = parquetPaths
	}
	if len(cold.missing) > 0 {
		// Cold-absent columns for the #255 scan-source augmentation; the
		// renderer folds them into S3_SCAN_SOURCE.
		sqlParams["ColdMissingColumns"] = cold.missing
	}
	if len(cold.pinned) > 0 {
		// Stale-typed columns for the #371 scan-source type pin; the
		// renderer adds them to the scan's REPLACE list.
		sqlParams["ColdPinnedColumns"] = cold.pinned
	}
}

// parquetScanScopeParts are the scope components of the parquet scan: the
// parquet scan source and the sort keys render into the skeleton.
func parquetScanScopeParts(parquetPaths []string, attributeOrders []model.AttributeOrder, cold coldScanSet) []string {
	var parts []string
	// The generation's objects render into the skeleton as SQL literals, so
	// they must scope the cache key: another generation's object set (a new
	// delta flushed, a base replaced) recompiles instead of reusing stale
	// paths.
	parts = append(parts, parquetPaths...)
	for _, o := range attributeOrders {
		parts = append(parts, strconv.Itoa(int(o.AttrID)), o.AttrName, o.ColumnName,
			string(o.StorageLocation), string(o.SortOrder))
	}
	parts = append(parts, "cold-missing")
	for _, mc := range cold.missing {
		parts = append(parts, mc.Name, mc.DuckDBType)
	}
	parts = append(parts, "cold-pinned")
	for _, pc := range cold.pinned {
		parts = append(parts, pc.Name, pc.DuckDBType)
	}
	return parts
}
