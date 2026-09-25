package federated

import (
	"context"
	"fmt"

	"github.com/lychee-technology/forma/internal/model"
)

// ExecuteFederatedKeysetQuery returns one keyset page across all three tiers
// plus the FULL match count, not the count remaining after the cursor. The
// page comes from DuckDB's federated template (hot via postgres_scan, warm and
// cold from S3), which applies the keyset WHERE in the visible CTE after the
// LWW dedup (rn = 1), so the cursor filters LWW winners rather than row
// versions (#212). A nil or empty cursor is the open first page
// (model.KeysetCursor.IsActive).
//
// The total is what separates this from Query, whose keyset pages report the
// template's COUNT(*) OVER() — rows remaining after the cursor. The keyset
// benchmark asserts the full count, so it calls this coordinator.
//
// This was ExecuteFederatedPaginatedQuery until #442. Its only caller always
// passed an active cursor, so the offset path behind the keyset early return —
// an in-memory Postgres/DuckDB merge — never ran outside unit tests. It was
// retired rather than wired to a caller: it read the hot tier twice,
// arbitrated conflicts by UpdatedAt, and capped its total at maxRows. Offset
// pagination is Query's job.
func (e *DBFederatedQueryEngine) ExecuteFederatedKeysetQuery(
	ctx context.Context,
	tables model.StorageTables,
	fq *model.FederatedAttributeQuery,
	limit int,
	attributeOrders []model.AttributeOrder,
	opts *model.FederatedQueryOptions,
) ([]*model.PersistentRecord, int64, error) {
	if err := validateFederatedQueryTarget(fq); err != nil {
		return nil, 0, fmt.Errorf("validate federated query: %w", err)
	}
	if limit <= 0 {
		limit = model.DefaultPageSize
	}
	// The DuckDB template below consumes the cursor unvalidated, so refuse a
	// malformed one here. Same call as the engine gate (engine.go): one
	// contract, both seams (#381).
	if err := validateKeysetCursor(fq.KeysetCursor, attributeOrders); err != nil {
		return nil, 0, fmt.Errorf("validate keyset cursor: %w", err)
	}

	maxRows := model.FederatedMaxRows
	if opts != nil && opts.MaxRows > 0 {
		maxRows = opts.MaxRows
	}
	// Clamp limit to maxRows to prevent unbounded fetch
	if maxRows > 0 && limit > maxRows {
		limit = maxRows
	}

	// Fetch from DuckDB (cold and warm via S3, hot via postgres_scan).
	// The template applies the keyset WHERE in the visible CTE AFTER the
	// ROW_NUMBER dedup picks rn = 1, so the cursor filters LWW winners, not
	// row versions. Applying it pre-dedup resurrected superseded versions for
	// cursors over any version-varying column (#212).
	//
	// created_at is no longer one of those: since #460 the S3 projection reads
	// the exported ltbase_created_at instead of aliasing changed_at, so an
	// ordinary row's creation stamp is the same on every version. Post-dedup
	// placement remains REQUIRED regardless — business attributes still vary
	// per version, and a delete-and-recreate history reuses a row_id with a
	// genuinely different creation time, so a pre-dedup cursor could still
	// admit a superseded version.
	if opts != nil && opts.IncludeExecutionPlan && opts.ExecutionPlan != nil {
		opts.ExecutionPlan.Routing = model.RoutingDecision{
			Tiers:     []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold},
			UseDuckDB: true,
			Reason:    "keyset pagination",
		}
		opts.ExecutionPlan.Notes = append(opts.ExecutionPlan.Notes, "keyset pagination")
	}

	duckRecs, total, err := e.ExecuteDuckDBFederatedQuery(ctx, tables, fq, limit, 0, attributeOrders, opts)
	if err != nil {
		return nil, 0, fmt.Errorf("fetch duckdb records: %w", err)
	}

	// With keyset, the DuckDB template dedups via rn = 1 and applies the
	// cursor post-dedup in the visible CTE (#212), so we apply limit
	// directly to the returned records.
	var page []*model.PersistentRecord
	if limit > 0 && limit < len(duckRecs) {
		page = duckRecs[:limit]
	} else {
		page = duckRecs
	}

	// Compute total count without the keyset constraint.
	// We strip the cursor and re-query with a minimal limit to get the count.
	countTotal, err := e.computeFederatedCount(ctx, tables, fq)
	if err != nil {
		return nil, 0, fmt.Errorf("compute federated count: %w", err)
	}

	return page, max(total, countTotal), nil
}

// computeFederatedCount returns the total number of unique rows matching the
// filter conditions across all tiers. It strips the keyset cursor to get the
// full unfiltered count via a lightweight query.
func (e *DBFederatedQueryEngine) computeFederatedCount(
	ctx context.Context,
	tables model.StorageTables,
	fq *model.FederatedAttributeQuery,
) (int64, error) {
	strippedQuery := *fq
	strippedQuery.KeysetCursor = nil
	// The pagination on the copy is now redundant with dispatchedQuery, which
	// renders every DuckDB query with the arguments it was dispatched with;
	// it stays so this copy reads as the query it is — a limit-1 recount with
	// no offset. Before that helper, the copy was the only thing keeping a
	// deep offset out of the recount, which otherwise streamed zero rows
	// again (#181).
	strippedQuery.Limit = 1
	strippedQuery.Offset = 0

	_, total, err := e.ExecuteDuckDBFederatedQuery(ctx, tables, &strippedQuery, 1, 0, nil, &model.FederatedQueryOptions{MaxRows: 1})
	if err != nil {
		return 0, fmt.Errorf("federated count query (schema %d): %w", fq.SchemaID, err)
	}
	return total, nil
}
