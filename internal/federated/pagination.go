package federated

import (
	"github.com/lychee-technology/forma/internal/model"
	"context"
	"fmt"
	"time"
)

// ExecuteFederatedPaginatedQuery performs a federated fetch across Postgres (hot) and DuckDB (cold/warm),
// merges results with last-write-wins semantics, and returns the requested page plus an accurate total
// deduplicated across sources.
//
// Notes:
// - This is an MVP coordinator: it caps per-source fetches (opts.MaxRows or default) to avoid OOM.
// - For very large result sets a keys-only two-phase approach should be implemented later.
func (e *DBFederatedQueryEngine) ExecuteFederatedPaginatedQuery(
	ctx context.Context,
	tables StorageTables,
	fq *FederatedAttributeQuery,
	limit, offset int,
	attributeOrders []AttributeOrder,
	opts *FederatedQueryOptions,
) ([]*PersistentRecord, int64, error) {
	if fq == nil {
		return nil, 0, fmt.Errorf("federated query cannot be nil")
	}
	if limit <= 0 {
		limit = model.DefaultPageSize
	}
	if offset < 0 {
		offset = 0
	}

	if opts != nil && opts.KeysetEnabled && fq.KeysetCursor != nil && len(fq.KeysetCursor.Columns) > 0 {
		return e.executeFederatedKeysetQuery(ctx, tables, fq, limit, attributeOrders, opts)
	}

	// If explicit attribute ordering is requested, prefer DuckDB federated execution
	// which can handle ordering correctly in SQL. Fall back to in-memory merge only
	// if DuckDB is unavailable and AllowPartialDegradedMode is true.
	if len(attributeOrders) > 0 {
		if e.duck != nil && e.cfg.Enabled {
			// Use DuckDB federated query which handles ordering correctly
			return e.ExecuteDuckDBFederatedQuery(ctx, tables, fq, limit, offset, attributeOrders, opts)
		}
		// DuckDB unavailable - only allow if degraded mode permitted
		if opts == nil || !opts.AllowPartialDegradedMode {
			return nil, 0, fmt.Errorf("ordered federated pagination requires DuckDB but DuckDB is unavailable")
		}
		// Otherwise fall through to in-memory merge which will ignore ordering
	}

	// Build shared hybrid WHERE clause
	clause, args, err := e.pgSource.BuildHybridConditions(tables, fq)
	if err != nil {
		return nil, 0, fmt.Errorf("build hybrid conditions: %w", err)
	}

	// Determine per-source fetch cap
	maxRows := model.FederatedMaxRows
	if opts != nil && opts.MaxRows > 0 {
		maxRows = opts.MaxRows
	}

	// Fetch from Postgres (hot)
	startPg := time.Now()
	pgRecs, _, err := e.pgSource.RunOptimizedQuery(ctx, tables, fq.SchemaID, clause, args, maxRows, 0, attributeOrders, fq.UseMainAsAnchor)
	pgDuration := time.Since(startPg).Milliseconds()
	if err != nil {
		return nil, 0, fmt.Errorf("fetch postgres records: %w", err)
	}
	// Record Postgres source info if execution plan requested
	if opts != nil && opts.IncludeExecutionPlan && opts.ExecutionPlan != nil {
		dp := DataSourcePlan{
			Tier:              DataTierHot,
			Engine:            "postgres",
			SQL:               "", // SQL not captured here
			RowEstimate:       0,
			PredicatePushdown: fq.UseMainAsAnchor,
			ActualRows:        int64(len(pgRecs)),
			DurationMs:        pgDuration,
			Reason:            "postgres optimized query",
		}
		opts.ExecutionPlan.Sources = append(opts.ExecutionPlan.Sources, dp)
		opts.ExecutionPlan.Timings["postgres_fetch"] = pgDuration
	}

	// Fetch from DuckDB (warm/cold)
	duckRecs, _, err := e.ExecuteDuckDBFederatedQuery(ctx, tables, fq, maxRows, 0, attributeOrders, opts)
	if err != nil {
		return nil, 0, fmt.Errorf("fetch duckdb records: %w", err)
	}

	// Merge across tiers using existing merge logic
	inputs := map[DataTier][]*PersistentRecord{
		DataTierHot:  pgRecs,
		DataTierWarm: nil,
		DataTierCold: duckRecs,
	}

	startMerge := time.Now()
	merged, err := MergePersistentRecordsByTier(inputs, fq.PreferHot)
	mergeMs := time.Since(startMerge).Milliseconds()
	if err != nil {
		return nil, 0, fmt.Errorf("merge records by tier: %w", err)
	}
	// Record merge plan if requested
	if opts != nil && opts.IncludeExecutionPlan && opts.ExecutionPlan != nil {
		opts.ExecutionPlan.Merge = MergePlan{
			Strategy:   MergeStrategyLastWriteWins,
			PreferHot:  fq.PreferHot,
			DedupKeys:  []string{"SchemaID:RowID"},
			DurationMs: mergeMs,
			Notes:      []string{"attribute-level deduplication applied"},
		}
		opts.ExecutionPlan.Timings["merge"] = mergeMs
	}

	total := int64(len(merged))

	// Apply pagination on merged, which is deterministically ordered by MergePersistentRecordsByTier
	start := offset
	if start >= len(merged) {
		return []*PersistentRecord{}, total, nil
	}
	end := min(start+limit, len(merged))
	page := merged[start:end]

	return page, total, nil
}

// executeFederatedKeysetQuery performs a keyset-cursor-based federated query.
// It delegates to DuckDB's federated template which applies the keyset WHERE
// clause to the unified source (both S3 parquet and Postgres via postgres_scan).
// Results are merged with LWW semantics via the template's QUALIFY clause and
// the requested page is returned along with the total count and next cursor.
func (e *DBFederatedQueryEngine) executeFederatedKeysetQuery(
	ctx context.Context,
	tables StorageTables,
	fq *FederatedAttributeQuery,
	limit int,
	attributeOrders []AttributeOrder,
	opts *FederatedQueryOptions,
) ([]*PersistentRecord, int64, error) {
	// Validate keyset cursor columns are supported
	if fq.KeysetCursor != nil {
		if err := validateKeysetColumns(fq.KeysetCursor.Columns); err != nil {
			return nil, 0, err
		}
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
	// The template applies the keyset WHERE in the unified CTE, so both
	// Postgres and S3 data are filtered by the cursor before LWW dedup.
	if opts != nil && opts.IncludeExecutionPlan && opts.ExecutionPlan != nil {
		opts.ExecutionPlan.Routing = RoutingDecision{
			Tiers:     []DataTier{DataTierHot, DataTierWarm, DataTierCold},
			UseDuckDB: true,
			Reason:    "keyset pagination",
		}
		opts.ExecutionPlan.Notes = append(opts.ExecutionPlan.Notes, "keyset pagination")
	}

	duckRecs, total, err := e.ExecuteDuckDBFederatedQuery(ctx, tables, fq, limit, 0, attributeOrders, opts)
	if err != nil {
		return nil, 0, fmt.Errorf("fetch duckdb records: %w", err)
	}

	// With keyset, the DuckDB template handles LWW dedup via QUALIFY,
	// so we apply limit directly to the returned records.
	var page []*PersistentRecord
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
	tables StorageTables,
	fq *FederatedAttributeQuery,
) (int64, error) {
	strippedQuery := *fq
	strippedQuery.KeysetCursor = nil

	_, total, err := e.ExecuteDuckDBFederatedQuery(ctx, tables, &strippedQuery, 1, 0, nil, &FederatedQueryOptions{MaxRows: 1})
	if err != nil {
		return 0, err
	}
	return total, nil
}
