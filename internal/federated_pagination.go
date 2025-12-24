package internal

import (
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
func (r *PostgresPersistentRecordRepository) ExecuteFederatedPaginatedQuery(
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
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}

	// Build shared hybrid WHERE clause
	clause, args, err := r.buildHybridConditions(tables.EAVData, tables.EntityMain, fq.AttributeQuery, 0, fq.UseMainAsAnchor)
	if err != nil {
		return nil, 0, fmt.Errorf("build hybrid conditions: %w", err)
	}

	// Determine per-source fetch cap
	maxRows := 10000
	if opts != nil && opts.MaxRows > 0 {
		maxRows = opts.MaxRows
	}

	// Fetch from Postgres (hot)
	startPg := time.Now()
	pgRecs, _, err := r.runOptimizedQuery(ctx, tables, fq.SchemaID, clause, args, maxRows, 0, attributeOrders, fq.UseMainAsAnchor)
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
	duckRecs, _, err := r.ExecuteDuckDBFederatedQuery(ctx, tables, fq, maxRows, 0, attributeOrders, opts)
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
			Strategy:   "last-write-wins",
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
	end := start + limit
	if end > len(merged) {
		end = len(merged)
	}
	page := merged[start:end]

	return page, total, nil
}
