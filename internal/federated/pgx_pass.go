package federated

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sortkey"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// The pgx pass (design.md §6.1.1; pgx-hot-leg-plan.md step 5), run under
// DuckDBConfig.HotLegTransport = pgx:
//
//  1. resolve and freeze the parquet path set, and stamp the cutoff;
//  2. open a REPEATABLE READ READ ONLY snapshot and read the dirty set and,
//     in the hot form, the hot keys;
//  3. sort the hot keys in Go and apply the cursor;
//  4. stage the dirty set in DuckDB and run the parquet-only statement over
//     the window the page can draw from;
//  5. merge and cut the page;
//  6. read the page's hot rows in the same snapshot, and end it.
//
// The hot-excluded form reads the strict dirty set and no hot key, so its
// page is the parquet window. A schema without parquet (#720) runs no DuckDB
// statement: the hot leg is the whole answer.

// pgxPass carries one pass's state between its steps.
type pgxPass struct {
	e      *DBFederatedQueryEngine
	tables model.StorageTables
	// q is the dispatched query: its Limit, Offset and AttributeOrders are
	// the page's (dispatchedQuery), and its cursor is normalized.
	q       *model.FederatedAttributeQuery
	planCtx *duckDBExecutionPlanContext
	hasHot  bool
	order   sortkey.Order
	src     pgxScanSources
	// noObjects is settled in the snapshot: the schema has no parquet, and
	// the pass runs no DuckDB statement.
	noObjects bool
	// probe is the breaker's half-open reservation; resolved records that a
	// DuckDB statement reported to the breaker, or that the probe went back.
	probe    ProbeToken
	resolved bool
	// snapshotMs is the time the pass spent reading its snapshot.
	snapshotMs int64
	// planCacheRecorded marks the plan-cache note as taken.
	planCacheRecorded bool
}

// releaseProbe hands the probe back unless the breaker already heard from
// DuckDB. A pass that failed before DuckDB ran learned nothing about it.
func (p *pgxPass) releaseProbe() {
	if p.resolved {
		return
	}
	p.resolved = true
	p.e.breaker.ReleaseProbe(p.probe)
}

// scan is the failed-scan context failDuckDBScan classifies against.
func (p *pgxPass) scan() scan {
	p.resolved = true
	return scan{
		parquetPaths:    p.src.paths,
		pathsFromSource: p.src.fromSource,
		globListed:      p.src.globListed,
		probe:           p.probe,
	}
}

// pgxPage is what a pass answered: the page's records in order, and the
// matches the merge counted (Query's total).
type pgxPage struct {
	records []*model.PersistentRecord
	total   int64
}

// streamPgxPass runs one pgx pass for an admitted request and hands the
// page's records to rowHandler in order. It owns the probe from here on.
func (e *DBFederatedQueryEngine) streamPgxPass(
	ctx context.Context,
	tables model.StorageTables,
	q *model.FederatedAttributeQuery,
	opts *model.FederatedQueryOptions,
	planCtx *duckDBExecutionPlanContext,
	probe ProbeToken,
	rowHandler func(context.Context, *model.PersistentRecord) error,
) (int64, error) {
	p := &pgxPass{e: e, tables: tables, q: q, planCtx: planCtx, probe: probe, hasHot: sqlgen.FederatedQueryHasHot(q)}
	defer p.releaseProbe()
	if e.hotLeg == nil {
		return 0, fmt.Errorf("duckdb.hotLegTransport is pgx, but the engine has no snapshot pool (WithPgxHotLeg): %w", errPgxHotLegNotConfigured)
	}
	if tables.ChangeLog == "" {
		return 0, fmt.Errorf("the pgx hot leg reads the dirty set from the change log, and no change log table is configured: %w", errPgxHotLegNotConfigured)
	}
	order, err := sortkey.PageOrder(q)
	if err != nil {
		return 0, fmt.Errorf("build the page's order: %w", err)
	}
	p.order = order
	// The build's own refusals come before any read, as on the attach path:
	// a filter literal the attribute cannot take is invalid input, and a
	// schema without metadata fails with ErrSchemaMetadataCacheRequired.
	// Neither may reach the snapshot, whose failures are degradable.
	cache, _ := e.schemaCacheByID(q.SchemaID)
	if _, _, err := e.translateDuckDBQuery(ctx, tables.EAVData, q, cache, map[string]any{}); err != nil {
		return 0, fmt.Errorf("build the pgx pass over schema %d: %w", q.SchemaID, err)
	}
	src, err := e.resolvePgxScanSources(ctx, tables, q, p.hasHot)
	if err != nil {
		p.resolved = true
		return 0, e.failScanSources(ctx, q, probe, err)
	}
	p.src = src
	planCtx.recordEmptyParquetScan(src.emptyCandidate)
	if opts != nil {
		// A pass reads every listed object or fails (#348, #722).
		opts.PartialScan = nil
	}
	page, err := p.run(ctx)
	if err != nil {
		return 0, fmt.Errorf("pgx pass over schema %d: %w", q.SchemaID, err)
	}
	for _, record := range page.records {
		if rowHandler == nil {
			break
		}
		if err := rowHandler(ctx, record); err != nil {
			return 0, fmt.Errorf("handle row %s of the merged page: %w", record.RowID, err)
		}
	}
	return page.total, nil
}

// run reads the snapshot, runs the parquet leg, merges and reads the page's
// hot rows. The snapshot stays open across the DuckDB statement, so the
// page's hot rows come from the snapshot their keys came from.
func (p *pgxPass) run(ctx context.Context) (pgxPage, error) {
	start := p.planCtx.now()
	snap, err := p.e.hotLeg.begin(ctx)
	if err != nil {
		return pgxPage{}, fmt.Errorf("open the pass's snapshot: %w: %w", ErrPostgresReadFailed, err)
	}
	defer snap.rollback(ctx)

	dirty, hot, err := p.readSnapshot(ctx, snap)
	p.snapshotMs = p.planCtx.millisSince(start)
	if err != nil {
		return pgxPage{}, err
	}
	window, err := p.window(len(hot))
	if err != nil {
		return pgxPage{}, err
	}
	leg, err := p.parquetLeg(ctx, dirty, window)
	if err != nil {
		return pgxPage{}, err
	}
	if err := checkTiersDisjoint(p.q.SchemaID, dirty, leg.rows); err != nil {
		return pgxPage{}, err
	}
	entries := mergeWindow(p.order, hot, leg.rows, window, p.pageOffset(), max(0, p.q.Limit))

	readStart := p.planCtx.now()
	records, hotOnPage, err := p.readPage(ctx, snap, entries)
	p.snapshotMs += p.planCtx.millisSince(readStart)
	if err != nil {
		return pgxPage{}, err
	}
	if err := snap.commit(ctx); err != nil {
		return pgxPage{}, fmt.Errorf("%w: %w", ErrPostgresReadFailed, err)
	}
	if p.noObjects {
		// The recheck stays as it is on the attach path (#720).
		if err := p.e.confirmNoParquetPublished(ctx, p.q); err != nil {
			return pgxPage{}, err
		}
	}
	p.report(ctx, pgxOutcome{leg: leg, dirtyRows: len(dirty), hotMatches: len(hot), hotOnPage: hotOnPage})
	return pgxPage{records: records, total: int64(len(hot)) + leg.count}, nil
}

// readSnapshot reads the dirty set, settles a set without objects (#720),
// enforces HotLegMaxDirtyRows, and reads the hot keys, sorted by the page's
// order and filtered by its cursor.
func (p *pgxPass) readSnapshot(ctx context.Context, snap hotLegSnapshot) ([]uuid.UUID, []model.HotKey, error) {
	form := DirtySetHotExcluded
	if p.hasHot {
		form = DirtySetHot
	}
	cl, err := snap.readChangeLog(ctx, ChangeLogSnapshotRequest{
		Tables:         p.tables,
		SchemaID:       p.q.SchemaID,
		Form:           form,
		GraceCutoffMs:  p.src.graceCutoffMs,
		ParquetHistory: p.src.emptyCandidate,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("read the dirty set in the pass's snapshot: %w: %w", ErrPostgresReadFailed, err)
	}
	p.planCtx.recordSnapshotDirtySet(p.tables.ChangeLog, p.q.SchemaID, form, p.src.graceCutoffMs, len(cl.DirtyIDs))
	if p.src.emptyCandidate {
		if err := p.settleEmptyCandidate(ctx, cl.HasParquetHistory); err != nil {
			return nil, nil, err
		}
	}
	if limit := p.e.cfg.HotLegMaxDirtyRows; limit > 0 && len(cl.DirtyIDs) > limit {
		return nil, nil, fmt.Errorf("schema %d: the dirty set holds %d rows, past duckdb.hotLegMaxDirtyRows (%d), so the pass does not stage it: %w",
			p.q.SchemaID, len(cl.DirtyIDs), limit, ErrFederatedReadFailed)
	}
	if !p.hasHot {
		return cl.DirtyIDs, nil, nil
	}
	hot, err := p.readHotKeys(ctx, snap)
	if err != nil {
		return nil, nil, err
	}
	return cl.DirtyIDs, hot, nil
}

// readHotKeys reads the hot keys of the dirty window that match the
// request's condition, sorts them and applies the cursor.
func (p *pgxPass) readHotKeys(ctx context.Context, snap hotLegSnapshot) ([]model.HotKey, error) {
	names := make([]string, len(p.order.Columns))
	for i, col := range p.order.Columns {
		names[i] = col.Name
	}
	keys, err := snap.queryHotKeys(ctx, model.HotKeysQuery{
		Tables:        p.tables,
		SchemaID:      p.q.SchemaID,
		Condition:     p.q.Condition,
		GraceCutoffMs: p.src.graceCutoffMs,
		SortColumns:   names,
	})
	if err != nil {
		return nil, fmt.Errorf("read the hot keys in the pass's snapshot: %w: %w", ErrPostgresReadFailed, err)
	}
	for _, k := range keys {
		if err := p.order.CheckKey(k.Key); err != nil {
			return nil, fmt.Errorf("hot key of row %s: %w", k.RowID, err)
		}
	}
	sortHotKeys(p.order, keys)
	return admitHotKeys(p.order, keys, p.q.KeysetCursor)
}

// pageOffset is the first merged position of the page: the request's offset,
// or 0 under a cursor, which starts the page itself.
func (p *pgxPass) pageOffset() int {
	if p.q.KeysetCursor.IsActive() {
		return 0
	}
	return max(0, p.q.Offset)
}

// window is the parquet window the page can draw from (design.md §6.1.1,
// Pagination and totals).
func (p *pgxPass) window(hotMatches int) (sqlgen.ParquetWindow, error) {
	limit := max(0, p.q.Limit)
	if p.q.KeysetCursor.IsActive() {
		return sqlgen.KeysetPageParquetWindow(limit)
	}
	return sqlgen.OffsetPageParquetWindow(p.pageOffset(), limit, hotMatches)
}

// parquetLeg runs the parquet leg over window and reads its count, recounting
// when the window carried none (#181). A schema without parquet has no leg:
// it holds no row and counts 0.
func (p *pgxPass) parquetLeg(ctx context.Context, dirty []uuid.UUID, window sqlgen.ParquetWindow) (parquetLeg, error) {
	if p.noObjects {
		return parquetLeg{countKnown: true}, nil
	}
	leg, err := p.runParquetLeg(ctx, dirty, window)
	if err != nil {
		return parquetLeg{}, err
	}
	if !leg.countKnown {
		count, err := p.recountParquetLeg(ctx, dirty)
		if err != nil {
			return parquetLeg{}, err
		}
		leg.count, leg.countKnown = count, true
	}
	return leg, nil
}

// readPage reads the page's hot rows in the snapshot and returns the page's
// records in merged order, with the number that came from the hot leg.
func (p *pgxPass) readPage(ctx context.Context, snap hotLegSnapshot, entries []pageEntry) ([]*model.PersistentRecord, int, error) {
	var ids []uuid.UUID
	for _, entry := range entries {
		if entry.hot != nil {
			ids = append(ids, entry.hot.RowID)
		}
	}
	var hotRecords []*model.PersistentRecord
	if len(ids) > 0 {
		var err error
		hotRecords, err = snap.readHotRows(ctx, p.tables, p.q.SchemaID, ids)
		if err != nil {
			return nil, 0, fmt.Errorf("read the page's hot rows in the pass's snapshot: %w: %w", ErrPostgresReadFailed, err)
		}
		if len(hotRecords) != len(ids) {
			return nil, 0, fmt.Errorf("read the page's hot rows: %d records for %d row ids: %w", len(hotRecords), len(ids), ErrPostgresReadFailed)
		}
	}
	records := make([]*model.PersistentRecord, 0, len(entries))
	next := 0
	for _, entry := range entries {
		if entry.hot != nil {
			records = append(records, hotRecords[next])
			next++
			continue
		}
		records = append(records, entry.parquet)
	}
	return records, len(ids), nil
}
