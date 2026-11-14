package federated

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/redact"
	"github.com/lychee-technology/forma/internal/sortkey"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// The parquet leg of a pgx pass (design.md §6.1.1, step 4): the dirty set
// staged on a pinned DuckDB connection, the parquet-only statement run there
// over the frozen path set, and its window streamed with each row's sort key.

// parquetLeg is what the parquet leg of one pass answered.
type parquetLeg struct {
	rows []parquetRow
	// count is the parquet leg's COUNT(*) OVER(), when countKnown.
	count      int64
	countKnown bool
	// sqlStr and args are the statement, for the execution plan.
	sqlStr string
	args   []any
	// translateMs, executeMs and streamMs are the leg's stages, reported as
	// the attach pass reports its own (emitDuckDBScanMetrics). Each sums the
	// leg's statements: the page's, and the #181 recount's when the page
	// carried no count (addStages). streamMs is the leg's row iteration
	// until the pass hands the page to its row handler, which adds the
	// handler loop's time (streamPgxPass).
	translateMs, executeMs, streamMs int64
}

// addStages adds the stages of another statement of the leg, the #181
// recount, to the leg's own, so the pass reports all the DuckDB work it did.
func (l *parquetLeg) addStages(o parquetLeg) {
	l.translateMs += o.translateMs
	l.executeMs += o.executeMs
	l.streamMs += o.streamMs
}

// runParquetLeg renders the parquet-only statement for window, stages dirty
// and runs the statement on one connection, and collects the window's rows.
// A failure that reached DuckDB is classified and reported to the breaker
// as a failed attach scan is (failDuckDBScan).
func (p *pgxPass) runParquetLeg(ctx context.Context, dirty []uuid.UUID, window sqlgen.ParquetWindow) (parquetLeg, error) {
	e := p.e
	translateStart := p.planCtx.now()
	sqlStr, args, hit, err := e.buildParquetOnlyQueryCached(ctx, p.tables, p.q, p.q.AttributeOrders, p.q.Limit, p.q.Offset, window, p.src.paths, p.src.cold)
	if err != nil {
		p.releaseProbe()
		return parquetLeg{}, fmt.Errorf("build the parquet-only statement: %w", err)
	}
	if hit != nil && !p.planCacheRecorded {
		// The page's statement only: the #181 recount reuses its plan.
		p.planCacheRecorded = true
		p.planCtx.recordPlanCache(*hit)
	}
	leg := parquetLeg{sqlStr: sqlStr, args: args, translateMs: p.planCtx.millisSince(translateStart)}

	querier, ok := e.duck.(stagedQuerier)
	if !ok {
		p.releaseProbe()
		return parquetLeg{}, fmt.Errorf("the duckdb executor %T cannot stage the dirty set on a pinned connection: %w", e.duck, errPgxHotLegNotConfigured)
	}
	// Each statement of the pass, the #181 recount included, marks its own
	// start, so a failure's duckdb_fetch measures the attempt that failed.
	p.planCtx.recordQueryStart()
	executeStart := p.planCtx.startQuery
	rows, err := querier.queryStaged(ctx, dirty, sqlStr, args...)
	if err != nil {
		err = redact.Error(err)
		p.planCtx.recordQueryFailure(err)
		return parquetLeg{}, e.failDuckDBScan(ctx, p.q, p.scan(), err, "execute the parquet-only statement")
	}
	leg.executeMs = p.planCtx.millisSince(executeStart)
	streamStart := p.planCtx.now()
	collected, count, countSet, err := streamParquetLegRows(rows, p.order)
	// The connection goes back to the pool before anything else queries
	// DuckDB: verification below, or the next statement of the pass.
	_ = rows.Close()
	if err != nil {
		err = redact.Error(err)
		if !errors.Is(err, ErrFederatedReadFailed) {
			p.resolved = true
			e.breakerRecordFailure(ctx, err)
			return parquetLeg{}, fmt.Errorf("stream the parquet leg: %w", err)
		}
		return parquetLeg{}, e.failDuckDBScan(ctx, p.q, p.scan(), err, "stream the parquet leg")
	}
	leg.streamMs = p.planCtx.millisSince(streamStart)
	// DuckDB answered: the breaker hears so now, whatever the PostgreSQL
	// reads after it do.
	p.resolved = true
	e.breakerRecordSuccess(ctx)
	leg.rows, leg.count, leg.countKnown = collected, count, countSet || parquetCountKnown(window, len(collected))
	return leg, nil
}

// recountParquetLeg reads the parquet leg's count when the page's window
// carried none (#181): the same statement over the window [0, 1), on the same
// staged dirty set and frozen path set, so it counts what the window was cut
// from. It returns the count and the recount statement's leg, whose stages
// the pass adds to the page statement's.
func (p *pgxPass) recountParquetLeg(ctx context.Context, dirty []uuid.UUID) (int64, parquetLeg, error) {
	leg, err := p.runParquetLeg(ctx, dirty, sqlgen.ParquetWindow{Offset: 0, Limit: 1})
	if err != nil {
		return 0, parquetLeg{}, fmt.Errorf("recount the parquet leg: %w", err)
	}
	if leg.countKnown && len(leg.rows) > 0 {
		return leg.count, leg, nil
	}
	return 0, leg, nil
}

// streamParquetLegRows reads the parquet-only result: the record layout
// duckDBScanBuffers scans, then one sort-key column per column of order. It
// returns the rows, and the parquet count when a row carried it.
func streamParquetLegRows(rows duckDBRowsIterator, order sortkey.Order) ([]parquetRow, int64, bool, error) {
	buffers := newDuckDBScanBuffers()
	var out []parquetRow
	var total int64
	totalSet := false
	for rows.Next() {
		scanArgs, attrsJSON, totalRec, _, _ := buffers.buildScanArgs()
		holders := newSortKeyHolders(order)
		scanArgs = append(scanArgs, holders.dests...)
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, 0, false, fmt.Errorf("scan parquet leg row: %w: %w", ErrFederatedReadFailed, err)
		}
		record := buffers.buildRecordFromBuffers()
		if attrsJSON.Valid {
			if err := parseDuckDBAttributesJSON(attrsJSON.String, record); err != nil {
				return nil, 0, false, err
			}
		}
		model.CleanupEmptyMaps(record)
		key, err := holders.key(order)
		if err != nil {
			return nil, 0, false, fmt.Errorf("read the sort key of parquet row %s: %w", record.RowID, err)
		}
		if !totalSet && totalRec.Valid {
			total, totalSet = totalRec.Int64, true
		}
		out = append(out, parquetRow{record: record, key: key})
	}
	if err := rows.Err(); err != nil {
		return nil, 0, false, fmt.Errorf("iterate parquet leg rows: %w: %w", ErrFederatedReadFailed, err)
	}
	return out, total, totalSet, nil
}

// sortKeyHolders are the scan destinations of one row's sort-key columns.
type sortKeyHolders struct {
	dests []any
}

func newSortKeyHolders(order sortkey.Order) sortKeyHolders {
	dests := make([]any, len(order.Columns))
	for i, col := range order.Columns {
		if col.Type == sortkey.UUID {
			dests[i] = &nullDuckDBUUID{}
			continue
		}
		var v any
		dests[i] = &v
	}
	return sortKeyHolders{dests: dests}
}

// key converts the scanned values to the Go types the comparator reads.
func (h sortKeyHolders) key(order sortkey.Order) ([]any, error) {
	key := make([]any, len(order.Columns))
	for i, col := range order.Columns {
		var raw any
		switch d := h.dests[i].(type) {
		case *nullDuckDBUUID:
			if d.Valid {
				raw = d.UUID
			}
		case *any:
			raw = *d
		}
		v, err := parquetKeyValue(col.Type, raw)
		if err != nil {
			return nil, fmt.Errorf("sort column %q: %w", col.Name, err)
		}
		key[i] = v
	}
	return key, nil
}

// parquetKeyValue reads one scanned sort-key value as its export type's Go
// type (sortkey: int64, float64, string, bool, uuid.UUID; nil for NULL).
// The statement orders by the column as the visible CTE types it, which is
// the export type (a column-bound attribute included: the order reads its
// visible column, not the bound column's storage image), so a value of
// another kind means the two orders disagree and is refused rather than
// converted.
func parquetKeyValue(t sortkey.ExportType, raw any) (any, error) {
	if raw == nil {
		return nil, nil
	}
	switch t {
	case sortkey.BigInt, sortkey.Integer, sortkey.SmallInt:
		if n, ok := scannedInteger(raw); ok {
			return n, nil
		}
	case sortkey.Double:
		switch v := raw.(type) {
		case float64:
			return v, nil
		case float32:
			return float64(v), nil
		}
	case sortkey.Varchar:
		switch v := raw.(type) {
		case string:
			return v, nil
		case []byte:
			return uuidTextKey(v)
		}
	case sortkey.Boolean:
		if v, ok := raw.(bool); ok {
			return v, nil
		}
	case sortkey.UUID:
		if v, ok := raw.(uuid.UUID); ok {
			return v, nil
		}
	}
	return nil, fmt.Errorf("the parquet leg returned %T for a %s sort key", raw, t)
}

// uuidTextKey reads a VARCHAR sort key that DuckDB returned as bytes. A
// column-bound uuid attribute exports through TRY_CAST(col AS UUID), so its
// parquet column is a DuckDB UUID, which the driver scans as its 16 bytes.
// Its key is the canonical text the hot leg derives (sortkey.DeriveAttribute),
// and DuckDB orders a UUID as that text orders (TestParquetUUIDSortKey). Read
// as text, the 16 bytes compared against the hot keys by their raw values and
// misplaced every hot row (#857). Any other bytes are refused.
func uuidTextKey(raw []byte) (any, error) {
	if len(raw) == 16 {
		if u, err := uuid.FromBytes(raw); err == nil {
			return u.String(), nil
		}
	}
	return nil, fmt.Errorf("the parquet leg returned %d bytes for a VARCHAR sort key, which only a uuid attribute's 16 bytes may stand for", len(raw))
}

// scannedInteger widens a scanned integer of any width to int64.
func scannedInteger(raw any) (int64, bool) {
	switch v := raw.(type) {
	case int64:
		return v, true
	case int32:
		return int64(v), true
	case int16:
		return int64(v), true
	case int8:
		return int64(v), true
	case int:
		return int64(v), true
	case uint8:
		return int64(v), true
	case uint16:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint64:
		if v <= math.MaxInt64 {
			return int64(v), true
		}
	}
	return 0, false
}
