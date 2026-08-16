package federated

// Shared engine-seam test fakes, extracted from engine_test.go as a pure
// move to keep it under the 500-line limit.

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/model"
)

type fakePostgresFederatedSource struct {
	queryCalls int
	lastQuery  *model.PersistentRecordQuery
	page       *model.PersistentRecordPage
}

func (f *fakePostgresFederatedSource) QueryPersistentRecords(ctx context.Context, query *model.PersistentRecordQuery) (*model.PersistentRecordPage, error) {
	f.queryCalls++
	f.lastQuery = query
	if f.page != nil {
		return f.page, nil
	}
	return &model.PersistentRecordPage{}, nil
}

func (f *fakePostgresFederatedSource) RunOptimizedQuery(ctx context.Context, tables model.StorageTables, schemaID int16, clause string, args []any, limit, offset int, attributeOrders []model.AttributeOrder, useMainTableAsAnchor bool) ([]*model.PersistentRecord, int64, error) {
	return nil, 0, nil
}

func (f *fakePostgresFederatedSource) BuildHybridConditions(tables model.StorageTables, fq *model.FederatedAttributeQuery) (string, []any, error) {
	return "1=1", nil, nil
}

type fakeDirtyIDFetcher struct {
	calls int
	ids   []uuid.UUID
	err   error
}

func (f *fakeDirtyIDFetcher) FetchDirtyRowIDs(ctx context.Context, changeLogTable string, schemaID int16) ([]uuid.UUID, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.ids, nil
}

type fakeDuckDBExecutor struct {
	calls         int
	describeCalls int
	lastSQL       string
	lastArgs      []any
	err           error
	rows          duckDBRowsIterator

	// globFiles, when non-nil, is the expansion every glob probe returns
	// instead of the default "listing not faked" error. describeCols, when
	// non-nil, answers DESCRIBE probes per path: the first map key that is a
	// substring of the probe SQL wins (like scriptedDescribeExecutor).
	// Both nil — the default — keeps the behavior every other engine-seam
	// test relies on: fixed system columns, unlistable globs.
	globFiles    []string
	describeCols map[string][][2]string

	// drainCalls counts the per-file drains the failure path issues (#251
	// verification and #351 guard identification). Counted separately from
	// calls for the same reason describeCalls is: `calls`/`rows`/`err` model
	// the MAIN scan, and every test that asserts on them means main scans.
	drainCalls int
}

// isBareParquetDrainSQL / isGuardedParquetDrainSQL recognize the two per-file
// drain renderings by their SQL shape, never by driver text: verifyParquetPaths
// (#251) drains `SELECT * FROM read_parquet('p')`, while identifyGuardViolations
// (#351) drains through sqlgen.BuildParquetScanSource, which wraps the read in
// the guarded `(SELECT * REPLACE (...) FROM ...) AS cold_scan` sub-select. A
// rendered main scan is the advanced template (`WITH dirty_ids AS (...`) and
// matches neither.
func isBareParquetDrainSQL(sqlStr string) bool {
	return strings.HasPrefix(sqlStr, "SELECT * FROM read_parquet(")
}

func isGuardedParquetDrainSQL(sqlStr string) bool {
	return strings.HasPrefix(sqlStr, "SELECT * FROM (SELECT ") &&
		strings.HasSuffix(sqlStr, ") AS cold_scan")
}

// answerParquetDrainSQL answers a per-file drain with a clean, BOUNDED
// iterator and reports false for anything else. Every sequencing fake in this
// package scripts MAIN scans; letting a drain consume the next scripted
// outcome would both spend a slot the test never budgeted and risk spinning
// forever, because a main-scan iterator may legitimately be unbounded
// (failingScanRows.Next never returns false) while a drain iterates to
// exhaustion. Clean is also the neutral answer: no test that merely fails a
// main scan then acquires a corruption (#251) or guard-violation (#351)
// verdict it never asked for.
func answerParquetDrainSQL(sqlStr string) (duckDBRowsIterator, bool) {
	if isBareParquetDrainSQL(sqlStr) || isGuardedParquetDrainSQL(sqlStr) {
		return &verifyFakeRows{rowsLeft: 1}, true
	}
	return nil, false
}

func (f *fakeDuckDBExecutor) Query(ctx context.Context, sql string, args ...any) (duckDBRowsIterator, error) {
	if strings.HasPrefix(sql, "DESCRIBE ") {
		// The pre-read schema validator (#189) probes each concrete parquet
		// path before the main scan; answer with the invariant-satisfying
		// system columns so engine-seam tests keep exercising the main query
		// without the probe consuming the canned rows or lastSQL. Probes are
		// counted separately so breaker tests can assert none reach storage.
		f.describeCalls++
		if f.describeCols != nil {
			for path, cols := range f.describeCols {
				if strings.Contains(sql, path) {
					return &fakeDescribeRows{cols: cols}, nil
				}
			}
			return nil, fmt.Errorf("unexpected describe probe: %s", sql)
		}
		return &fakeDescribeRows{cols: [][2]string{
			{"row_id", "UUID"}, {"changed_at", "BIGINT"}, {"deleted_at", "BIGINT"},
		}}, nil
	}
	if strings.HasPrefix(sql, "SELECT file FROM glob(") {
		// Glob expansion probe (#189): listing is not faked — inconclusive,
		// mirroring an unreachable store; the validator defers to the main
		// read.
		f.describeCalls++
		if f.globFiles != nil {
			return &fakeStringRows{vals: f.globFiles}, nil
		}
		return nil, fmt.Errorf("glob listing not faked")
	}
	if drained, ok := answerParquetDrainSQL(sql); ok {
		f.drainCalls++
		return drained, nil
	}
	f.calls++
	f.lastSQL = sql
	f.lastArgs = args
	if f.err != nil {
		return nil, f.err
	}
	return f.rows, nil
}

type singleDuckDBRow struct {
	rowID   uuid.UUID
	scanned bool
}

func (r *singleDuckDBRow) Next() bool {
	if r.scanned {
		return false
	}
	r.scanned = true
	return true
}

func (r *singleDuckDBRow) Scan(dest ...any) error {
	return populateDuckDBRow(dest, 7, r.rowID, 10, 20, nil, "[]", 1)
}

func (r *singleDuckDBRow) Err() error {
	return nil
}

func (r *singleDuckDBRow) Close() error {
	return nil
}
