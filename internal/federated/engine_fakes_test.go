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
	err        error
}

func (f *fakePostgresFederatedSource) QueryPersistentRecords(ctx context.Context, query *model.PersistentRecordQuery) (*model.PersistentRecordPage, error) {
	f.queryCalls++
	f.lastQuery = query
	if f.err != nil {
		return nil, f.err
	}
	if f.page != nil {
		return f.page, nil
	}
	return &model.PersistentRecordPage{}, nil
}

// stagedFake lends a Query-only DuckDB fake the staged seam a federated
// pass runs its parquet leg through (stagedQuerier): the dirty set is not
// staged, and the statement goes to Query like any other.
type stagedFake struct{ DuckDBQueryExecutor }

func (s stagedFake) queryStaged(ctx context.Context, _ []uuid.UUID, sqlStr string, args ...any) (duckDBRowsIterator, error) {
	return s.Query(ctx, sqlStr, args...)
}

// withFakeHotLeg runs the engine's passes over leg, an in-memory snapshot,
// and lends the DuckDB fake the engine was built with the staged seam, so
// an engine-seam test built on a Query-only fake runs the federated pass.
// A nil leg is an empty snapshot: no dirty row and no hot row. A leg with
// hot records needs its t set.
func withFakeHotLeg(leg *fakeHotLeg) EngineOption {
	if leg == nil {
		leg = &fakeHotLeg{}
	}
	if leg.records == nil {
		leg.records = map[uuid.UUID]pgxRow{}
	}
	return func(e *DBFederatedQueryEngine) {
		e.hotLeg = leg
		if _, ok := e.duck.(stagedQuerier); !ok && e.duck != nil {
			e.duck = stagedFake{e.duck}
		}
	}
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
}

// isBareParquetDrainSQL / isGuardedParquetDrainSQL recognize the two per-file
// drain renderings by their SQL shape, never by driver text: verifyParquetPaths
// (#251) drains `SELECT * FROM read_parquet('p')`, while identifyGuardViolations
// (#351) drains through sqlgen.BuildParquetScanSource, which wraps the read in
// the guarded `(SELECT * REPLACE (...) FROM ...) AS cold_scan` sub-select. A
// rendered main scan is the parquet-only template (`WITH s3_source AS (...`)
// and matches neither.
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
			{"row_id", "UUID"}, {"changed_at", "BIGINT"}, {"deleted_at", "BIGINT"}, {"ltbase_created_at", "BIGINT"},
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
		// Per-file drains (#251 verification, #351 identification) never touch
		// calls/lastSQL/rows: those model the MAIN scan, and every test that
		// asserts on them means main scans.
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
