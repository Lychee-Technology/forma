package federated

import (
	"context"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// sequencedDuckDBExecutor returns one prepared iterator (or error) per Query
// call, so a test can hand the paged query an empty result and the recount a
// row that carries the window total — or a failure. sqls and args record
// each main statement, so a test can read the window it rendered (window).
type sequencedDuckDBExecutor struct {
	calls int
	rows  []duckDBRowsIterator
	errs  []error
	sqls  []string
	args  [][]any
}

func (f *sequencedDuckDBExecutor) Query(ctx context.Context, sql string, args ...any) (duckDBRowsIterator, error) {
	// Per-file drains are answered clean off-sequence: the sequence scripts
	// main scans, and duck.calls is asserted as a main-scan odometer
	// (see answerParquetDrainSQL).
	if drained, ok := answerParquetDrainSQL(sql); ok {
		return drained, nil
	}
	idx := f.calls
	f.calls++
	f.sqls, f.args = append(f.sqls, sql), append(f.args, args)
	if idx < len(f.errs) && f.errs[idx] != nil {
		return nil, f.errs[idx]
	}
	if idx >= len(f.rows) {
		return &emptyDuckDBRows{}, nil
	}
	return f.rows[idx], nil
}

type emptyDuckDBRows struct{}

func (e *emptyDuckDBRows) Next() bool             { return false }
func (e *emptyDuckDBRows) Scan(dest ...any) error { return nil }
func (e *emptyDuckDBRows) Err() error             { return nil }
func (e *emptyDuckDBRows) Close() error           { return nil }

// totalOnlyDuckDBRow yields a single row whose total_records column carries
// the given count — the shape the limit-1/offset-0 recount comes back in.
type totalOnlyDuckDBRow struct {
	total   int64
	scanned bool
}

func (r *totalOnlyDuckDBRow) Next() bool {
	if r.scanned {
		return false
	}
	r.scanned = true
	return true
}

func (r *totalOnlyDuckDBRow) Scan(dest ...any) error {
	return populateDuckDBRow(dest, 7, uuid.New(), 10, 20, nil, "[]", r.total)
}

func (r *totalOnlyDuckDBRow) Err() error   { return nil }
func (r *totalOnlyDuckDBRow) Close() error { return nil }

// newEmptyPageTestEngine builds an engine over duck whose passes read an
// empty snapshot.
func newEmptyPageTestEngine(t *testing.T, pg *fakePostgresFederatedSource, duck DuckDBQueryExecutor) *DBFederatedQueryEngine {
	t.Helper()
	return NewDBFederatedQueryEngine(pg, duck, nil,
		forma.DuckDBConfig{Enabled: true, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid}},
		testMetadataCacheSchema7(t), withTestParquetPath(), withFakeHotLeg(nil))
}

// renderedWindow is the page form and the window of one parquet-only
// statement: a keyset page binds LIMIT ? last, an offset page LIMIT ?
// OFFSET ?.
type renderedWindow struct {
	keyset        bool
	limit, offset int64
}

// window reads the i-th main statement f recorded.
func (f *sequencedDuckDBExecutor) window(t *testing.T, i int) renderedWindow {
	t.Helper()
	require.Greater(t, len(f.args), i, "statement %d did not run", i)
	args := f.args[i]
	if !strings.Contains(f.sqls[i], "OFFSET ?") {
		require.NotEmpty(t, args)
		return renderedWindow{keyset: true, limit: args[len(args)-1].(int64)}
	}
	require.GreaterOrEqual(t, len(args), 2)
	return renderedWindow{limit: args[len(args)-2].(int64), offset: args[len(args)-1].(int64)}
}

// TestDBFederatedQueryEngine_EmptyResultAtOffsetZeroSkipsRecount guards the
// #181 recount's condition: at offset 0 an empty result is a genuine total
// of 0 and no second query may fire. (A page past the last match counts
// itself: TestPgxQueryCountsAnEmptyDeepPageInOnePass.)
func TestDBFederatedQueryEngine_EmptyResultAtOffsetZeroSkipsRecount(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &sequencedDuckDBExecutor{rows: []duckDBRowsIterator{&emptyDuckDBRows{}}}
	engine := newEmptyPageTestEngine(t, &fakePostgresFederatedSource{}, duck)

	page, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		&model.FederatedAttributeQuery{
			AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
			PreferredTiers: []model.DataTier{model.DataTierWarm, model.DataTierCold},
		}, nil)

	require.NoError(t, err)
	require.Empty(t, page.Records)
	require.Equal(t, int64(0), page.TotalRecords)
	require.Equal(t, 1, duck.calls, "offset 0 empty result must not recount")
}
