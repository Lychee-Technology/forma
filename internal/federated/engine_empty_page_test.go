package federated

import (
	"context"
	"testing"
	"text/template"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/stretchr/testify/require"
)

// sequencedDuckDBExecutor returns one prepared iterator per Query call, so a
// test can hand the paged query an empty result and the fallback recount a
// row that carries the window total.
type sequencedDuckDBExecutor struct {
	calls int
	rows  []duckDBRowsIterator
}

func (f *sequencedDuckDBExecutor) Query(ctx context.Context, sql string, args ...any) (duckDBRowsIterator, error) {
	idx := f.calls
	f.calls++
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

func newEmptyPageTestEngine(t *testing.T, duck DuckDBQueryExecutor, builtQueries *[]model.FederatedAttributeQuery) *DBFederatedQueryEngine {
	t.Helper()
	engine := NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, &fakeDirtyIDFetcher{}, duck, nil,
		forma.DuckDBConfig{Enabled: true, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid}},
		testMetadataCacheSchema7(t), "")
	engine.buildDuckSQL = func(tpl *template.Template, params any, q *model.FederatedAttributeQuery, dirtyIDs []uuid.UUID, dual *sqlgen.DualClauses) (string, []any, error) {
		*builtQueries = append(*builtQueries, *q)
		return "SELECT fake", nil, nil
	}
	return engine
}

// TestDBFederatedQueryEngine_EmptyPageRecountsTotal pins the #181 fallback:
// the DuckDB template carries COUNT(*) OVER() on data rows, so a page beyond
// the last match streams zero rows and the engine must recount via
// computeFederatedCount instead of reporting totalRecords=0.
func TestDBFederatedQueryEngine_EmptyPageRecountsTotal(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &sequencedDuckDBExecutor{rows: []duckDBRowsIterator{
		&emptyDuckDBRows{},
		&totalOnlyDuckDBRow{total: 42},
	}}
	var built []model.FederatedAttributeQuery
	engine := newEmptyPageTestEngine(t, duck, &built)

	page, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		&model.FederatedAttributeQuery{
			AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10, Offset: 50},
			// Cold-only tiers force the DuckDB route under the hybrid
			// strategy regardless of page size.
			PreferredTiers: []model.DataTier{model.DataTierWarm, model.DataTierCold},
		}, nil)

	require.NoError(t, err)
	require.Empty(t, page.Records)
	require.Equal(t, int64(42), page.TotalRecords)
	require.Equal(t, model.ComputeTotalPages(42, 10), page.TotalPages)
	require.Equal(t, 2, duck.calls, "empty deep page must trigger exactly one recount")
	// The advanced template renders LIMIT/OFFSET from the query object, so the
	// recount must reach the builder with the pagination zeroed — passing
	// limit/offset only as call parameters re-renders the deep offset (#181).
	require.Len(t, built, 2)
	require.Equal(t, 50, built[0].Offset)
	require.Equal(t, 1, built[1].Limit, "recount must render LIMIT 1")
	require.Equal(t, 0, built[1].Offset, "recount must render OFFSET 0")
}

// TestDBFederatedQueryEngine_EmptyResultAtOffsetZeroSkipsRecount guards the
// other half of the #181 condition: at offset 0 an empty result is a genuine
// total of 0 and no second query may fire.
func TestDBFederatedQueryEngine_EmptyResultAtOffsetZeroSkipsRecount(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &sequencedDuckDBExecutor{rows: []duckDBRowsIterator{&emptyDuckDBRows{}}}
	var built []model.FederatedAttributeQuery
	engine := newEmptyPageTestEngine(t, duck, &built)

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
