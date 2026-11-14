package federated

import (
	"context"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// The pgx pass seen through Query: totals, the #181 recount, and the
// degraded fallback.

func pgxRoutedQuery(offset, limit int) *model.FederatedAttributeQuery {
	q := pgxQuery()
	q.Offset, q.Limit = offset, limit
	// An explicit multi-tier request routes to the federated path (#468).
	q.PreferredTiers = []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold}
	return q
}

// A page past the last match reports the full count from its own pass: the
// pass recounts the parquet leg in its snapshot, so Query runs no second
// pass for the #181 recount.
func TestPgxQueryCountsAnEmptyDeepPageInOnePass(t *testing.T) {
	f := newPgxFixture(t, tieRunRows())
	e := f.engine(t, []string{f.dir + "/base.parquet"})
	page, err := e.Query(context.Background(), tieRunTables, pgxRoutedQuery(40, 5), nil)
	require.NoError(t, err)
	require.Empty(t, page.Records)
	require.Equal(t, int64(12), page.TotalRecords)
	require.Equal(t, 3, page.TotalPages)
	require.Equal(t, 1, f.leg.begins, "one pass, one snapshot")

	page, err = e.Query(context.Background(), tieRunTables, pgxRoutedQuery(10, 5), nil)
	require.NoError(t, err)
	require.Len(t, page.Records, 2)
	require.Equal(t, int64(12), page.TotalRecords)
}

// A pass refused past HotLegMaxDirtyRows is degradable: under
// AllowPartialDegradedMode, PostgreSQL alone answers it.
func TestPgxQueryDegradesADirtySetPastTheCap(t *testing.T) {
	f := newPgxFixture(t, tieRunRows())
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 12}}
	e := f.engine(t, []string{f.dir + "/base.parquet"})
	e.pgSource = pg
	e.cfg.HotLegMaxDirtyRows = 1
	opts := &model.FederatedQueryOptions{AllowPartialDegradedMode: true}
	page, err := e.Query(context.Background(), tieRunTables, pgxRoutedQuery(0, 5), opts)
	require.NoError(t, err)
	require.Equal(t, 1, pg.queryCalls, "the fallback reads PostgreSQL alone")
	require.Equal(t, int64(12), page.TotalRecords)
}

// Engine construction says once that a negative grace reads as 0; a
// non-negative one logs nothing.
func TestPgxEngineLogsANegativeGraceOnce(t *testing.T) {
	build := func(grace int64, opts ...EngineOption) *observer.ObservedLogs {
		core, logs := observer.New(zap.WarnLevel)
		cfg := forma.DuckDBConfig{FlushVisibilityGraceMs: grace}
		all := append([]EngineOption{WithLogger(zap.New(core))}, opts...)
		NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, nil, nil, cfg, nil, all...)
		return logs
	}
	logs := build(-1)
	require.Equal(t, 1, logs.Len())
	require.Contains(t, logs.All()[0].Message, "reads as 0")
	require.Zero(t, build(0).Len())
	require.Equal(t, 1, build(0, WithFlushVisibilityGrace(-time.Second)).Len(),
		"a negative WithFlushVisibilityGrace is read the same way")
}
