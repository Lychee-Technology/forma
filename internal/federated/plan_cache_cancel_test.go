package federated

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/queryplan"
	"github.com/stretchr/testify/require"
)

// TestBuildDuckDBQueryCancelledPlanWaiterDoesNotRenderDirectly pins the
// federated half of #467: the plan cache hands a waiter its context error
// promptly, and buildDuckDBQueryWithPlan must surface that error rather than
// treat it as a miss and render the query through the direct builder, which
// takes no context. The compile is held in flight by occupying the request's
// own cache key, since sqlgen.CompileDuckDBQuery offers nothing to block on.
func TestBuildDuckDBQueryCancelledPlanWaiterDoesNotRenderDirectly(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	e := NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, &fakeDirtyIDFetcher{}, &fakeDuckDBExecutor{}, nil,
		hybridDuckConfig(), testMetadataCacheSchema7(t), "host=x",
		WithPlanCache(queryplan.NewCache(16)), withTestParquetPath())

	tables := model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"}
	q := coldTierQuery()
	paths := []string{testParquetPath}
	const limit, offset = 25, 0
	key, err := e.duckPlanCacheKey(tables, dispatchedQuery(q, limit, offset, nil), nil, nil, limit, offset, paths, coldScanSet{})
	require.NoError(t, err)

	// Leader: holds the slot until released, then caches nothing so the
	// sanity call below compiles for real.
	started := make(chan struct{})
	release := make(chan struct{})
	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		_, _, _ = e.planCache.GetOrBuild(context.Background(), key, func() (any, error) {
			close(started)
			<-release
			return nil, errors.New("synthetic leader build")
		})
	}()
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sqlStr, args, _, err := e.buildDuckDBQueryWithPlan(ctx, tables, q, nil, nil, limit, offset, paths, 0, coldScanSet{},
		newDuckDBExecutionPlanContext(nil, time.Now))
	require.ErrorIs(t, err, context.Canceled,
		"a waiter whose context ended must get that error back, not a directly rendered query")
	require.Empty(t, sqlStr)
	require.Nil(t, args)

	close(release)
	select {
	case <-leaderDone:
	case <-time.After(5 * time.Second):
		t.Fatal("leader did not finish after release")
	}

	// The same inputs render with a live context: the direct builder would
	// have produced SQL for the cancelled caller, which is what the
	// assertion above rules out.
	sqlStr, _, _, err = e.buildDuckDBQueryWithPlan(context.Background(), tables, q, nil, nil, limit, offset, paths, 0, coldScanSet{},
		newDuckDBExecutionPlanContext(nil, time.Now))
	require.NoError(t, err)
	require.NotEmpty(t, sqlStr)
}
