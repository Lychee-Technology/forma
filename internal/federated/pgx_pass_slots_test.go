package federated

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// signallingHotLeg reports each snapshot a pass opens.
type signallingHotLeg struct {
	*fakeHotLeg
	begun chan struct{}
}

func (s signallingHotLeg) begin(ctx context.Context) (hotLegSnapshot, error) {
	s.begun <- struct{}{}
	return s.fakeHotLeg.begin(ctx)
}

// A pass waits for a slot before it opens its snapshot, so the pgx passes in
// flight hold at most DuckDBConfig.MaxConnections pooled PostgreSQL
// connections. While the only slot is taken, a pass opens no snapshot; once
// it is released, the pass runs and answers.
func TestPgxPassWaitsForASlotBeforeItsSnapshot(t *testing.T) {
	ctx := context.Background()
	f := newPgxFixture(t, tieRunRows())
	e := f.engine(t, []string{f.dir + "/base.parquet"})
	require.Equal(t, 1, cap(e.pgxPassSlots), "an unset duckdb.maxConnections admits one pass")
	leg := signallingHotLeg{fakeHotLeg: f.leg, begun: make(chan struct{}, 1)}
	e.hotLeg = leg

	release, err := e.acquirePgxPassSlot(ctx)
	require.NoError(t, err)
	type result struct {
		page *model.PersistentRecordPage
		err  error
	}
	done := make(chan result, 1)
	go func() {
		page, err := e.Query(ctx, tieRunTables, pgxQuery(), nil)
		done <- result{page, err}
	}()
	select {
	case <-leg.begun:
		t.Fatal("the pass opened its snapshot while every slot was taken")
	case r := <-done:
		t.Fatalf("the pass answered while every slot was taken: %v", r.err)
	case <-time.After(100 * time.Millisecond):
	}
	release()
	<-leg.begun
	r := <-done
	require.NoError(t, r.err)
	require.EqualValues(t, 12, r.page.TotalRecords)
	require.Len(t, e.pgxPassSlots, 0, "the pass returns its slot")
}

// A request whose context ends while it waits opens no snapshot, fails with
// its context's error, and hands the breaker's probe back.
func TestPgxPassSlotWaitEndsWithTheRequest(t *testing.T) {
	f := newPgxFixture(t, tieRunRows())
	e := f.engine(t, []string{f.dir + "/base.parquet"})
	release, err := e.acquirePgxPassSlot(context.Background())
	require.NoError(t, err)
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = e.Query(ctx, tieRunTables, pgxQuery(), nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.DeadlineExceeded), "got %v", err)
	require.Zero(t, f.leg.begins)
}

// The bound follows the DuckDB pool's size.
func TestPgxPassSlotsFollowDuckDBMaxConnections(t *testing.T) {
	for _, tc := range []struct{ maxConnections, want int }{{-1, 1}, {0, 1}, {1, 1}, {4, 4}} {
		require.Equal(t, tc.want, cap(newPgxPassSlots(tc.maxConnections)), "maxConnections %d", tc.maxConnections)
	}
}
