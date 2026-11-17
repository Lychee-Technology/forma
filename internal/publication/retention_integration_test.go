package publication

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestRetentionKeepsSupersededGenerationsForTheGrace pins the collector's
// view: the current generation is always retained, a superseded one until its
// grace has passed on the database clock, and a history row is forgotten only
// once expired.
func TestRetentionKeepsSupersededGenerationsForTheGrace(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	cur := f.initialized(t, 100)
	for range 3 {
		next, err := f.publish(ctx, cur, f.next(t, cur, KindFlush), nil)
		require.NoError(t, err)
		cur = next
	}

	retained, err := Retained(ctx, f.db, f.t, 100, time.Hour)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 3}, numbers(retained))
	expired, err := Expired(ctx, f.db, f.t, 100, time.Hour)
	require.NoError(t, err)
	require.Empty(t, expired)
	ok, err := Forget(ctx, f.db, f.t, 100, 1, time.Hour)
	require.NoError(t, err)
	require.False(t, ok, "a generation inside its grace is not forgotten")

	expired, err = Expired(ctx, f.db, f.t, 100, 0)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, numbers(expired), "the current generation never expires")
	require.Equal(t, cur.ManifestKey, mustRetained(t, f, 0)[0].ManifestKey)
	for _, g := range expired {
		ok, err := Forget(ctx, f.db, f.t, 100, g.Generation, 0)
		require.NoError(t, err)
		require.True(t, ok)
	}
	require.Equal(t, []int64{3}, numbers(mustRetained(t, f, time.Hour)))
}

func mustRetained(t *testing.T, f *fixture, grace time.Duration) []Generation {
	t.Helper()
	gens, err := Retained(context.Background(), f.db, f.t, 100, grace)
	require.NoError(t, err)
	return gens
}

func numbers(gens []Generation) []int64 {
	out := make([]int64, 0, len(gens))
	for _, g := range gens {
		out = append(out, g.Generation)
	}
	return out
}
