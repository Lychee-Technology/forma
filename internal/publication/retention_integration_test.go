package publication

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestRetentionKeepsSupersededGenerationsForTheGrace pins the collector's
// view: the current generation is always retained, a superseded one until a
// collector retired it and its grace has passed on the database clock, and a
// history row is forgotten only once expired.
func TestRetentionKeepsSupersededGenerationsForTheGrace(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	cur := f.initialized(t, 100)
	for range 3 {
		next, err := f.publish(ctx, cur, f.next(t, cur, KindFlush), nil)
		require.NoError(t, err)
		cur = next
	}

	require.Equal(t, []int64{1, 2, 3}, numbers(mustRetained(t, f, time.Hour)))
	expired, err := Expired(ctx, DB(f.db), f.t, 100, 0)
	require.NoError(t, err)
	require.Empty(t, expired, "a superseded generation no collector retired never expires, whatever its superseded_at")

	for g, want := range map[int64]bool{1: true, 2: true, 3: false} {
		ok, err := Retire(ctx, DB(f.db), f.t, 100, g)
		require.NoError(t, err)
		require.Equal(t, want, ok, "retire generation %d", g)
	}
	ok, err := Retire(ctx, DB(f.db), f.t, 100, 1)
	require.NoError(t, err)
	require.False(t, ok, "a generation is retired once: a later pass does not push its time back")

	require.Equal(t, []int64{1, 2, 3}, numbers(mustRetained(t, f, time.Hour)))
	ok, err = Forget(ctx, DB(f.db), f.t, 100, 1, time.Hour)
	require.NoError(t, err)
	require.False(t, ok, "a generation inside its grace is not forgotten")

	expired, err = Expired(ctx, DB(f.db), f.t, 100, 0)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, numbers(expired), "the current generation never expires")
	head := mustRetained(t, f, 0)[0]
	require.Equal(t, []any{cur.ManifestKey, cur.ManifestSHA256, cur.Token}, []any{head.ManifestKey, head.ManifestSHA256, head.Token},
		"the history row carries what a collector verifies the manifest against")
	for _, g := range expired {
		ok, err := Forget(ctx, DB(f.db), f.t, 100, g.Generation, 0)
		require.NoError(t, err)
		require.True(t, ok)
	}
	require.Equal(t, []int64{3}, numbers(mustRetained(t, f, time.Hour)))
}

// TestRetireCountsFromTheReplacingCommit is the late-commit schedule for the
// reader's grace: the transaction that replaces generation 1 (a publication,
// or a drop) starts, a reader chooses generation 1 in a snapshot taken after
// that start, and the replacement commits only then. superseded_at is the
// replacement's start, before the reader's snapshot, so a grace counted from
// it would be short by however long the commit was late. Retire, run after a
// snapshot that saw the replacement committed, stamps a time after the
// reader's snapshot.
func TestRetireCountsFromTheReplacingCommit(t *testing.T) {
	for name, replace := range map[string]func(context.Context, Tx, *fixture, Record) error{
		"publication": func(ctx context.Context, tx Tx, f *fixture, cur Record) error {
			_, err := Publish(ctx, tx, f.t, cur, f.next(t, cur, KindFlush))
			return err
		},
		"drop": func(ctx context.Context, tx Tx, f *fixture, cur Record) error {
			_, err := Reset(ctx, tx, f.t, cur)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			ctx := context.Background()
			g1, err := f.publish(ctx, f.initialized(t, 100), f.next(t, f.read(t, 100), KindFlush), nil)
			require.NoError(t, err)

			late, err := f.db.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer func() { _ = late.Rollback() }()
			require.NoError(t, replace(ctx, SQLTx(late), f, g1))
			time.Sleep(50 * time.Millisecond)

			reader, err := f.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
			require.NoError(t, err)
			defer func() { _ = reader.Rollback() }()
			chosen, err := Read(ctx, SQLTx(reader), f.t, 100)
			require.NoError(t, err)
			require.Equal(t, g1.Token, chosen.Token, "the reader chooses generation 1: its replacement has not committed")
			time.Sleep(50 * time.Millisecond)
			require.NoError(t, late.Commit())

			gens := mustRetained(t, f, time.Hour)
			require.NotZero(t, gens[0].SupersededAt)
			require.Less(t, gens[0].SupersededAt, chosen.ReadAt, "superseded_at predates the reader's snapshot")
			ok, err := Retire(ctx, DB(f.db), f.t, 100, 1)
			require.NoError(t, err)
			require.True(t, ok)
			retired := mustRetained(t, f, time.Hour)[0]
			require.Greater(t, retired.RetiredAt, chosen.ReadAt, "the grace counts from after the reader's snapshot")
		})
	}
}

// TestRaiseHorizonFencesAStagedPublication pins the collector's fence: once
// the horizon is at or past a writer's staging time, its publication cannot
// commit, whatever its budget; the record's token is unchanged, so only the
// staging is refused.
func TestRaiseHorizonFencesAStagedPublication(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	cur := f.initialized(t, 100)
	next := f.next(t, cur, KindFlush)
	time.Sleep(5 * time.Millisecond)

	horizon, err := RaiseHorizon(ctx, DB(f.db), f.t, 100, 0)
	require.NoError(t, err)
	require.GreaterOrEqual(t, horizon, next.StagedAt)
	lower, err := RaiseHorizon(ctx, DB(f.db), f.t, 100, time.Hour)
	require.NoError(t, err)
	require.Equal(t, horizon, lower, "the horizon never moves back")
	fenced := f.read(t, 100)
	require.Equal(t, horizon, fenced.Horizon)
	require.Equal(t, cur.Token, fenced.Token)

	_, err = f.publish(ctx, cur, next, nil)
	require.ErrorIs(t, err, ErrStagingExpired)
	require.False(t, Conflict(err))
	require.Equal(t, fenced, f.read(t, 100))

	time.Sleep(5 * time.Millisecond)
	_, err = f.publish(ctx, cur, f.next(t, cur, KindFlush), nil)
	require.NoError(t, err, "staging that begins after the horizon commits")
}

// TestRaiseHorizonWaitsOutAPublicationInFlight is the late-commit schedule for
// orphans: a publication passed its compare-and-set and has not committed
// when the collector raises its horizon. The raise cannot complete before the
// publication ends, so the history the collector reads after it holds the
// publication's generation, and the collector never takes its objects for
// orphans. On PostgreSQL the raise waits on the row lock; at REPEATABLE READ
// it then fails with 40001 and is sent again. On a DSQL cluster nothing waits,
// and one of the two fails at COMMIT, which this test cannot stage.
func TestRaiseHorizonWaitsOutAPublicationInFlight(t *testing.T) {
	f := newFixture(t)
	if f.cluster {
		t.Skip("holding off the raise needs a row lock wait, which DSQL does not have")
	}
	ctx := context.Background()
	cur := f.initialized(t, 100)

	tx, err := f.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	inFlight, err := Publish(ctx, SQLTx(tx), f.t, cur, f.next(t, cur, KindFlush))
	require.NoError(t, err)

	raiseCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		for {
			_, err := RaiseHorizon(raiseCtx, DB(f.db), f.t, 100, 0)
			if err == nil || !Conflict(err) {
				done <- err
				return
			}
		}
	}()
	select {
	case err := <-done:
		t.Fatalf("the collector raised its horizon while a publication was in flight: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	require.NoError(t, tx.Commit())
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the raise did not complete after the publication committed")
	}

	retained := mustRetained(t, f, time.Hour)
	require.Equal(t, []int64{1}, numbers(retained))
	require.Equal(t, inFlight.ManifestKey, retained[0].ManifestKey, "the collector's history holds the generation that committed during the raise")
}

func mustRetained(t *testing.T, f *fixture, grace time.Duration) []Generation {
	t.Helper()
	gens, err := Retained(context.Background(), DB(f.db), f.t, 100, grace)
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
