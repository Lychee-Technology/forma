package reconcile

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// TestAReaderPinnedToAGenerationCompletesWhileTheNextPublishesAndGCRuns: a
// reader pinned to generation N still fetches N's manifest and objects after
// N+1 publishes and GC runs twice; once the retention grace elapses, GC
// deletes N's manifest and objects and forgets all expired rows (advancing
// forgotten_generation cleanly). The passes run under a grace far longer than
// any pass takes and the grace is then elapsed by aging the stamps, so neither
// half depends on how fast the passes run.
func TestAReaderPinnedToAGenerationCompletesWhileTheNextPublishesAndGCRuns(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	g1 := f.publishInit(t, f.initialized(t), 2)
	g2 := f.publish(t, f.stageFlush(t, g1, 1))

	reader, err := f.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	require.NoError(t, err)
	defer func() { _ = reader.Rollback() }()
	pinned, err := publication.Read(ctx, publication.SQLTx(reader), f.tables, testSchema)
	require.NoError(t, err)
	require.Equal(t, g2.rec.Generation, pinned.Generation, "the reader chose generation N")

	g3 := f.publishCompact(t, g2)
	for pass := range 2 {
		rep := f.pass(t, agedOptions(true))
		require.NoError(t, rep.Err)
		require.Empty(t, rep.Findings)
		require.Empty(t, rep.Collected.Objects, "pass %d deletes nothing a retained generation lists", pass)
		require.Empty(t, rep.Collected.Manifests, "pass %d", pass)
		if pass == 0 {
			require.Equal(t, []int64{1, 2}, rep.Collected.Retired, "first pass retires superseded generations")
		}
	}

	ref, err := manifestv2.RecordRef(pinned.Record)
	require.NoError(t, err)
	raw, err := f.store.Get(ctx, pinned.ManifestKey)
	require.NoError(t, err, "pinned manifest still exists")
	m, err := manifestv2.Verify(raw, ref)
	require.NoError(t, err)
	for _, key := range m.Keys() {
		ok, err := f.store.Exists(ctx, key)
		require.NoError(t, err)
		require.True(t, ok, "pinned object %s still exists", key)
	}
	require.NoError(t, reader.Commit())

	f.ageStamps(t, time.Hour)
	rep := f.pass(t, agedOptions(true))
	require.NoError(t, rep.Err)
	require.Equal(t, []int64{1, 2}, rep.Collected.Forgotten, "both expired rows are forgotten cleanly")
	require.Equal(t, f.keysOf(g3), f.store.keys(), "only N+1's manifest and objects remain")

	life, err := publication.ReadLifecycle(ctx, publication.DB(f.db), f.tables, testSchema, agedOptions(false).RetentionGrace)
	require.NoError(t, err)
	require.Equal(t, int64(2), life.ForgottenGeneration, "durable floor proves generations <= 2 were forgotten")

	diag := f.pass(t, agedOptions(false))
	require.Equal(t, [2]int{1, 0}, [2]int{diag.Retained, diag.Expired}, "only current generation remains")
	require.Empty(t, diag.Findings)
}

// TestCollectorWaitsOutAPublicationInFlight raises the horizon while a
// publication holds the record row: the raise waits for the commit, so the
// pass's snapshot sees the new generation and protects its objects.
func TestCollectorWaitsOutAPublicationInFlight(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	g1 := f.publishInit(t, f.initialized(t), 1)
	s := f.stageFlush(t, g1, 1)
	old := f.serverMillis(t) - time.Hour.Milliseconds()
	for _, key := range append(s.manifest.Keys(), s.next.ManifestKey) {
		raw, err := f.store.Get(ctx, key)
		require.NoError(t, err)
		f.store.set(key, raw, old)
	}

	tx, err := f.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = publication.Publish(ctx, publication.SQLTx(tx), f.tables, s.cur, s.next)
	require.NoError(t, err)

	type outcome struct {
		report Report
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		report, err := f.reconciler(testOptions(true)).Run(ctx)
		done <- outcome{report, err}
	}()
	select {
	case <-done:
		t.Fatal("pass finished while publication held the record row")
	case <-time.After(300 * time.Millisecond):
	}
	require.NoError(t, tx.Commit())
	out := <-done
	require.NoError(t, out.err)
	rep := out.report.Schemas[0]
	require.NoError(t, rep.Err)
	require.Equal(t, int64(2), rep.Generation)
	require.Empty(t, rep.Collected.Objects)
	require.Empty(t, rep.Collected.Manifests)
	for _, key := range append(s.manifest.Keys(), s.next.ManifestKey) {
		ok, err := f.store.Exists(ctx, key)
		require.NoError(t, err)
		require.True(t, ok, "%s is protected by committed generation", key)
	}
}

// TestCollectorDeletesOnlyOrphansBelowItsHorizon verifies horizon fencing:
// fenced staging is deleted and can never commit; young staging is kept and
// still commits; unrecognized foreign keys are reported and never deleted.
func TestCollectorDeletesOnlyOrphansBelowItsHorizon(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	g1 := f.publishInit(t, f.initialized(t), 1)
	old := f.serverMillis(t) - time.Hour.Milliseconds()
	v1Delta := "data/7/" + uuid.NewString() + ".parquet"
	f.store.set(v1Delta, []byte("v1"), old)
	f.store.set("data/7/manifest.json", []byte("{}"), old)
	f.store.set("data/70/e1/base/"+uuid.Must(uuid.NewV7()).String()+".parquet", []byte("x"), old)

	fenced := f.stageFlush(t, g1, 1)
	waitOutListingPrecision()
	opts := testOptions(true)
	opts.OrphanAge = 100 * time.Millisecond
	rep := f.pass(t, opts)
	require.NoError(t, rep.Err)
	require.ElementsMatch(t, f.stagedKeys(fenced, g1), rep.Collected.Objects)
	require.Equal(t, []FindingKind{UnrecognizedObject, UnrecognizedObject}, kinds(rep.Findings))
	_, err := f.commit(t, fenced)
	require.ErrorIs(t, err, publication.ErrStagingExpired)

	opts.OrphanAge = time.Hour
	inside := f.stageFlush(t, g1, 1)
	rep = f.pass(t, opts)
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Collected.Objects)
	require.Equal(t, 2, rep.PendingOrphans)
	_, err = f.commit(t, inside)
	require.NoError(t, err)
	for _, key := range []string{v1Delta, "data/7/manifest.json"} {
		ok, err := f.store.Exists(ctx, key)
		require.NoError(t, err)
		require.True(t, ok, "%s is never deleted", key)
	}
}

// TestADroppedSchemasGenerationsExpireAndAreCollected verifies that a dropped
// schema retires its generation, deletes its manifest and objects after grace,
// and forgets 100% of its history rows cleanly using the durable floor.
func TestADroppedSchemasGenerationsExpireAndAreCollected(t *testing.T) {
	f := newFixture(t)
	g1 := f.publishInit(t, f.initialized(t), 2)
	require.NoError(t, f.inTx(t, func(tx publication.Tx) error {
		_, err := publication.Reset(context.Background(), tx, f.tables, g1.rec)
		return err
	}))
	rep := f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Equal(t, publication.Dropped, rep.State)
	require.Equal(t, []int64{1}, rep.Collected.Retired)
	require.Empty(t, rep.Findings)

	waitOutGrace()
	rep = f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Equal(t, []int64{1}, rep.Collected.Forgotten, "all expired history rows are forgotten")
	require.Len(t, rep.Collected.Manifests, 1)
	require.Empty(t, f.store.keys())

	rep = f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Findings, "durable forgotten_generation proves history complete")
	require.Empty(t, rep.Plan.Objects)
}
