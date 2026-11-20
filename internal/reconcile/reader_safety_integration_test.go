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

// TestAReaderPinnedToAGenerationCompletesWhileTheNextPublishesAndGCRuns is the
// reader-safety acceptance: a reader that chose generation N in its snapshot
// still fetches N's manifest and every object it lists after N+1 published
// and garbage collection ran twice, and only a pass after the retention grace
// deletes what N alone listed.
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

	g3 := f.publishCompact(t, g2) // N+1 replaces every file N lists
	for pass := range 2 {
		rep := f.pass(t, testOptions(true))
		require.NoError(t, rep.Err)
		require.Empty(t, rep.Findings)
		require.Empty(t, rep.Collected.Objects, "pass %d deletes nothing a retained generation lists", pass)
		require.Empty(t, rep.Collected.Manifests, "pass %d", pass)
		if pass == 0 {
			require.Equal(t, []int64{1, 2}, rep.Collected.Retired, "the first pass retires the superseded generations")
		}
	}

	ref, err := manifestv2.RecordRef(pinned.Record)
	require.NoError(t, err)
	raw, err := f.store.Get(ctx, pinned.ManifestKey)
	require.NoError(t, err, "the pinned generation's manifest is still there")
	m, err := manifestv2.Verify(raw, ref)
	require.NoError(t, err)
	for _, key := range m.Keys() {
		ok, err := f.store.Exists(ctx, key)
		require.NoError(t, err)
		require.True(t, ok, "the pinned generation's object %s is still there", key)
	}
	require.NoError(t, reader.Commit())

	waitOutGrace()
	rep := f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Equal(t, []int64{1}, rep.Collected.Forgotten, "N's row stays as the history's floor")
	require.Equal(t, f.keysOf(g3), f.store.keys(), "only N+1's manifest and objects remain")
	diag := f.pass(t, testOptions(false))
	require.Equal(t, [2]int{1, 1}, [2]int{diag.Retained, diag.Expired}, "the history holds N+1 and N's row as its floor")
	require.Empty(t, diag.Findings)
}

// TestCollectorWaitsOutAPublicationInFlight raises the horizon while a
// publication holds the record row: the raise waits for the commit, so the
// pass's snapshot sees the new generation, and its objects stay although
// they are older than the horizon.
func TestCollectorWaitsOutAPublicationInFlight(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	g1 := f.publishInit(t, f.initialized(t), 1)
	s := f.stageFlush(t, g1, 1)
	old := f.serverMillis(t) - time.Hour.Milliseconds()
	for _, key := range append(s.manifest.Keys(), s.next.ManifestKey) {
		raw, err := f.store.Get(ctx, key)
		require.NoError(t, err)
		f.store.set(key, raw, old) // only being named protects them
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
		t.Fatal("the pass finished while the publication held the record row")
	case <-time.After(300 * time.Millisecond):
	}
	require.NoError(t, tx.Commit())
	out := <-done
	require.NoError(t, out.err)
	rep := out.report.Schemas[0]
	require.NoError(t, rep.Err)
	require.Equal(t, int64(2), rep.Generation, "the pass's snapshot saw the publication committed")
	require.Empty(t, rep.Collected.Objects)
	require.Empty(t, rep.Collected.Manifests)
	for _, key := range append(s.manifest.Keys(), s.next.ManifestKey) {
		ok, err := f.store.Exists(ctx, key)
		require.NoError(t, err)
		require.True(t, ok, "%s is named by the committed generation", key)
	}
}

// TestCollectorDeletesOnlyOrphansBelowItsHorizon: the staging of a
// publication the horizon fenced is deleted, and that publication can never
// commit; staging newer than the horizon less the clock margin is kept, and
// its publication still commits; a key no V2 writer stages is reported and
// never deleted, however old.
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
	waitOutListingPrecision() // past the orphan age, the clock margin and the listing's precision
	opts := testOptions(true)
	opts.OrphanAge = 100 * time.Millisecond
	rep := f.pass(t, opts)
	require.NoError(t, rep.Err)
	require.ElementsMatch(t, f.stagedKeys(fenced, g1), rep.Collected.Objects, "the fenced staging is deleted, nothing else")
	require.Equal(t, []FindingKind{UnrecognizedObject, UnrecognizedObject}, kinds(rep.Findings))
	_, err := f.commit(t, fenced)
	require.ErrorIs(t, err, publication.ErrStagingExpired, "a publication whose staging the pass deleted never commits")

	opts.OrphanAge = time.Hour
	inside := f.stageFlush(t, g1, 1)
	rep = f.pass(t, opts)
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Collected.Objects, "staging above the horizon is kept")
	require.Equal(t, 2, rep.PendingOrphans, "its delta and its manifest wait for the horizon")
	_, err = f.commit(t, inside)
	require.NoError(t, err, "a publication inside the horizon still commits")
	for _, key := range []string{v1Delta, "data/7/manifest.json"} {
		ok, err := f.store.Exists(ctx, key)
		require.NoError(t, err)
		require.True(t, ok, "%s is never deleted", key)
	}
}

// TestADroppedSchemasGenerationsExpireAndAreCollected: a drop supersedes the
// current generation, a pass retires it, and once its grace has passed a pass
// deletes its manifest and objects and forgets it.
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
	require.Empty(t, rep.Findings, "a dropped schema needs no operator")

	waitOutGrace()
	rep = f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Collected.Forgotten, "the only expired row stays as the history's floor")
	require.Len(t, rep.Collected.Manifests, 1)
	require.Empty(t, f.store.keys())
	rep = f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Findings, "the floor proves the history complete")
	require.Empty(t, rep.Plan.Objects)
}
