package reconcile

import (
	"bytes"
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// TestMissingCurrentHistoryRecoveryTakesTwoPublications runs the documented
// recovery of a lost current history row without a backup: the rendered
// action says one publication and a grace are not enough, and they are not.
// A re-init supersedes the unrecorded generation, which becomes a history
// gap; a grace after the re-init the collection is still refused. Only once
// a later publication supersedes the re-init's generation, a pass retires
// it and it expires does the collection resume, and then it collects the
// unrecorded generation's manifest and the objects only it listed.
func TestMissingCurrentHistoryRecoveryTakesTwoPublications(t *testing.T) {
	f := newFixture(t)
	g1 := f.publishInit(t, f.initialized(t), 1)
	f.exec(t, "DELETE FROM %s WHERE generation = 1", f.tables.Generation)

	rep := f.pass(t, testOptions(false))
	require.NoError(t, rep.Err)
	require.Equal(t, []FindingKind{MissingGeneration}, kinds(rep.Findings))
	require.Equal(t, actionMissingHistory, rep.Findings[0].Action)
	var out bytes.Buffer
	Report{Schemas: []SchemaReport{rep}}.Render(&out)
	for _, step := range []string{
		"do not restore the generation history row from a backup",
		"carries superseded_at 0",
		"takes two publications",
		"then reported as missing_history",
		"a later one must supersede that publication's generation in turn, a pass retire it, and the retention grace pass",
		"Waiting the retention grace after a single publication or re-init is not enough",
	} {
		require.Contains(t, out.String(), step)
	}

	reinit := f.publishInit(t, g1.rec, 1)
	refused := func(retired []int64) {
		t.Helper()
		rep := f.pass(t, testOptions(true))
		require.NoError(t, rep.Err)
		require.Equal(t, []FindingKind{HistoryGap}, kinds(rep.Findings))
		require.Equal(t, g1.rec.Generation, rep.Findings[0].Generation, "the unrecorded generation is the gap")
		require.Equal(t, actionHistoryGap, rep.Findings[0].Action)
		require.NotEmpty(t, rep.Refused)
		require.Equal(t, retired, rep.Collected.Retired)
		require.Empty(t, rep.Collected.Objects)
		require.Empty(t, rep.Collected.Manifests)
		require.Subset(t, f.store.keys(), f.keysOf(g1), "the unrecorded generation is kept")
	}
	refused(nil)
	waitOutGrace()
	refused(nil) // a grace after the re-init: still refused

	f.publish(t, f.stageFlush(t, reinit, 1))
	refused([]int64{reinit.rec.Generation}) // the re-init's generation is retired, not expired
	waitOutListingPrecision()               // past the grace, and the unrecorded generation's keys below the orphan cutoff

	rep = f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Findings, "the re-init's generation expired above the gap")
	require.Empty(t, rep.Refused)
	require.Subset(t, rep.Collected.Objects, f.keysOf(g1), "the unrecorded generation's manifest and objects are collected")
	require.Equal(t, []string{reinit.rec.ManifestKey}, rep.Collected.Manifests)
	require.Empty(t, rep.Collected.Forgotten, "the expired re-init row stays as the history's floor")
}

// TestLostRecordRecoveryFencesAReaderOfAnUnrecordedHead is R4-F1: a reader
// pins generation N, then both the record and N's history row are lost. The
// documented recovery (init-db's ReadHistory and InitializeAfterLoss)
// rebuilds the counter from the surviving N-1, so the history shows no gap,
// and nothing names N's manifest or the objects it lists. A collection right
// after the recovery, inside the reader's safety window, must delete none of
// them: the recovery's lifecycle fence refuses it, in the diagnosis's plan
// too. Once the reader is done and the retention grace after the recovery
// has passed, the collection resumes and collects them.
func TestLostRecordRecoveryFencesAReaderOfAnUnrecordedHead(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	g1 := f.publishInit(t, f.initialized(t), 2)
	head := f.publish(t, f.stageFlush(t, g1, 1))
	rep := f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Equal(t, []int64{g1.rec.Generation}, rep.Collected.Retired)
	// N-1 expires as the history's floor below N, and N's keys fall below
	// the orphan cutoff: only the fence keeps them once N's row is lost.
	waitOutListingPrecision()

	reader, err := f.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	require.NoError(t, err)
	defer func() { _ = reader.Rollback() }()
	pinned, err := publication.Read(ctx, publication.SQLTx(reader), f.tables, testSchema)
	require.NoError(t, err)
	require.Equal(t, head.rec.Generation, pinned.Generation, "the reader pinned generation N")

	f.exec(t, "DELETE FROM %s", f.tables.Record)
	f.exec(t, "DELETE FROM %s WHERE generation = 2", f.tables.Generation)
	require.NoError(t, f.inTx(t, func(tx publication.Tx) error {
		h, err := publication.ReadHistory(ctx, tx, f.tables, testSchema)
		if err != nil {
			return err
		}
		_, wrote, err := publication.InitializeAfterLoss(ctx, tx, f.tables, testSchema, h, publication.AwaitingInit)
		require.True(t, wrote)
		return err
	}))
	recovered := f.record(t)
	require.Equal(t, g1.rec.Generation, recovered.Generation, "the counter is rebuilt from the surviving N-1")

	for _, gc := range []bool{false, true} {
		rep := f.pass(t, testOptions(gc))
		require.NoError(t, rep.Err)
		require.Equal(t, []FindingKind{AwaitingInit}, kinds(rep.Findings), "the history shows no gap above the rebuilt counter")
		require.Contains(t, rep.Refused, "lifecycle epoch", "gc=%v", gc)
		require.Empty(t, rep.Plan.Objects, "gc=%v", gc)
		require.Empty(t, rep.Plan.Manifests, "gc=%v", gc)
		if gc {
			require.Empty(t, rep.Collected.Objects)
			require.Empty(t, rep.Collected.Manifests)
			require.Empty(t, rep.Collected.Forgotten)
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

	waitOutListingPrecision() // past the fence's grace, and the unrecorded head's keys below the orphan cutoff
	rep = f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Refused)
	require.Subset(t, rep.Collected.Objects, f.keysOf(head), "the unrecorded head's manifest and objects are collected once the fence passed")
	require.Empty(t, f.store.keys(), "nothing names any object the schema staged before the loss")
}

// TestRecoveryStampsItsFenceAfterTheLoss: the recovery's transaction may
// have begun before the loss committed, and a reader may have read the lost
// record in between. The lifecycle fence is stamped after the statement that
// found the record missing, so it is later than the loss, and later than the
// transaction's own start, which the other transitions stamp.
func TestRecoveryStampsItsFenceAfterTheLoss(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.publishInit(t, f.initialized(t), 1)
	tx, err := f.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var began int64
	require.NoError(t, tx.QueryRowContext(ctx, "SELECT "+sqlgen.ServerTimeMillisSQL).Scan(&began))
	time.Sleep(50 * time.Millisecond)
	f.exec(t, "DELETE FROM %s", f.tables.Record)
	lost := f.serverMillis(t)
	require.Greater(t, lost, began)
	q := publication.SQLTx(tx)
	h, err := publication.ReadHistory(ctx, q, f.tables, testSchema)
	require.NoError(t, err)
	_, wrote, err := publication.InitializeAfterLoss(ctx, q, f.tables, testSchema, h, publication.AwaitingInit)
	require.NoError(t, err)
	require.True(t, wrote)
	require.NoError(t, tx.Commit())

	fence, err := publication.ReadLifecycleFence(ctx, publication.DB(f.db), f.tables, testSchema, time.Hour)
	require.NoError(t, err)
	require.True(t, fence.Present)
	require.True(t, fence.Inside)
	require.GreaterOrEqual(t, fence.MovedAt, lost, "the fence is stamped after the loss, not at the transaction's start %d", began)
}

// TestARecordWithoutItsLifecycleEpochStopsTheCollection: a record whose
// lifecycle epoch row is gone gives no proof of when it was last recovered,
// so the pass reports it with its action and deletes nothing.
func TestARecordWithoutItsLifecycleEpochStopsTheCollection(t *testing.T) {
	f := newFixture(t)
	g1 := f.publishInit(t, f.initialized(t), 1)
	f.publishCompact(t, g1)
	require.NoError(t, f.pass(t, testOptions(true)).Err) // retires g1
	waitOutGrace()
	f.exec(t, "DELETE FROM %s", f.tables.Epoch)

	rep := f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Equal(t, []FindingKind{MissingLifecycleEpoch}, kinds(rep.Findings))
	require.Equal(t, actionMissingLifecycle, rep.Findings[0].Action)
	require.Contains(t, rep.Refused, string(MissingLifecycleEpoch))
	require.Empty(t, rep.Collected.Objects)
	require.Empty(t, rep.Collected.Manifests)
	require.Subset(t, f.store.keys(), f.keysOf(g1), "the expired generation is kept")
}

// TestALaterTransitionKeepsTheRecoveryFence is R5-F1: a transition whose
// transaction began before the loss, more than a grace before the recovery,
// reads the recovered record under READ COMMITTED and moves it on (a drop).
// Its stamp is its transaction's start, older than the recovery's: the
// lifecycle fence must not go back to it, or a collection inside the pinned
// reader's window would take the unrecorded head's manifest and objects.
// The drop's transaction is aged first (R6-F4), and the head's keys with it
// past the orphan cutoff; only then does the reader pin N, and the loss, the
// recovery, the drop and the collection all run while the reader is still
// inside its deadline, which the protocol promises to protect.
func TestALaterTransitionKeepsTheRecoveryFence(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	opts := testOptions(true)
	g1 := f.publishInit(t, f.initialized(t), 2)
	head := f.publish(t, f.stageFlush(t, g1, 1))
	rep := f.pass(t, opts)
	require.NoError(t, rep.Err)
	require.Equal(t, []int64{g1.rec.Generation}, rep.Collected.Retired)

	drop, err := f.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	require.NoError(t, err)
	defer func() { _ = drop.Rollback() }()
	var began int64
	require.NoError(t, drop.QueryRowContext(ctx, "SELECT "+sqlgen.ServerTimeMillisSQL).Scan(&began))
	// The drop's transaction ages past the grace before the recovery, N-1
	// expires as the history's floor below N, and N's keys fall below the
	// orphan cutoff: only the fence keeps them once N's row is lost.
	waitOutListingPrecision()

	reader, err := f.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	require.NoError(t, err)
	defer func() { _ = reader.Rollback() }()
	pinned, err := publication.Read(ctx, publication.SQLTx(reader), f.tables, testSchema)
	require.NoError(t, err)
	pinnedAt := time.Now()
	require.Equal(t, head.rec.Generation, pinned.Generation, "the reader pinned generation N")

	f.exec(t, "DELETE FROM %s", f.tables.Record)
	f.exec(t, "DELETE FROM %s WHERE generation = 2", f.tables.Generation)
	require.NoError(t, f.inTx(t, func(tx publication.Tx) error {
		h, err := publication.ReadHistory(ctx, tx, f.tables, testSchema)
		if err != nil {
			return err
		}
		_, wrote, err := publication.InitializeAfterLoss(ctx, tx, f.tables, testSchema, h, publication.AwaitingInit)
		require.True(t, wrote)
		return err
	}))
	stamped, err := publication.ReadLifecycleFence(ctx, publication.DB(f.db), f.tables, testSchema, time.Hour)
	require.NoError(t, err)
	require.Greater(t, stamped.MovedAt, began+opts.RetentionGrace.Milliseconds())

	q := publication.SQLTx(drop)
	recovered, err := publication.Read(ctx, q, f.tables, testSchema)
	require.NoError(t, err, "READ COMMITTED reads the record recovered after the transaction began")
	_, err = publication.Reset(ctx, q, f.tables, recovered.Record)
	require.NoError(t, err)
	require.NoError(t, drop.Commit())

	fence, err := publication.ReadLifecycleFence(ctx, publication.DB(f.db), f.tables, testSchema, time.Hour)
	require.NoError(t, err)
	require.Equal(t, stamped.MovedAt, fence.MovedAt, "the drop keeps the recovery's later stamp, not its start %d", began)

	rep = f.pass(t, opts)
	require.NoError(t, rep.Err)
	require.Contains(t, rep.Refused, "lifecycle epoch")
	require.Empty(t, rep.Collected.Objects)
	require.Empty(t, rep.Collected.Manifests)
	require.Empty(t, rep.Collected.Forgotten)
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
	require.Less(t, time.Since(pinnedAt), opts.ReaderDeadline,
		"the loss, the recovery, the drop and the collection ran while the reader was inside its deadline")
	require.NoError(t, reader.Commit())
}

// TestAnInvalidRecordRecoversAtAFreshEpoch is R6-F3: the action for an
// invalid record never installs an older pointer. Following it, the row is
// deleted and init-db's recovery writes the record one epoch above the
// lifecycle epoch, AWAITING_INIT, so no stale pointer is served: a writer
// that staged over the former head misses, and the re-init publishes at the
// fresh epoch while collection waits out the recovery's grace.
func TestAnInvalidRecordRecoversAtAFreshEpoch(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	g1 := f.publishInit(t, f.initialized(t), 1)
	head := f.publish(t, f.stageFlush(t, g1, 1))
	stale := f.stageFlush(t, head, 1)
	f.exec(t, "UPDATE %s SET generation = -1", f.tables.Record)

	rep := f.pass(t, testOptions(false))
	require.NoError(t, rep.Err)
	require.Equal(t, []FindingKind{RecordInvalid}, kinds(rep.Findings))
	require.Equal(t, actionRecordInvalid, rep.Findings[0].Action)
	require.Contains(t, rep.Findings[0].Action, "never from a backup copy of the row")

	f.exec(t, "DELETE FROM %s", f.tables.Record)
	require.NoError(t, f.inTx(t, func(tx publication.Tx) error {
		h, err := publication.ReadHistory(ctx, tx, f.tables, testSchema)
		if err != nil {
			return err
		}
		_, wrote, err := publication.InitializeAfterLoss(ctx, tx, f.tables, testSchema, h, publication.AwaitingInit)
		require.True(t, wrote)
		return err
	}))
	recovered := f.record(t)
	require.Equal(t, publication.AwaitingInit, recovered.State, "readers fail closed until the re-init publishes")
	require.Greater(t, recovered.Epoch, head.rec.Epoch, "the recovery takes a fresh epoch")
	require.Equal(t, head.rec.Generation, recovered.Generation, "no generation number is used again")
	_, err := f.commit(t, stale)
	require.Error(t, err, "a writer that staged over the former head never publishes")

	reinit := f.publishInit(t, recovered, 1)
	require.Equal(t, recovered.Epoch, reinit.rec.Epoch)
	rep = f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Findings)
	require.Contains(t, rep.Refused, "lifecycle epoch", "collection waits out the recovery's grace")
	require.Equal(t, []int64{g1.rec.Generation, head.rec.Generation}, rep.Collected.Retired)
}
