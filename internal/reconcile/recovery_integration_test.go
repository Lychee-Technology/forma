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
)

// TestLostRecordRecoveryFencesAReaderOfAnUnrecordedHead verifies our post-commit
// RetireRecovery fence: a reader pins generation N, then both the record and
// N's history row are lost. InitializeAfterLoss rebuilds the record from N-1
// with recovery_epoch = rec.Epoch and recovery_retired_at = 0.
//   - Diagnosis and GC refuse to delete anything while recovery_retired_at = 0
//     (first GC pass stamps recovery_retired_at = now(), strictly post-commit!).
//   - A second GC pass inside grace G still refuses to delete anything, keeping
//     the pinned reader's manifest and objects intact.
//   - After grace G elapses, GC collects N's orphaned keys and forgets N-1.
func TestLostRecordRecoveryFencesAReaderOfAnUnrecordedHead(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	opts := agedOptions(true)
	g1 := f.publishInit(t, f.initialized(t), 2)
	head := f.publish(t, f.stageFlush(t, g1, 1))
	rep := f.pass(t, opts)
	require.NoError(t, rep.Err)
	require.Equal(t, []int64{g1.rec.Generation}, rep.Collected.Retired)
	f.ageStamps(t, time.Hour)

	reader, err := f.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	require.NoError(t, err)
	defer func() { _ = reader.Rollback() }()
	pinned, err := publication.Read(ctx, publication.SQLTx(reader), f.tables, testSchema)
	require.NoError(t, err)
	pinnedAt := time.Now()
	require.Equal(t, head.rec.Generation, pinned.Generation)

	f.exec(t, "DELETE FROM %s", f.tables.Record)
	f.exec(t, "DELETE FROM %s WHERE generation = 2", f.tables.Generation)
	recoverLostRecord(t, f)
	recovered := f.record(t)
	require.Equal(t, g1.rec.Generation, recovered.Generation)
	require.Greater(t, recovered.Epoch, head.rec.Epoch)

	// Diagnosis refuses collection before RetireRecovery runs.
	diag := f.pass(t, agedOptions(false))
	require.NoError(t, diag.Err)
	require.Equal(t, []FindingKind{AwaitingInit}, kinds(diag.Findings))
	require.Contains(t, diag.Refused, "recovery fence")
	require.Empty(t, diag.Plan.Objects)
	require.Empty(t, diag.Plan.Manifests)

	// First GC pass stamps recovery_retired_at (post-commit) and deletes nothing.
	pass1 := f.pass(t, opts)
	require.NoError(t, pass1.Err)
	require.True(t, pass1.Collected.RecoveryRetired, "first GC pass retires the recovery fence")
	require.Contains(t, pass1.Refused, "recovery fence")
	require.Less(t, time.Since(pinnedAt), opts.ReaderDeadline)
	f.requireOrphansEligible(t, pass1, opts, f.keysOf(head))
	require.Empty(t, pass1.Collected.Objects)
	require.Empty(t, pass1.Collected.Manifests)
	require.Empty(t, pass1.Collected.Forgotten)

	// Second GC pass inside grace G still deletes nothing and does not re-stamp.
	pass2 := f.pass(t, opts)
	require.NoError(t, pass2.Err)
	require.False(t, pass2.Collected.RecoveryRetired, "recovery fence already retired")
	require.Contains(t, pass2.Refused, "recovery fence")
	require.Empty(t, pass2.Collected.Objects)
	require.Empty(t, pass2.Collected.Manifests)

	requirePinnedIntact(t, f, pinned.Record)
	require.NoError(t, reader.Commit())

	// Once grace G elapses after recovery_retired_at, GC resumes cleanly.
	f.ageRecovery(t, time.Hour)
	rep = f.pass(t, opts)
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Refused)
	require.Subset(t, rep.Collected.Objects, f.keysOf(head))
	require.Equal(t, []int64{g1.rec.Generation}, rep.Collected.Forgotten)
	require.Empty(t, f.store.keys())
}

// TestALaterTransitionKeepsTheRecoveryFence verifies that a concurrent Reset
// (drop) or Publish after InitializeAfterLoss never clears or regresses the
// monotonic recovery fence on _publication_epoch, without relying on
// clock_timestamp().
func TestALaterTransitionKeepsTheRecoveryFence(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	opts := agedOptions(true)
	g1 := f.publishInit(t, f.initialized(t), 2)
	head := f.publish(t, f.stageFlush(t, g1, 1))
	require.NoError(t, f.pass(t, opts).Err)
	f.ageStamps(t, time.Hour)

	reader, err := f.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	require.NoError(t, err)
	defer func() { _ = reader.Rollback() }()
	pinned, err := publication.Read(ctx, publication.SQLTx(reader), f.tables, testSchema)
	require.NoError(t, err)

	f.exec(t, "DELETE FROM %s", f.tables.Record)
	f.exec(t, "DELETE FROM %s WHERE generation = 2", f.tables.Generation)
	recoverLostRecord(t, f)

	// Move the record via Reset (drop) immediately after recovery.
	recovered := f.record(t)
	require.NoError(t, f.inTx(t, func(tx publication.Tx) error {
		_, err := publication.Reset(ctx, tx, f.tables, recovered)
		return err
	}))

	// Lifecycle still carries recovery_epoch > 0, recovery_retired_at = 0.
	life, err := publication.ReadLifecycle(ctx, publication.DB(f.db), f.tables, testSchema, opts.RetentionGrace)
	require.NoError(t, err)
	require.True(t, life.RecoveryActive)
	require.Equal(t, recovered.Epoch, life.RecoveryEpoch)
	require.Zero(t, life.RecoveryRetiredAt)

	// GC retires the recovery fence and keeps every object of the pinned head.
	rep := f.pass(t, opts)
	require.NoError(t, rep.Err)
	require.True(t, rep.Collected.RecoveryRetired)
	require.Contains(t, rep.Refused, "recovery fence")
	f.requireOrphansEligible(t, rep, opts, f.keysOf(head))
	require.Empty(t, rep.Collected.Objects)
	require.Empty(t, rep.Collected.Manifests)
	requirePinnedIntact(t, f, pinned.Record)
	require.NoError(t, reader.Commit())
}

// TestARecordWithoutItsLifecycleEpochStopsTheCollection verifies that a missing
// or mismatched lifecycle epoch row blocks GC completely.
func TestARecordWithoutItsLifecycleEpochStopsTheCollection(t *testing.T) {
	f := newFixture(t)
	g1 := f.publishInit(t, f.initialized(t), 1)
	f.publishCompact(t, g1)
	require.NoError(t, f.pass(t, testOptions(true)).Err)
	waitOutGrace()
	f.exec(t, "DELETE FROM %s", f.tables.Epoch)

	rep := f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Equal(t, []FindingKind{MissingLifecycleEpoch}, kinds(rep.Findings))
	require.Equal(t, actionMissingLifecycle, rep.Findings[0].Action)
	require.Contains(t, rep.Refused, string(MissingLifecycleEpoch))
	require.Empty(t, rep.Collected.Objects)
	require.Empty(t, rep.Collected.Manifests)
	require.Subset(t, f.store.keys(), f.keysOf(g1))
}

// TestAnInvalidRecordRecoversAtAFreshEpoch verifies that deleting a corrupt
// record row and running InitializeAfterLoss + cdc-init restores a clean state
// at a fresh epoch while arming the recovery fence and retiring the former head.
func TestAnInvalidRecordRecoversAtAFreshEpoch(t *testing.T) {
	f := newFixture(t)
	g1 := f.publishInit(t, f.initialized(t), 1)
	head := f.publish(t, f.stageFlush(t, g1, 1))
	stale := f.stageFlush(t, head, 1)
	f.exec(t, "UPDATE %s SET generation = -1", f.tables.Record)

	rep := f.pass(t, testOptions(false))
	require.NoError(t, rep.Err)
	require.Equal(t, []FindingKind{RecordInvalid}, kinds(rep.Findings))
	require.Equal(t, actionRecordInvalid, rep.Findings[0].Action)

	f.exec(t, "DELETE FROM %s", f.tables.Record)
	recoverLostRecord(t, f)
	recovered := f.record(t)
	require.Equal(t, publication.AwaitingInit, recovered.State)
	require.Greater(t, recovered.Epoch, head.rec.Epoch)
	require.Equal(t, head.rec.Generation, recovered.Generation)
	_, err := f.commit(t, stale)
	require.Error(t, err, "writer staged over former head cannot commit")

	reinit := f.publishInit(t, recovered, 1)
	require.Equal(t, recovered.Epoch, reinit.rec.Epoch)
	rep = f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Findings)
	require.True(t, rep.Collected.RecoveryRetired)
	require.Contains(t, rep.Refused, "recovery fence")
	require.True(t, rep.Residual())
	var out bytes.Buffer
	Report{Schemas: []SchemaReport{rep}}.Render(&out)
	require.Contains(t, out.String(), "collection refused")
	require.NotContains(t, out.String(), ": clean (")
	require.Equal(t, []int64{g1.rec.Generation, head.rec.Generation}, rep.Collected.Retired)
}

func recoverLostRecord(t *testing.T, f *fixture) {
	t.Helper()
	require.NoError(t, f.inTx(t, func(tx publication.Tx) error {
		h, err := publication.ReadHistory(context.Background(), tx, f.tables, testSchema)
		if err != nil {
			return err
		}
		_, wrote, err := publication.InitializeAfterLoss(context.Background(), tx, f.tables, testSchema, h, publication.AwaitingInit)
		require.True(t, wrote)
		return err
	}))
}

func requirePinnedIntact(t *testing.T, f *fixture, pinned publication.Record) {
	t.Helper()
	ctx := context.Background()
	ref, err := manifestv2.RecordRef(pinned)
	require.NoError(t, err)
	raw, err := f.store.Get(ctx, pinned.ManifestKey)
	require.NoError(t, err)
	m, err := manifestv2.Verify(raw, ref)
	require.NoError(t, err)
	for _, key := range m.Keys() {
		ok, err := f.store.Exists(ctx, key)
		require.NoError(t, err)
		require.True(t, ok, "pinned object %s missing", key)
	}
}
