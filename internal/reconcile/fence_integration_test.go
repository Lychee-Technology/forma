package reconcile

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// TestAStaleLifecycleRowDoesNotEndTheRecoveryFence is R7-F2: a reader pins
// generation N, then the record and N's history row are lost, and init-db
// recovers the record one epoch above, stamping the lifecycle fence. A
// non-atomic restore then puts back the lifecycle row's old copy: the old
// epoch, with a stamp long past the grace. The surviving history rebuilds
// the counter without a gap, so only the fence stood between N's keys and
// the collection, and the restored stamp would have let it delete them. The
// row's epoch is not the record's, so the diagnosis and the collection both
// refuse, and nothing is deleted or forgotten while the reader is still
// inside its deadline. Written back at the record's epoch with the database
// time, as the action says, the row fences the collection for the grace.
func TestAStaleLifecycleRowDoesNotEndTheRecoveryFence(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	opts := testOptions(true)
	opts.ReaderDeadline, opts.RetentionGrace = 2*time.Second, 2100*time.Millisecond // room for the passes inside the deadline
	g1 := f.publishInit(t, f.initialized(t), 2)
	head := f.publish(t, f.stageFlush(t, g1, 1))
	rep := f.pass(t, opts)
	require.NoError(t, rep.Err)
	require.Equal(t, []int64{g1.rec.Generation}, rep.Collected.Retired)
	// N-1 expires as the history's floor below N, and N's keys fall below
	// the orphan cutoff: only the fence keeps them once N's row is lost.
	time.Sleep(opts.RetentionGrace + publication.LastModifiedPrecision + 300*time.Millisecond)

	var backupEpoch, backupStamp int64 // the lifecycle row as a backup holds it
	require.NoError(t, f.db.QueryRow(fmt.Sprintf("SELECT epoch, updated_at FROM %s WHERE schema_id = $1", f.tables.Epoch),
		testSchema).Scan(&backupEpoch, &backupStamp))

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
	recovered := f.record(t)
	require.Equal(t, backupEpoch+1, recovered.Epoch)
	_, err = f.db.Exec(fmt.Sprintf("UPDATE %s SET epoch = $1, updated_at = $2 WHERE schema_id = $3", f.tables.Epoch),
		backupEpoch, backupStamp, testSchema)
	require.NoError(t, err, "the lifecycle row's backup is restored over the recovery's")
	stale, err := publication.ReadLifecycleFence(ctx, publication.DB(f.db), f.tables, testSchema, opts.RetentionGrace)
	require.NoError(t, err)
	require.False(t, stale.Inside, "the restored stamp alone would let the collection run")

	keys, forgettable := f.store.keys(), f.historyRows(t)
	for _, gc := range []bool{false, true} {
		o := opts
		o.GC = gc
		rep := f.pass(t, o)
		require.NoError(t, rep.Err)
		require.Equal(t, []FindingKind{StaleLifecycleEpoch, AwaitingInit}, kinds(rep.Findings), "gc=%v", gc)
		require.Equal(t, actionStaleLifecycle, rep.Findings[0].Action)
		require.NotEmpty(t, rep.Refused, "gc=%v", gc)
		require.Empty(t, rep.Plan.Objects, "gc=%v", gc)
		require.Empty(t, rep.Plan.Manifests, "gc=%v", gc)
		require.Empty(t, rep.Plan.Forget, "gc=%v", gc)
		if gc {
			require.Empty(t, rep.Collected.Objects)
			require.Empty(t, rep.Collected.Manifests)
			require.Empty(t, rep.Collected.Forgotten)
		}
	}
	require.Equal(t, keys, f.store.keys(), "nothing is deleted")
	require.Equal(t, forgettable, f.historyRows(t), "nothing is forgotten")
	ref, err := manifestv2.RecordRef(pinned.Record)
	require.NoError(t, err)
	raw, err := f.store.Get(ctx, pinned.ManifestKey)
	require.NoError(t, err)
	m, err := manifestv2.Verify(raw, ref)
	require.NoError(t, err)
	for _, key := range m.Keys() {
		ok, err := f.store.Exists(ctx, key)
		require.NoError(t, err)
		require.True(t, ok, "the pinned generation's object %s is still there", key)
	}
	require.Less(t, time.Since(pinnedAt), opts.ReaderDeadline, "the reader is still inside its deadline")
	require.NoError(t, reader.Commit())

	// The documented action: the row back at the record's epoch, with the
	// database time. The fence holds for the grace after it.
	_, err = f.db.Exec(fmt.Sprintf("UPDATE %s SET epoch = $1, updated_at = %s WHERE schema_id = $2",
		f.tables.Epoch, sqlgen.ServerTimeMillisSQL), recovered.Epoch, testSchema)
	require.NoError(t, err)
	rep = f.pass(t, opts)
	require.NoError(t, rep.Err)
	require.Equal(t, []FindingKind{AwaitingInit}, kinds(rep.Findings))
	require.Contains(t, rep.Refused, "lifecycle epoch moved")
	require.Equal(t, keys, f.store.keys())
}

// TestALifecycleRowAboveTheRecordStopsTheCollection: a lifecycle row whose
// epoch is above the record's says the record row went back to an older
// copy. The pass reports it with the invalid record's recovery and deletes
// nothing.
func TestALifecycleRowAboveTheRecordStopsTheCollection(t *testing.T) {
	f := newFixture(t)
	g1 := f.publishInit(t, f.initialized(t), 1)
	f.publishCompact(t, g1)
	require.NoError(t, f.pass(t, testOptions(true)).Err) // retires g1
	waitOutListingPrecision()
	f.exec(t, "UPDATE %s SET epoch = epoch + 1", f.tables.Epoch)
	keys := f.store.keys()

	rep := f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Equal(t, []FindingKind{StaleLifecycleEpoch}, kinds(rep.Findings))
	require.Equal(t, actionRecordBehindLifecycle, rep.Findings[0].Action)
	require.NotEmpty(t, rep.Refused)
	require.Empty(t, rep.Collected.Objects)
	require.Empty(t, rep.Collected.Manifests)
	require.Empty(t, rep.Collected.Forgotten)
	require.Equal(t, keys, f.store.keys())
}

// historyRows answers the schema's generation numbers the history holds.
func (f *fixture) historyRows(t *testing.T) []int64 {
	t.Helper()
	rows, err := f.db.Query(fmt.Sprintf("SELECT generation FROM %s WHERE schema_id = $1 ORDER BY generation", f.tables.Generation), testSchema)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var out []int64
	for rows.Next() {
		var g int64
		require.NoError(t, rows.Scan(&g))
		out = append(out, g)
	}
	require.NoError(t, rows.Err())
	return out
}
