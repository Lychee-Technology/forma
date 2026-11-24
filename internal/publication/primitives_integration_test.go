package publication

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// TestSupersedeFailsClosedOnMissingOrDisagreeingHistory verifies that both
// Publish and Reset roll back with ErrHistoryInconsistent (not Conflict) when
// the current generation's history row is missing, carries an early stamp, or
// disagrees with the publication record on any identity field.
func TestSupersedeFailsClosedOnMissingOrDisagreeingHistory(t *testing.T) {
	mutations := map[string]func(t *testing.T, f *fixture, cur Record){
		"missing row": func(t *testing.T, f *fixture, cur Record) {
			_, err := f.db.Exec(fmt.Sprintf("DELETE FROM %s WHERE schema_id = $1 AND generation = $2", f.t.Generation), cur.SchemaID, cur.Generation)
			require.NoError(t, err)
		},
		"early superseded_at": func(t *testing.T, f *fixture, cur Record) {
			_, err := f.db.Exec(fmt.Sprintf("UPDATE %s SET superseded_at = 1 WHERE schema_id = $1 AND generation = $2", f.t.Generation), cur.SchemaID, cur.Generation)
			require.NoError(t, err)
		},
		"early retired_at": func(t *testing.T, f *fixture, cur Record) {
			_, err := f.db.Exec(fmt.Sprintf("UPDATE %s SET retired_at = 1 WHERE schema_id = $1 AND generation = $2", f.t.Generation), cur.SchemaID, cur.Generation)
			require.NoError(t, err)
		},
		"different epoch": func(t *testing.T, f *fixture, cur Record) {
			_, err := f.db.Exec(fmt.Sprintf("UPDATE %s SET epoch = $3 WHERE schema_id = $1 AND generation = $2", f.t.Generation), cur.SchemaID, cur.Generation, cur.Epoch+1)
			require.NoError(t, err)
		},
		"different parent_generation": func(t *testing.T, f *fixture, cur Record) {
			_, err := f.db.Exec(fmt.Sprintf("UPDATE %s SET parent_generation = $3 WHERE schema_id = $1 AND generation = $2", f.t.Generation), cur.SchemaID, cur.Generation, cur.ParentGeneration+1)
			require.NoError(t, err)
		},
		"different manifest_key": func(t *testing.T, f *fixture, cur Record) {
			_, err := f.db.Exec(fmt.Sprintf("UPDATE %s SET manifest_key = 'data/100/_manifests/e1/other.json' WHERE schema_id = $1 AND generation = $2", f.t.Generation), cur.SchemaID, cur.Generation)
			require.NoError(t, err)
		},
		"different manifest_sha256": func(t *testing.T, f *fixture, cur Record) {
			_, err := f.db.Exec(fmt.Sprintf("UPDATE %s SET manifest_sha256 = $3 WHERE schema_id = $1 AND generation = $2", f.t.Generation), cur.SchemaID, cur.Generation, strings.Repeat("f", 64))
			require.NoError(t, err)
		},
		"different lineage": func(t *testing.T, f *fixture, cur Record) {
			_, err := f.db.Exec(fmt.Sprintf("UPDATE %s SET lineage = $3 WHERE schema_id = $1 AND generation = $2", f.t.Generation), cur.SchemaID, cur.Generation, uuid.New())
			require.NoError(t, err)
		},
		"different cas_token": func(t *testing.T, f *fixture, cur Record) {
			_, err := f.db.Exec(fmt.Sprintf("UPDATE %s SET cas_token = $3 WHERE schema_id = $1 AND generation = $2", f.t.Generation), cur.SchemaID, cur.Generation, uuid.New())
			require.NoError(t, err)
		},
	}

	for name, mutate := range mutations {
		t.Run("publish/"+name, func(t *testing.T) {
			f := newFixture(t)
			ctx := context.Background()
			g1, err := f.publish(ctx, f.initialized(t, 100), f.next(t, f.read(t, 100), KindFlush), nil)
			require.NoError(t, err)
			mutate(t, f, g1)

			_, err = f.publish(ctx, g1, f.next(t, g1, KindFlush), nil)
			require.ErrorIs(t, err, ErrHistoryInconsistent)
			require.False(t, Conflict(err), "history inconsistency must not trigger CAS retry")
			require.Equal(t, g1, f.read(t, 100), "failed Publish must leave the record rolled back")
		})
		t.Run("reset/"+name, func(t *testing.T) {
			f := newFixture(t)
			ctx := context.Background()
			g1, err := f.publish(ctx, f.initialized(t, 100), f.next(t, f.read(t, 100), KindFlush), nil)
			require.NoError(t, err)
			mutate(t, f, g1)

			err = f.inTx(ctx, func(tx Tx) error {
				_, err := Reset(ctx, tx, f.t, g1)
				return err
			})
			require.ErrorIs(t, err, ErrHistoryInconsistent)
			require.False(t, Conflict(err))
			require.Equal(t, g1, f.read(t, 100), "failed Reset must leave the record rolled back")
		})
	}
}

// TestForgetFailsClosedWithoutLifecycleRow verifies that Forget rolls back the
// history row deletion when the lifecycle epoch row is missing, so the durable
// forgotten_generation floor can never lag behind deleted rows.
func TestForgetFailsClosedWithoutLifecycleRow(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	g1, err := f.publish(ctx, f.initialized(t, 100), f.next(t, f.read(t, 100), KindFlush), nil)
	require.NoError(t, err)
	_, err = f.publish(ctx, g1, f.next(t, g1, KindFlush), nil)
	require.NoError(t, err)
	ok, err := Retire(ctx, DB(f.db), f.t, 100, 1)
	require.NoError(t, err)
	require.True(t, ok)

	_, err = f.db.Exec(fmt.Sprintf("DELETE FROM %s WHERE schema_id = $1", f.t.Epoch), 100)
	require.NoError(t, err)

	err = f.inTx(ctx, func(tx Tx) error {
		_, err := Forget(ctx, tx, f.t, 100, 1, 0)
		return err
	})
	require.ErrorIs(t, err, ErrLifecycleLost)
	expired, err := Expired(ctx, DB(f.db), f.t, 100, 0)
	require.NoError(t, err)
	require.Equal(t, []int64{1}, numbers(expired), "rolled-back Forget must keep the history row")
}

// TestInitializeAfterLossArmsMonotonicRecoveryFenceAndSupersedesFormerHead pins
// the full lost-record recovery lifecycle: former head superseded (so it does
// not leak), recovery_epoch armed with recovery_retired_at = 0, preserved
// across subsequent Reset/Activate/Publish, retired strictly post-commit via
// RetireRecovery, and re-armed cleanly on a second record loss.
func TestInitializeAfterLossArmsMonotonicRecoveryFenceAndSupersedesFormerHead(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	cur := f.initialized(t, 100)
	cur, err := f.publish(ctx, cur, f.next(t, cur, KindFlush), nil)
	require.NoError(t, err)
	_, err = f.publish(ctx, cur, f.next(t, cur, KindFlush), nil)
	require.NoError(t, err)
	f.forget(t, 1) // forgotten_generation is now 1

	// Lose the publication record row while generation 2 is still unsuperseded.
	_, err = f.db.Exec(fmt.Sprintf("DELETE FROM %s WHERE schema_id = $1", f.t.Record), 100)
	require.NoError(t, err)

	h, err := ReadHistory(ctx, DB(f.db), f.t, 100)
	require.NoError(t, err)
	require.Equal(t, int64(1), h.ForgottenGeneration)
	var rec Record
	require.NoError(t, f.inTx(ctx, func(tx Tx) error {
		var wrote bool
		rec, wrote, err = InitializeAfterLoss(ctx, tx, f.t, 100, h, AwaitingInit)
		require.True(t, wrote)
		return err
	}))
	require.Equal(t, int64(2), rec.Epoch)
	require.Equal(t, int64(2), rec.Generation)

	// Former head (gen 2) is superseded so GC can retire it instead of leaking it forever.
	retained := mustRetained(t, f, time.Hour)
	require.Equal(t, []int64{2}, numbers(retained))
	require.NotZero(t, retained[0].SupersededAt)
	require.Zero(t, retained[0].RetiredAt)

	// Recovery fence is active, recovery_retired_at is 0, and forgotten_generation stayed 1.
	lc, err := ReadLifecycle(ctx, DB(f.db), f.t, 100, time.Hour)
	require.NoError(t, err)
	require.True(t, lc.Present)
	require.Equal(t, int64(2), lc.Epoch)
	require.Equal(t, int64(1), lc.ForgottenGeneration)
	require.Equal(t, int64(2), lc.RecoveryEpoch)
	require.Zero(t, lc.RecoveryRetiredAt)
	require.True(t, lc.RecoveryActive)

	// A subsequent Reset + Activate does not weaken the recovery fence.
	dropped := f.reset(t, f.read(t, 100))
	_ = f.activate(t, dropped)
	lc, err = ReadLifecycle(ctx, DB(f.db), f.t, 100, time.Hour)
	require.NoError(t, err)
	require.Equal(t, int64(4), lc.Epoch)
	require.Equal(t, int64(2), lc.RecoveryEpoch)
	require.Zero(t, lc.RecoveryRetiredAt)
	require.True(t, lc.RecoveryActive)

	// Collector stamps RetireRecovery post-commit (once).
	ok, err := RetireRecovery(ctx, DB(f.db), f.t, 100, 2)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = RetireRecovery(ctx, DB(f.db), f.t, 100, 2)
	require.NoError(t, err)
	require.False(t, ok, "RetireRecovery stamps only once")

	// Inside grace it stays active; under zero grace it has elapsed.
	lc, err = ReadLifecycle(ctx, DB(f.db), f.t, 100, time.Hour)
	require.NoError(t, err)
	require.NotZero(t, lc.RecoveryRetiredAt)
	require.True(t, lc.RecoveryActive)
	lcNow, err := ReadLifecycle(ctx, DB(f.db), f.t, 100, 0)
	require.NoError(t, err)
	require.False(t, lcNow.RecoveryActive)

	// A second record loss raises recovery_epoch and resets recovery_retired_at to 0;
	// a stale RetireRecovery for the old epoch matches nothing.
	_, err = f.db.Exec(fmt.Sprintf("DELETE FROM %s WHERE schema_id = $1", f.t.Record), 100)
	require.NoError(t, err)
	h2, err := ReadHistory(ctx, DB(f.db), f.t, 100)
	require.NoError(t, err)
	require.NoError(t, f.inTx(ctx, func(tx Tx) error {
		_, wrote, err := InitializeAfterLoss(ctx, tx, f.t, 100, h2, AwaitingInit)
		require.True(t, wrote)
		return err
	}))
	ok, err = RetireRecovery(ctx, DB(f.db), f.t, 100, 2)
	require.NoError(t, err)
	require.False(t, ok, "stale recoveryEpoch must not retire a newer recovery")
	lc, err = ReadLifecycle(ctx, DB(f.db), f.t, 100, 0)
	require.NoError(t, err)
	require.Equal(t, int64(5), lc.RecoveryEpoch)
	require.Zero(t, lc.RecoveryRetiredAt)
	require.True(t, lc.RecoveryActive)
}

// TestGraceMillisExactSQLBoundary pins the minimum positive grace boundary
// (60s + 1ns => GraceMillis = 60002 ms) inside one fixed-now() transaction for
// generation retention, Forget, and ReadLifecycle.
func TestGraceMillisExactSQLBoundary(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	cur := f.initialized(t, 100)
	for range 4 {
		var err error
		cur, err = f.publish(ctx, cur, f.next(t, cur, KindFlush), nil)
		require.NoError(t, err)
	}
	grace := 60*time.Second + time.Nanosecond // GraceMillis == 60002

	tx, err := f.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var nowMs int64
	require.NoError(t, tx.QueryRowContext(ctx, "SELECT "+sqlgen.ServerTimeMillisSQL).Scan(&nowMs))

	// Stamp generations 1, 2, 3 at now-60002 (expired), now-60001 (retained), now-60000 (retained).
	for gen, offset := range map[int64]int64{1: 60_002, 2: 60_001, 3: 60_000} {
		_, err := tx.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET retired_at = $3 WHERE schema_id = $1 AND generation = $2", f.t.Generation),
			100, gen, nowMs-offset)
		require.NoError(t, err)
	}
	retained, err := Retained(ctx, SQLTx(tx), f.t, 100, grace)
	require.NoError(t, err)
	require.Equal(t, []int64{2, 3, 4}, numbers(retained))
	expired, err := Expired(ctx, SQLTx(tx), f.t, 100, grace)
	require.NoError(t, err)
	require.Equal(t, []int64{1}, numbers(expired))

	// Lifecycle recovery boundary matches the same 60001 (active) vs 60002 (inactive).
	_, err = tx.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET recovery_epoch = 1, recovery_retired_at = $2 WHERE schema_id = $1", f.t.Epoch),
		100, nowMs-60_001)
	require.NoError(t, err)
	lc, err := ReadLifecycle(ctx, SQLTx(tx), f.t, 100, grace)
	require.NoError(t, err)
	require.True(t, lc.RecoveryActive, "60001 ms back is still inside 60s+1ns grace")

	_, err = tx.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET recovery_retired_at = $2 WHERE schema_id = $1", f.t.Epoch),
		100, nowMs-60_002)
	require.NoError(t, err)
	lc, err = ReadLifecycle(ctx, SQLTx(tx), f.t, 100, grace)
	require.NoError(t, err)
	require.False(t, lc.RecoveryActive, "60002 ms back is past 60s+1ns grace")

	// Forget refuses gen 2 (60001 ms back) and accepts gen 1 (60002 ms back).
	ok, err := Forget(ctx, SQLTx(tx), f.t, 100, 2, grace)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = Forget(ctx, SQLTx(tx), f.t, 100, 1, grace)
	require.NoError(t, err)
	require.True(t, ok)
}

// TestInitializeAfterLossPreservesForgottenGenerationFloor verifies that once
// GC has forgotten every history row of a dropped schema, losing and
// recovering the publication record initializes the allocation counter from
// forgotten_generation rather than 0 so generation numbers are never reused.
func TestInitializeAfterLossPreservesForgottenGenerationFloor(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	cur := f.initialized(t, 100)
	cur, err := f.publish(ctx, cur, f.next(t, cur, KindFlush), nil)
	require.NoError(t, err)
	cur, err = f.publish(ctx, cur, f.next(t, cur, KindFlush), nil)
	require.NoError(t, err)
	_ = f.reset(t, cur)
	f.forget(t, 1)
	f.forget(t, 2)

	// History table is now empty; only forgotten_generation on the epoch row records 1..2.
	require.Empty(t, mustRetained(t, f, time.Hour))
	_, err = f.db.Exec(fmt.Sprintf("DELETE FROM %s WHERE schema_id = $1", f.t.Record), 100)
	require.NoError(t, err)

	h, err := ReadHistory(ctx, DB(f.db), f.t, 100)
	require.NoError(t, err)
	require.True(t, h.Lost())
	require.Equal(t, History{Epoch: 2, ForgottenGeneration: 2, Generations: 0, MaxEpoch: 0, MaxGeneration: 0}, h)

	var rec Record
	require.NoError(t, f.inTx(ctx, func(tx Tx) error {
		var wrote bool
		rec, wrote, err = InitializeAfterLoss(ctx, tx, f.t, 100, h, Dropped)
		require.True(t, wrote)
		return err
	}))
	require.Equal(t, int64(3), rec.Epoch)
	require.Equal(t, int64(2), rec.Generation, "recovered allocation counter must honor forgotten_generation")
	require.Equal(t, int64(2), f.read(t, 100).Generation)

	// Re-activating and publishing allocates generation 3 rather than reusing 1..2.
	active := f.activate(t, rec)
	require.Equal(t, int64(4), active.Epoch)
	require.Equal(t, int64(2), active.Generation)
	g3, err := f.publish(ctx, active, f.next(t, active, KindFlush), nil)
	require.NoError(t, err)
	require.Equal(t, int64(3), g3.Generation)
	require.Zero(t, g3.ParentGeneration)

	// Forgetting the new generation advances the durable forgotten floor to 3.
	_ = f.reset(t, g3)
	f.forget(t, 3)
	lc, err := ReadLifecycle(ctx, DB(f.db), f.t, 100, time.Hour)
	require.NoError(t, err)
	require.Equal(t, int64(3), lc.ForgottenGeneration)
}

func TestStorageBindingLifecycle(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	// A deployment whose init-db predates the binding table.
	_, err := f.db.Exec("DROP TABLE " + f.t.Storage)
	require.NoError(t, err)

	// Before the binding table exists, ReadStorage reports unbound on a pool
	// (not an error), while inside a Tx PostgreSQL aborts the transaction on
	// 42P01 so ReadStorage surfaces the error rather than claiming unbound.
	_, ok, err := ReadStorage(ctx, DB(f.db), f.t)
	require.NoError(t, err)
	require.False(t, ok)
	target := StorageTarget{Endpoint: "https://lake.s3.example", Bucket: "lake", Prefix: "data"}
	_, err = CheckStorage(ctx, DB(f.db), f.t, target)
	require.ErrorIs(t, err, ErrStorageTableMissing, "a writer on such a deployment is told to run init-db before it stages")
	err = f.inTx(ctx, func(tx Tx) error { _, err := BindStorageTx(ctx, tx, f.t, target); return err })
	require.ErrorIs(t, err, ErrStorageTableMissing, "and its publication transaction binds nothing")
	err = f.inTx(ctx, func(tx Tx) error {
		_, _, err := ReadStorage(ctx, tx, f.t)
		return err
	})
	require.Error(t, err, "ReadStorage inside a Tx must not mask 42P01 on an aborted transaction")

	// Create the table using StorageStatement.
	entityMain := strings.TrimSuffix(strings.Trim(f.t.Record, `"`), RecordTableSuffix)
	_, err = f.db.Exec(StorageStatement(forma.EngineAuroraDSQL, entityMain))
	require.NoError(t, err)

	_, ok, err = ReadStorage(ctx, DB(f.db), f.t)
	require.NoError(t, err)
	require.False(t, ok)

	// Ambiguous endpoint path is rejected before writing.
	_, err = BindStorage(ctx, DB(f.db), f.t, StorageTarget{Endpoint: "https://lake.s3.example/tenant//", Bucket: "lake", Prefix: "data"})
	require.ErrorIs(t, err, ErrStorageTargetInvalid)

	first := StorageTarget{Endpoint: "https://lake.s3.example/tenant", Bucket: "lake", Prefix: "data/"}
	b, err := BindStorage(ctx, DB(f.db), f.t, first)
	require.NoError(t, err)
	require.True(t, b.Target.Same(first))
	require.Equal(t, "data", b.Target.Prefix)
	require.NotZero(t, b.BoundAt)

	// Second bind to a different target keeps the first binding untouched.
	other := StorageTarget{Endpoint: "https://s3.example/lake", Bucket: "lake", Prefix: "data"}
	b2, err := BindStorage(ctx, DB(f.db), f.t, other)
	require.NoError(t, err)
	require.True(t, b2.Target.Same(first))
	require.False(t, b2.Target.Same(other))
}

func TestSchemaIDsIncludesDroppedAndLostRecordSchemas(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	_ = f.initialized(t, 10)
	dropped := f.initialized(t, 20)
	f.reset(t, dropped)
	_ = f.initialized(t, 30)
	_, err := f.db.Exec(fmt.Sprintf("DELETE FROM %s WHERE schema_id = $1", f.t.Record), 30)
	require.NoError(t, err)

	ids, err := SchemaIDs(ctx, DB(f.db), f.t)
	require.NoError(t, err)
	require.Equal(t, []int16{10, 20, 30}, ids)
}
