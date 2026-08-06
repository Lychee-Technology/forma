package internal

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// Real-Postgres anchors for the #274 strictly-ordered per-row version
// contract (frozen clock; split from the main integration file to keep both
// under the 500-line source limit).

// TestSameMillisecondWritesStayStrictlyOrderedIntegration pins the #274
// write-side version contract against real PostgreSQL: serialized writes to
// one row NEVER share a version timestamp, even when the wall clock does not
// advance between them. GREATEST($now, ltbase_updated_at + 1) computes the
// effective version in PG, RETURNING hands it back, and change_log receives
// the identical stamp — pre-#274 two same-millisecond updates tied at the
// clock read and the cold-tier LWW had no discriminator left to order them.
func TestSameMillisecondWritesStayStrictlyOrderedIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	pool := connectTestPostgres(t, ctx)
	tables := createTempPersistentTables(t, ctx, pool)

	mc := schemameta.NewMetadataCache()
	require.NoError(t, mc.RegisterSchema("monotonic_schema", 1, forma.SchemaAttributeCache{
		"a": {AttributeName: "a", AttributeID: 10, ValueType: forma.ValueTypeText},
	}))
	repo := NewDBPersistentRecordRepository(pool, mc)

	frozen := time.Date(2026, 8, 5, 6, 7, 8, 0, time.UTC)
	repo.withClock(func() time.Time { return frozen })
	frozenMillis := frozen.UnixMilli()

	rowID := uuid.New()
	record := &model.PersistentRecord{
		SchemaID:  1,
		RowID:     rowID,
		TextItems: map[string]string{"text_01": "v1"},
	}
	require.NoError(t, repo.InsertPersistentRecord(ctx, tables, record))
	require.Equal(t, frozenMillis, record.UpdatedAt, "create stamps the clock read")

	assertChangeLogStamp := func(wantChangedAt int64, wantDeleted *int64) {
		t.Helper()
		var changedAt int64
		var deletedAt pgtype.Int8
		query := fmt.Sprintf(`SELECT changed_at, deleted_at FROM %s WHERE schema_id = $1 AND row_id = $2 AND flushed_at = 0`, sanitizeIdentifier(tables.ChangeLog))
		require.NoError(t, pool.QueryRow(ctx, query, record.SchemaID, rowID).Scan(&changedAt, &deletedAt))
		require.Equal(t, wantChangedAt, changedAt, "change_log must carry the effective version, not the clock read")
		if wantDeleted == nil {
			require.False(t, deletedAt.Valid)
		} else {
			require.True(t, deletedAt.Valid)
			require.Equal(t, *wantDeleted, deletedAt.Int64)
		}
	}

	// Update #1 on the same frozen millisecond: the effective version must
	// advance past the create, in both stores.
	record.TextItems["text_01"] = "v2"
	require.NoError(t, repo.UpdatePersistentRecord(ctx, tables, record))
	require.Equal(t, frozenMillis+1, record.UpdatedAt, "same-millisecond update must advance the version by 1")
	assertChangeLogStamp(frozenMillis+1, nil)

	// Update #2, still on the same frozen millisecond: strictly ordered again.
	record.TextItems["text_01"] = "v3"
	require.NoError(t, repo.UpdatePersistentRecord(ctx, tables, record))
	require.Equal(t, frozenMillis+2, record.UpdatedAt)
	assertChangeLogStamp(frozenMillis+2, nil)

	// Delete, same frozen millisecond: the tombstone must rank strictly after
	// the clock-ahead live version or the delete loses the LWW tie.
	require.NoError(t, repo.DeletePersistentRecord(ctx, tables, record.SchemaID, rowID))
	wantTombstone := frozenMillis + 3
	assertChangeLogStamp(wantTombstone, &wantTombstone)
}

// TestRecreateOutranksClockAheadTombstoneIntegration pins the #274 recreate
// leg against real PostgreSQL: a row recreated after a clock-ahead delete
// must obtain a version strictly above its retained tombstone — a bare
// clock-read stamp would regress the slot-0 version and lose the LWW fold to
// the tombstone forever once it reaches parquet (review round 3 P1).
func TestRecreateOutranksClockAheadTombstoneIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	pool := connectTestPostgres(t, ctx)
	tables := createTempPersistentTables(t, ctx, pool)

	mc := schemameta.NewMetadataCache()
	require.NoError(t, mc.RegisterSchema("recreate_schema", 1, forma.SchemaAttributeCache{
		"a": {AttributeName: "a", AttributeID: 10, ValueType: forma.ValueTypeText},
	}))
	repo := NewDBPersistentRecordRepository(pool, mc)

	frozen := time.Date(2026, 8, 6, 7, 8, 9, 0, time.UTC)
	repo.withClock(func() time.Time { return frozen })
	frozenMillis := frozen.UnixMilli()

	rowID := uuid.New()
	record := &model.PersistentRecord{
		SchemaID:  1,
		RowID:     rowID,
		TextItems: map[string]string{"text_01": "v1"},
	}
	require.NoError(t, repo.InsertPersistentRecord(ctx, tables, record))
	require.Equal(t, frozenMillis, record.UpdatedAt, "fresh row_id stamps the clock read")

	// Same-millisecond delete: the tombstone lands at T+1, ahead of the
	// still-frozen clock.
	require.NoError(t, repo.DeletePersistentRecord(ctx, tables, record.SchemaID, rowID))

	// Recreate on the same frozen millisecond: the new live version must land
	// strictly above the retained tombstone (T+2), not at the clock read (T).
	recreated := &model.PersistentRecord{
		SchemaID:  1,
		RowID:     rowID,
		TextItems: map[string]string{"text_01": "v2"},
	}
	require.NoError(t, repo.InsertPersistentRecord(ctx, tables, recreated))
	require.Equal(t, frozenMillis, recreated.CreatedAt, "CreatedAt stays the identity clock read")
	require.Equal(t, frozenMillis+2, recreated.UpdatedAt,
		"recreate must outrank the retained tombstone, not regress to the clock read")

	var changedAt int64
	var deletedAt pgtype.Int8
	query := fmt.Sprintf(`SELECT changed_at, deleted_at FROM %s WHERE schema_id = $1 AND row_id = $2 AND flushed_at = 0`, sanitizeIdentifier(tables.ChangeLog))
	require.NoError(t, pool.QueryRow(ctx, query, recreated.SchemaID, rowID).Scan(&changedAt, &deletedAt))
	require.Equal(t, frozenMillis+2, changedAt, "change_log carries the recreate's effective version")
	require.False(t, deletedAt.Valid, "the slot-0 entry is live again")

	var mainVer int64
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(
		"SELECT ltbase_updated_at FROM %s WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2",
		sanitizeIdentifier(tables.EntityMain)), recreated.SchemaID, rowID).Scan(&mainVer))
	require.Equal(t, frozenMillis+2, mainVer, "entity_main and change_log stay same-source (#210)")
}

// TestConcurrentDeleteRecreateSerializesIntegration pins the #274 round-4
// contract against real PostgreSQL: create and delete allocate versions
// under a per-row transaction advisory lock, so a recreate can never read
// the row's version history while a concurrent delete's tombstone is in
// flight. The test holds the same lock from a separate session to keep the
// window deterministically open, proves the recreate blocks, flushes the
// tombstone INSIDE the window (the review's exact race: CDC marks the
// tombstone between the recreate's history read and its change-log upsert),
// and then shows the released recreate still lands strictly above the
// flushed tombstone.
func TestConcurrentDeleteRecreateSerializesIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	pool := connectTestPostgres(t, ctx)
	tables := createTempPersistentTables(t, ctx, pool)

	mc := schemameta.NewMetadataCache()
	require.NoError(t, mc.RegisterSchema("serialize_schema", 1, forma.SchemaAttributeCache{
		"a": {AttributeName: "a", AttributeID: 10, ValueType: forma.ValueTypeText},
	}))
	repo := NewDBPersistentRecordRepository(pool, mc)

	frozen := time.Date(2026, 8, 6, 9, 10, 11, 0, time.UTC)
	repo.withClock(func() time.Time { return frozen })
	frozenMillis := frozen.UnixMilli()

	rowID := uuid.New()
	record := &model.PersistentRecord{
		SchemaID:  1,
		RowID:     rowID,
		TextItems: map[string]string{"text_01": "v1"},
	}
	require.NoError(t, repo.InsertPersistentRecord(ctx, tables, record))
	require.NoError(t, repo.DeletePersistentRecord(ctx, tables, record.SchemaID, rowID))
	// Same frozen millisecond: the tombstone sits at T+1, ahead of the clock.

	// A separate session takes the same per-row lock, simulating an in-flight
	// version allocation the recreate must wait behind. Session-level and
	// transaction-level advisory locks share one lock table.
	lockConn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer lockConn.Release()
	key := rowVersionLockKey(record.SchemaID, rowID)
	_, err = lockConn.Exec(ctx, "SELECT pg_advisory_lock($1)", key)
	require.NoError(t, err)

	recreated := &model.PersistentRecord{
		SchemaID:  1,
		RowID:     rowID,
		TextItems: map[string]string{"text_01": "v2"},
	}
	done := make(chan error, 1)
	go func() { done <- repo.InsertPersistentRecord(ctx, tables, recreated) }()

	// The recreate must be blocked on the lock — its history read has not
	// happened yet.
	select {
	case err := <-done:
		t.Fatalf("recreate completed while the row version lock was held: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	// Flush the tombstone INSIDE the held window: the recreate's later
	// history read must still see it (flushed slots count).
	_, err = pool.Exec(ctx, fmt.Sprintf(
		"UPDATE %s SET flushed_at = $1 WHERE schema_id = $2 AND row_id = $3 AND flushed_at = 0",
		sanitizeIdentifier(tables.ChangeLog)), frozenMillis+500, record.SchemaID, rowID)
	require.NoError(t, err)

	_, err = lockConn.Exec(ctx, "SELECT pg_advisory_unlock($1)", key)
	require.NoError(t, err)
	require.NoError(t, <-done, "recreate must proceed once the lock releases")

	require.Equal(t, frozenMillis+2, recreated.UpdatedAt,
		"the recreate must land strictly above the flushed tombstone it waited behind")

	var liveChangedAt int64
	var liveDeleted pgtype.Int8
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(
		"SELECT changed_at, deleted_at FROM %s WHERE schema_id = $1 AND row_id = $2 AND flushed_at = 0",
		sanitizeIdentifier(tables.ChangeLog)), record.SchemaID, rowID).Scan(&liveChangedAt, &liveDeleted))
	require.Equal(t, frozenMillis+2, liveChangedAt)
	require.False(t, liveDeleted.Valid)

	// True-concurrency sweep on fresh rows: under the frozen clock every
	// delete/recreate pair must still come out strictly ordered.
	assertConcurrentDeleteRecreatePairsStayOrdered(ctx, t, pool, repo, tables, frozenMillis)
}

// assertConcurrentDeleteRecreatePairsStayOrdered races a delete against a
// recreate (retrying past the PK conflict) on fresh rows and asserts the
// final live version is exactly create+2. The unflushed tombstone slot being
// overwritten by the recreate is correct behavior (it never reached
// parquet), so the observable invariant is the final live version:
// serialization forces delete-then-recreate, and under the frozen clock that
// is exactly T, T+1, T+2 — pre-fix the recreate could tie the tombstone at
// T+1 or stamp the bare clock T.
func assertConcurrentDeleteRecreatePairsStayOrdered(ctx context.Context, t *testing.T, pool *pgxpool.Pool, repo *DBPersistentRecordRepository, tables model.StorageTables, frozenMillis int64) {
	t.Helper()
	for i := 0; i < 10; i++ {
		id := uuid.New()
		require.NoError(t, repo.InsertPersistentRecord(ctx, tables,
			&model.PersistentRecord{SchemaID: 1, RowID: id, TextItems: map[string]string{"text_01": "x"}}))

		delDone := make(chan error, 1)
		recDone := make(chan error, 1)
		go func() { delDone <- repo.DeletePersistentRecord(ctx, tables, 1, id) }()
		go func() {
			for {
				err := repo.InsertPersistentRecord(ctx, tables,
					&model.PersistentRecord{SchemaID: 1, RowID: id, TextItems: map[string]string{"text_01": "y"}})
				if err == nil || !errors.Is(err, forma.ErrConflict) {
					recDone <- err
					return
				}
			}
		}()
		require.NoError(t, <-delDone)
		require.NoError(t, <-recDone)

		var live int64
		require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(
			"SELECT changed_at FROM %s WHERE schema_id = $1 AND row_id = $2 AND flushed_at = 0 AND deleted_at IS NULL",
			sanitizeIdentifier(tables.ChangeLog)), 1, id).Scan(&live))
		require.Equal(t, frozenMillis+2, live,
			"pair %d: the recreate must land strictly above the concurrent tombstone, never tie or regress", i)
	}
}
