package internal

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/testdb"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// Real-PostgreSQL proofs of #645's statement-side classification: each test
// makes the server raise the SQLSTATE for real, through a repository write
// path, and asserts the typed outcome the caller sees.

// openTunedPool opens a second pool on the integration database with cfg
// adjusted by tune. Call it after testdb.Connect, which settles whether the
// database is reachable (skip locally, fail in CI).
func openTunedPool(t *testing.T, ctx context.Context, tune func(*pgxpool.Config)) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(testdb.ResolveDSN())
	require.NoError(t, err)
	tune(cfg)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

// hookedPool runs beforeCommit just before each of its transactions commits,
// so a test can stop a writer at the COMMIT boundary.
type hookedPool struct {
	*pgxpool.Pool
	beforeCommit func(ctx context.Context, tx pgx.Tx)
}

func (p *hookedPool) BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
	tx, err := p.Pool.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &hookedTx{Tx: tx, beforeCommit: p.beforeCommit}, nil
}

type hookedTx struct {
	pgx.Tx
	beforeCommit func(ctx context.Context, tx pgx.Tx)
}

func (t *hookedTx) Commit(ctx context.Context) error {
	if t.beforeCommit != nil {
		t.beforeCommit(ctx, t.Tx)
	}
	return t.Tx.Commit(ctx)
}

// waitForAdvisoryWaiter blocks until some session is waiting, ungranted, on
// the single-bigint advisory lock key. pg_locks splits such a key into
// classid (high 32 bits) and objid (low 32 bits), with objsubid 1.
func waitForAdvisoryWaiter(ctx context.Context, t *testing.T, pool *pgxpool.Pool, key int64) {
	t.Helper()
	hi, lo := int64(uint64(key)>>32), int64(uint64(key)&0xffffffff)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var waiting bool
		err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_locks
			WHERE locktype = 'advisory' AND NOT granted
			  AND classid::bigint = $1 AND objid::bigint = $2 AND objsubid = 1)`, hi, lo).Scan(&waiting)
		if err != nil {
			t.Errorf("poll pg_locks: %v", err)
			return
		}
		if waiting {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("no session started waiting on advisory lock %d", key)
}

// mainText reads text_01 of a main row straight from the table; found is
// false when the row is absent.
func mainText(ctx context.Context, t *testing.T, pool *pgxpool.Pool, tables model.StorageTables, rowID uuid.UUID) (string, bool) {
	t.Helper()
	var text *string
	err := pool.QueryRow(ctx, fmt.Sprintf("SELECT text_01 FROM %s WHERE ltbase_schema_id = 1 AND ltbase_row_id = $1",
		sanitizeIdentifier(tables.EntityMain)), rowID).Scan(&text)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false
	}
	require.NoError(t, err)
	if text == nil {
		return "", true
	}
	return *text, true
}

func requireSQLState(t *testing.T, err error, code string) {
	t.Helper()
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr), "the driver error must stay in the chain: %v", err)
	require.Equal(t, code, pgErr.Code)
}

// TestInsertDuplicateRowIDIsUniqueConflict is Given/When/Then 1: a create
// whose row id already exists is a unique conflict that still satisfies
// ErrConflict. Since #651 it is found without a failed statement (the main
// insert is ON CONFLICT DO NOTHING under the row lock), so inside a Tx it is
// an expected outcome, and its message names the schema and the row. A 23505
// that still reaches the classifier keeps the curated row-identity message
// (TestMergeClassifiesUniqueViolationAsConflict).
func TestInsertDuplicateRowIDIsUniqueConflict(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := testdb.Connect(t, ctx)
	tables := createTempPersistentTables(t, ctx, pool)
	repo := NewDBPersistentRecordRepository(pool, concurrencyTestCache(t, 1))

	rowID := uuid.New()
	first := &model.PersistentRecord{SchemaID: 1, RowID: rowID, TextItems: map[string]string{"text_01": "first"}}
	require.NoError(t, repo.InsertPersistentRecord(ctx, tables, first))

	second := &model.PersistentRecord{SchemaID: 1, RowID: rowID, TextItems: map[string]string{"text_01": "second"}}
	err := repo.InsertPersistentRecord(ctx, tables, second)
	require.Error(t, err)

	require.ErrorIs(t, err, forma.ErrUniqueConflict)
	require.ErrorIs(t, err, forma.ErrConflict, "callers that predate #645 keep matching")
	require.False(t, forma.IsRetryable(err))
	require.NotErrorIs(t, err, forma.ErrCommitOutcomeUnknown)
	var pgErr *pgconn.PgError
	require.False(t, errors.As(err, &pgErr), "no statement failed: %v", err)

	msg, ok := forma.ResolvePublicMessage(err)
	require.True(t, ok)
	require.Equal(t, fmt.Sprintf("schema with id 1 already has a row with ID %s", rowID), msg)
	for _, physical := range []string{"ltbase_", "pkey", "Key (", "entity_main"} {
		require.NotContains(t, msg, physical)
	}

	got, found := mainText(ctx, t, pool, tables, rowID)
	require.True(t, found)
	require.Equal(t, "first", got, "the rejected create must leave the stored row alone")
}

// TestLockTimeoutIsRetryableConflict_PostgresOnly is Given/When/Then 2: a
// write whose advisory-lock wait outlives lock_timeout gets 55P03 from the
// lock statement itself, which before #645 reached the caller unclassified.
func TestLockTimeoutIsRetryableConflict_PostgresOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := testdb.Connect(t, ctx)
	tables := createTempPersistentTables(t, ctx, pool)
	cache := concurrencyTestCache(t, 1)

	rowID := uuid.New()
	seed := &model.PersistentRecord{SchemaID: 1, RowID: rowID, TextItems: map[string]string{"text_01": "seed"}}
	require.NoError(t, NewDBPersistentRecordRepository(pool, cache).InsertPersistentRecord(ctx, tables, seed))

	holder, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer holder.Release()
	key := rowVersionLockKey(1, rowID)
	_, err = holder.Exec(ctx, "SELECT pg_advisory_lock($1)", key)
	require.NoError(t, err)
	defer func() { _, _ = holder.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", key) }()

	timed := openTunedPool(t, ctx, func(cfg *pgxpool.Config) {
		cfg.ConnConfig.RuntimeParams["lock_timeout"] = "100ms"
	})
	_, err = NewDBPersistentRecordRepository(timed, cache).MergePersistentRecord(ctx, tables, 1, rowID, model.RowGuard{},
		func(context.Context, *model.PersistentRecord, model.TxClock) (*model.PersistentRecord, error) {
			t.Error("the merge ran without the row lock")
			return nil, errors.New("unreachable")
		})
	require.Error(t, err)

	require.ErrorIs(t, err, forma.ErrRetryableConflict)
	require.True(t, forma.IsRetryable(err))
	require.NotErrorIs(t, err, forma.ErrConflict)
	requireSQLState(t, err, sqlstateLockNotAvailable)
	_, published := forma.ResolvePublicMessage(err)
	require.False(t, published, "a retryable conflict publishes nothing")

	got, _ := mainText(ctx, t, pool, tables, rowID)
	require.Equal(t, "seed", got, "a retryable conflict is rolled back")
}

// TestWriteSerializationFailureIsRetryable_PostgresOnly produces a real 40001
// on a write path. Forma pins nothing above READ COMMITTED on PostgreSQL, so
// the victim's session defaults to REPEATABLE READ, the level Aurora DSQL
// always runs at. Its delete takes its snapshot when the lock statement
// starts, waits behind a delete of the same row that commits meanwhile, and
// then finds the row gone under its snapshot.
func TestWriteSerializationFailureIsRetryable_PostgresOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := testdb.Connect(t, ctx)
	tables := createTempPersistentTables(t, ctx, pool)
	cache := concurrencyTestCache(t, 1)

	rowID := uuid.New()
	seed := &model.PersistentRecord{SchemaID: 1, RowID: rowID, TextItems: map[string]string{"text_01": "seed"}}
	require.NoError(t, NewDBPersistentRecordRepository(pool, cache).InsertPersistentRecord(ctx, tables, seed))
	key := rowVersionLockKey(1, rowID)

	atCommit := make(chan struct{})
	first := NewDBPersistentRecordRepository(&hookedPool{Pool: pool, beforeCommit: func(context.Context, pgx.Tx) {
		close(atCommit)
		waitForAdvisoryWaiter(ctx, t, pool, key) // the victim holds its snapshot
	}}, cache)
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.DeletePersistentRecord(ctx, tables, 1, rowID, model.RowGuard{}) }()
	select {
	case <-atCommit:
	case err := <-firstDone:
		t.Fatalf("the first delete finished before reaching COMMIT: %v", err)
	}

	repeatable := openTunedPool(t, ctx, func(cfg *pgxpool.Config) {
		cfg.ConnConfig.RuntimeParams["default_transaction_isolation"] = "repeatable read"
	})
	err := NewDBPersistentRecordRepository(repeatable, cache).DeletePersistentRecord(ctx, tables, 1, rowID, model.RowGuard{})
	require.NoError(t, <-firstDone)
	require.Error(t, err)

	require.ErrorIs(t, err, forma.ErrRetryableConflict)
	require.True(t, forma.IsRetryable(err))
	require.NotErrorIs(t, err, forma.ErrConflict)
	requireSQLState(t, err, sqlstateSerializationFailure)
}

// TestWriteDeadlockIsRetryableConflict_PostgresOnly is Given/When/Then 3.
// Forma's own writers cannot deadlock each other (#554 sorts every batch's
// locks), so the other party is an interactive transaction that takes the
// same advisory locks in the opposite order. PostgreSQL aborts exactly one of
// the two with 40P01. That one is a retryable conflict and is rolled back
// whole: both rows end up carrying the survivor's marker.
func TestWriteDeadlockIsRetryableConflict_PostgresOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := testdb.Connect(t, ctx)
	tables := createTempPersistentTables(t, ctx, pool)
	repo := NewDBPersistentRecordRepository(pool, concurrencyTestCache(t, 1))

	keys := sortedRowKeys([]model.PersistentRecordKey{{SchemaID: 1, RowID: uuid.New()}, {SchemaID: 1, RowID: uuid.New()}})
	first, second := keys[0], keys[1] // the batch locks first, then second
	for _, key := range keys {
		seed := &model.PersistentRecord{SchemaID: 1, RowID: key.RowID, TextItems: map[string]string{"text_01": "seed"}}
		require.NoError(t, repo.InsertPersistentRecord(ctx, tables, seed))
	}

	holdsSecond := make(chan struct{})
	interactive := make(chan error, 1)
	go func() {
		interactive <- repo.inWriteTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
			if err := lockRowVersion(ctx, tx, 1, second.RowID); err != nil {
				return err
			}
			close(holdsSecond)
			// The batch now holds first and waits on second: close the cycle.
			waitForAdvisoryWaiter(ctx, t, pool, rowVersionLockKey(1, second.RowID))
			if err := lockRowVersion(ctx, tx, 1, first.RowID); err != nil {
				return err
			}
			for _, key := range keys {
				marker := &model.PersistentRecord{SchemaID: 1, RowID: key.RowID, UpdatedAt: repo.nowMillis(),
					TextItems: map[string]string{"text_01": "interactive"}}
				if err := repo.updateMainRow(ctx, tx, tables.EntityMain, marker); err != nil {
					return err
				}
			}
			return nil
		})
	}()
	select {
	case <-holdsSecond:
	case err := <-interactive:
		t.Fatalf("the interactive transaction finished before locking: %v", err)
	}

	_, batchErr := repo.BatchMergePersistentRecords(ctx, tables, keys,
		func(_ context.Context, _ int, existing *model.PersistentRecord) (*model.PersistentRecord, error) {
			next := *existing
			next.TextItems = map[string]string{"text_01": "batch"}
			return &next, nil
		})
	interactiveErr := <-interactive

	aborted, survivor := batchErr, "interactive"
	if batchErr == nil {
		aborted, survivor = interactiveErr, "batch"
	} else {
		require.NoError(t, interactiveErr, "exactly one party is aborted")
	}
	require.Error(t, aborted, "PostgreSQL must abort one party of the deadlock")
	require.ErrorIs(t, aborted, forma.ErrRetryableConflict)
	require.True(t, forma.IsRetryable(aborted))
	require.NotErrorIs(t, aborted, forma.ErrConflict)
	requireSQLState(t, aborted, sqlstateDeadlockDetected)
	_, published := forma.ResolvePublicMessage(aborted)
	require.False(t, published, "a retryable conflict publishes nothing")

	for _, key := range keys {
		got, _ := mainText(ctx, t, pool, tables, key.RowID)
		require.Equal(t, survivor, got, "the aborted party must be rolled back whole")
	}
}
