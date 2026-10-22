package cdc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/schemalease"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/lychee-technology/forma/internal/testdb"
)

// The Aurora DSQL engine's schema lease (#633). By default the tests run on
// the integration PostgreSQL. DSQL runs every transaction at REPEATABLE
// READ, so the race test runs on sessions at that level too, where losing a
// race to one row is 40001 as on DSQL. FORMA_DSQL_DSN names an Aurora DSQL
// cluster instead (docs/aurora-dsql.md), where the test that needs a lock
// wait skips.

// pgTestDB opens a database/sql pool on the integration Postgres whose
// sessions default to isolation ("" keeps the server's default). Its
// sessions share an application name of their own, which waitForLockWaiter
// filters on.
func pgTestDB(t *testing.T, isolation string) *sql.DB {
	t.Helper()
	testdb.Connect(t, context.Background()) // skips when Postgres is absent locally
	cfg, err := pgx.ParseConfig(testdb.ResolveDSN())
	require.NoError(t, err)
	if isolation != "" {
		cfg.RuntimeParams["default_transaction_isolation"] = isolation
	}
	cfg.RuntimeParams["application_name"] = fmt.Sprintf("cdc_test_%d", time.Now().UnixNano())
	db := stdlib.OpenDB(*cfg)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// leaseTarget opens the schema lease's test target: the Aurora DSQL cluster
// FORMA_DSQL_DSN names, or else the integration Postgres at isolation.
func leaseTarget(t *testing.T, isolation string) (db *sql.DB, cluster bool) {
	t.Helper()
	dsn := os.Getenv("FORMA_DSQL_DSN")
	if dsn == "" {
		return pgTestDB(t, isolation), false
	}
	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err, "connect to FORMA_DSQL_DSN")
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.Ping(), "connect to FORMA_DSQL_DSN")
	return db, true
}

// leaseTestTable creates a schema-lease table of its own for t.
func leaseTestTable(t *testing.T, db *sql.DB) string {
	t.Helper()
	table := schemalease.Table(fmt.Sprintf("cdc_lease_test_%d", time.Now().UnixNano()))
	_, err := db.Exec(schemalease.Statement(table))
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS " + table) })
	return table
}

func leaseRow(t *testing.T, db *sql.DB, table string, schemaID int16) (holder uuid.UUID, expiresAt int64, ok bool) {
	t.Helper()
	err := db.QueryRow(fmt.Sprintf("SELECT holder, expires_at FROM %s WHERE schema_id = $1", table), schemaID).Scan(&holder, &expiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return uuid.Nil, 0, false
	}
	require.NoError(t, err)
	return holder, expiresAt, true
}

func serverMillis(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	var ms int64
	require.NoError(t, db.QueryRow("SELECT "+sqlgen.ServerTimeMillisSQL).Scan(&ms))
	return ms
}

func TestLeaseSchemaLockExcludesUntilReleased(t *testing.T) {
	db, _ := leaseTarget(t, "")
	table := leaseTestTable(t, db)
	ctx := context.Background()
	first := newLeaseSchemaLocker(db, table, time.Minute, 15*time.Second)
	second := newLeaseSchemaLocker(db, table, time.Minute, 15*time.Second)

	held, release, ok, err := first.TryLock(ctx, 7)
	require.NoError(t, err)
	require.True(t, ok)
	require.NotNil(t, release)

	_, otherRelease, ok, err := second.TryLock(ctx, 7)
	require.NoError(t, err)
	require.False(t, ok, "a live lease must exclude a second holder")
	require.Nil(t, otherRelease)

	_, releaseEight, ok, err := second.TryLock(ctx, 8)
	require.NoError(t, err)
	require.True(t, ok, "leases are per schema")
	releaseEight()

	release()
	require.ErrorIs(t, held.Err(), context.Canceled)
	require.NotErrorIs(t, context.Cause(held), ErrSchemaLockLost, "a release is not a loss")
	_, _, exists := leaseRow(t, db, table, 7)
	require.False(t, exists, "release must delete the lease")

	_, release, ok, err = second.TryLock(ctx, 7)
	require.NoError(t, err)
	require.True(t, ok, "a released schema must be free")
	release()
	release() // idempotent
}

func TestLeaseSchemaLockTakesOverOnlyAnExpiredLease(t *testing.T) {
	db, _ := leaseTarget(t, "")
	table := leaseTestTable(t, db)
	ctx := context.Background()
	locker := newLeaseSchemaLocker(db, table, time.Minute, 15*time.Second)
	dead := uuid.New()

	_, err := db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (7, $1, %s + 60000)", table, sqlgen.ServerTimeMillisSQL), dead)
	require.NoError(t, err)
	_, _, ok, err := locker.TryLock(ctx, 7)
	require.NoError(t, err)
	require.False(t, ok, "a lease that has not expired must not be taken over")

	_, err = db.Exec(fmt.Sprintf("UPDATE %s SET expires_at = %s - 1 WHERE schema_id = 7", table, sqlgen.ServerTimeMillisSQL))
	require.NoError(t, err)
	_, release, ok, err := locker.TryLock(ctx, 7)
	require.NoError(t, err)
	require.True(t, ok, "an expired lease must be taken over")
	holder, expiresAt, _ := leaseRow(t, db, table, 7)
	require.NotEqual(t, dead, holder)
	require.Greater(t, expiresAt, serverMillis(t, db)+50_000, "the takeover must set a fresh expiry")
	release()
}

func TestLeaseSchemaLockRenewsPastItsTTL(t *testing.T) {
	db, _ := leaseTarget(t, "")
	table := leaseTestTable(t, db)
	ctx := context.Background()
	locker := newLeaseSchemaLocker(db, table, 600*time.Millisecond, 150*time.Millisecond)

	held, release, ok, err := locker.TryLock(ctx, 7)
	require.NoError(t, err)
	require.True(t, ok)
	defer release()

	time.Sleep(1500 * time.Millisecond)
	require.NoError(t, held.Err(), "a renewed lease must outlive its TTL")
	_, _, ok, err = newLeaseSchemaLocker(db, table, time.Minute, 15*time.Second).TryLock(ctx, 7)
	require.NoError(t, err)
	require.False(t, ok, "a renewed lease must still exclude")
}

// TestLeaseSchemaLockEndsHeldOnTakeover covers a holder that finds another
// holder's token at its next renewal, as after a pause longer than the TTL:
// held ends with ErrSchemaLockLost, and release leaves the new lease alone.
func TestLeaseSchemaLockEndsHeldOnTakeover(t *testing.T) {
	db, _ := leaseTarget(t, "")
	table := leaseTestTable(t, db)
	locker := newLeaseSchemaLocker(db, table, 600*time.Millisecond, 150*time.Millisecond)

	held, release, ok, err := locker.TryLock(context.Background(), 7)
	require.NoError(t, err)
	require.True(t, ok)
	usurper := uuid.New()
	_, err = db.Exec(fmt.Sprintf("UPDATE %s SET holder = $1 WHERE schema_id = 7", table), usurper)
	require.NoError(t, err)

	select {
	case <-held.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("held must end once the lease belongs to another holder")
	}
	require.ErrorIs(t, context.Cause(held), ErrSchemaLockLost)
	release()
	holder, _, exists := leaseRow(t, db, table, 7)
	require.True(t, exists, "release must not delete another holder's lease")
	require.Equal(t, usurper, holder)
}

// TestLeaseSchemaLockStopsBeforeItsLeaseExpires is the safety property: a
// holder whose renewals cannot get through ends held while the database
// still counts its lease live, so no other holder could have taken it.
func TestLeaseSchemaLockStopsBeforeItsLeaseExpires(t *testing.T) {
	db, cluster := leaseTarget(t, "")
	if cluster {
		t.Skip("blocking the renewals needs a row lock wait, which DSQL does not have")
	}
	table := leaseTestTable(t, db)
	locker := newLeaseSchemaLocker(db, table, 600*time.Millisecond, 150*time.Millisecond)

	held, release, ok, err := locker.TryLock(context.Background(), 7)
	require.NoError(t, err)
	require.True(t, ok)
	defer release()
	blocker, err := db.Begin()
	require.NoError(t, err)
	defer func() { _ = blocker.Rollback() }()
	_, err = blocker.Exec(fmt.Sprintf("SELECT 1 FROM %s WHERE schema_id = 7 FOR UPDATE", table))
	require.NoError(t, err)

	select {
	case <-held.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("held must end when no renewal gets through")
	}
	stoppedAt := serverMillis(t, db)
	require.ErrorIs(t, context.Cause(held), ErrSchemaLockLost)
	require.Contains(t, context.Cause(held).Error(), "not renewed within")
	require.NoError(t, blocker.Rollback())
	_, expiresAt, _ := leaseRow(t, db, table, 7)
	require.Less(t, stoppedAt, expiresAt, "held must end before the lease can be taken over")
}

// TestLeaseSchemaLockRefusesAnAcquireThatOutlivedItsLease pins that an
// acquire answered too late to reach its first renewal fails with that cause
// and frees the schema, rather than handing out a held context that has
// already ended. The database set the expiry from the statement's start, so
// a hold timed from the answer instead would outlast the lease.
func TestLeaseSchemaLockRefusesAnAcquireThatOutlivedItsLease(t *testing.T) {
	db, cluster := leaseTarget(t, "")
	if cluster {
		t.Skip("delaying the acquire needs a row lock wait, which DSQL does not have")
	}
	table := leaseTestTable(t, db)
	_, err := db.Exec(fmt.Sprintf("INSERT INTO %s (schema_id, holder, expires_at) VALUES (7, $1, 0)", table), uuid.New())
	require.NoError(t, err)
	blocker, err := db.Begin()
	require.NoError(t, err)
	defer func() { _ = blocker.Rollback() }()
	_, err = blocker.Exec(fmt.Sprintf("SELECT 1 FROM %s WHERE schema_id = 7 FOR UPDATE", table))
	require.NoError(t, err)

	locker := newLeaseSchemaLocker(db, table, 600*time.Millisecond, 150*time.Millisecond)
	type result struct {
		held context.Context
		ok   bool
		err  error
	}
	done := make(chan result, 1)
	go func() {
		held, release, ok, err := locker.TryLock(context.Background(), 7)
		if ok {
			defer release()
		}
		done <- result{held: held, ok: ok, err: err}
	}()
	waitForLockWaiter(t, db)
	time.Sleep(locker.validFor())
	require.NoError(t, blocker.Rollback())

	got := <-done
	require.Error(t, got.err)
	require.Contains(t, got.err.Error(), "schema 7")
	require.Contains(t, got.err.Error(), "too late to reach its first renewal")
	require.False(t, got.ok)
	require.Nil(t, got.held)
	_, _, present := leaseRow(t, db, table, 7)
	require.False(t, present, "the refused acquire must give its lease up")
}

// TestLeaseSchemaLockOneWinnerAmongRacingAcquirers races acquirers on a free
// schema and on an expired lease, at both isolation levels: each round has
// exactly one holder, and the losers answer ok false, never an error.
func TestLeaseSchemaLockOneWinnerAmongRacingAcquirers(t *testing.T) {
	for _, isolation := range []string{"read committed", "repeatable read"} {
		t.Run(isolation, func(t *testing.T) {
			db, cluster := leaseTarget(t, isolation)
			if cluster && isolation != "repeatable read" {
				t.Skip("DSQL runs only REPEATABLE READ")
			}
			db.SetMaxOpenConns(16)
			table := leaseTestTable(t, db)
			for round := range 20 {
				if round%2 == 1 {
					_, err := db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (7, $1, %s - 1)", table, sqlgen.ServerTimeMillisSQL), uuid.New())
					require.NoError(t, err)
				}
				releases := raceAcquirers(t, db, table, 8)
				require.Len(t, releases, 1, "round %d must have exactly one holder", round)
				releases[0]()
			}
		})
	}
}

func raceAcquirers(t *testing.T, db *sql.DB, table string, n int) []func() {
	t.Helper()
	var (
		mu       sync.Mutex
		releases []func()
		errs     []error
		wg       sync.WaitGroup
	)
	start := make(chan struct{})
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			locker := newLeaseSchemaLocker(db, table, time.Minute, 15*time.Second)
			_, release, ok, err := locker.TryLock(context.Background(), 7)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
			}
			if ok {
				releases = append(releases, release)
			}
		}()
	}
	close(start)
	wg.Wait()
	require.Empty(t, errs, "a lost race must answer ok false, not an error")
	return releases
}
