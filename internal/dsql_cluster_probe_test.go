package internal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

// TestDSQLClusterProbe runs #633's check against the Aurora DSQL cluster
// FORMA_DSQL_DSN names and logs what the cluster answers, for the issue
// record:
//
//	PGPASSWORD=<IAM token> FORMA_DSQL_DSN='postgres://admin@<endpoint>:5432/postgres?sslmode=require' \
//	  go test ./internal -run TestDSQLClusterProbe -v
//
// docs/aurora-dsql.md shows how to generate the token.
//
// The six statements and the advisory-lock exclusion check are logged, not
// asserted: Forma's aurora-dsql engine uses none of them. What it does use
// is asserted: two transactions that write one guard row, present or not,
// never both commit.
func TestDSQLClusterProbe(t *testing.T) {
	dsn := os.Getenv("FORMA_DSQL_DSN")
	if dsn == "" {
		t.Skip("FORMA_DSQL_DSN names no Aurora DSQL cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	a, b := probeConnect(t, ctx, dsn), probeConnect(t, ctx, dsn)

	t.Log("#633 check, one session, as psql runs it:")
	for _, stmt := range []string{
		"SELECT pg_advisory_xact_lock(1)",
		"SELECT pg_try_advisory_lock(1, 1)",
		"SELECT pg_advisory_unlock(1, 1)",
		"BEGIN ISOLATION LEVEL READ COMMITTED",
		"SHOW transaction_isolation",
		"COMMIT",
	} {
		t.Logf("  %s;\n    -> %s", stmt, probeRun(ctx, a, stmt))
	}
	t.Logf("a plain BEGIN, as the aurora-dsql engine sends it:\n    -> %s",
		probeRun(ctx, a, "BEGIN; SHOW transaction_isolation; COMMIT"))
	probeAdvisoryExclusion(t, ctx, a, probeConnect(t, ctx, dsn))

	table := fmt.Sprintf("forma_probe_guard_%d", time.Now().UnixNano())
	_, err := a.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (k INT PRIMARY KEY, generation BIGINT NOT NULL)", table))
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = a.Exec(context.Background(), "DROP TABLE IF EXISTS "+table) })
	_, err = a.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, 1)", table))
	require.NoError(t, err)
	for _, k := range []int{1, 2} {
		first, second := probeGuardRace(ctx, a, b, table, k)
		t.Logf("two transactions upsert guard row %d (present: %t): first COMMIT -> %v; second COMMIT -> %v", k, k == 1, first, second)
		require.NoError(t, first)
		var pgErr *pgconn.PgError
		require.True(t, errors.As(second, &pgErr) && pgErr.Code == "40001",
			"the second writer of one guard row must fail with 40001, got %v", second)
	}
}

func probeConnect(t *testing.T, ctx context.Context, dsn string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err, "connect to FORMA_DSQL_DSN")
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// probeRun sends sql over the simple protocol, as psql does, and renders
// each result: its rows and command tag, or its error with SQLSTATE.
func probeRun(ctx context.Context, conn *pgx.Conn, sql string) string {
	results, err := conn.PgConn().Exec(ctx, sql).ReadAll()
	var out []string
	for _, r := range results {
		var rows []string
		for _, row := range r.Rows {
			for _, v := range row {
				rows = append(rows, string(v))
			}
		}
		out = append(out, fmt.Sprintf("%s %v", r.CommandTag, rows))
	}
	if err != nil {
		out = append(out, probeError(err))
	}
	return strings.Join(out, "; ")
}

func probeError(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return fmt.Sprintf("ERROR %s: %s (%s)", pgErr.Code, pgErr.Message, pgErr.Detail)
	}
	return "ERROR " + err.Error()
}

// probeAdvisoryExclusion is the issue's two-session check, run only when
// pg_advisory_xact_lock is accepted: a lock that returns at once in the
// second session excludes nobody. b is the check's own: a wait it gives up
// on may close it.
func probeAdvisoryExclusion(t *testing.T, ctx context.Context, a, b *pgx.Conn) {
	t.Helper()
	tx, err := a.Begin(ctx)
	if err != nil {
		t.Logf("advisory exclusion: BEGIN failed: %s", probeError(err))
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(1)"); err != nil {
		t.Logf("advisory exclusion: not checked, the lock is refused: %s", probeError(err))
		return
	}
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, err = b.Exec(waitCtx, "SELECT pg_advisory_xact_lock(1)")
	if waitCtx.Err() != nil {
		t.Log("advisory exclusion: the second session waited 3s for the lock the first holds")
		return
	}
	t.Logf("advisory exclusion: the second session returned at once (%v): the lock excludes nobody", err)
}

// probeGuardRace runs Forma's guard upsert of row k in two open
// transactions, then commits them in turn, and answers both COMMIT errors.
func probeGuardRace(ctx context.Context, a, b *pgx.Conn, table string, k int) (first, second error) {
	upsert := fmt.Sprintf("INSERT INTO %s AS g (k, generation) VALUES ($1, 1) ON CONFLICT (k) DO UPDATE SET generation = g.generation + 1", table)
	txA, err := a.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin the first transaction: %w", err), nil
	}
	defer func() { _ = txA.Rollback(ctx) }()
	txB, err := b.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin the second transaction: %w", err), nil
	}
	defer func() { _ = txB.Rollback(ctx) }()
	if _, err := txA.Exec(ctx, upsert, k); err != nil {
		return fmt.Errorf("first upsert: %w", err), nil
	}
	upserted := make(chan error, 1)
	go func() {
		_, err := txB.Exec(ctx, upsert, k)
		upserted <- err
	}()
	var upsertErr error
	select {
	case upsertErr = <-upserted: // DSQL: no statement waits
		first = txA.Commit(ctx)
	case <-time.After(2 * time.Second): // PostgreSQL: it waits for the first
		first = txA.Commit(ctx)
		upsertErr = <-upserted
	}
	if upsertErr != nil {
		// A statement-time conflict is the same adjudication, reported sooner.
		return first, fmt.Errorf("second upsert: %w", upsertErr)
	}
	return first, txB.Commit(ctx)
}
