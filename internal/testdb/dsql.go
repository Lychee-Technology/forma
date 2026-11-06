package testdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// The environment that names an aurora-dsql test target (#633, #739).
// FORMA_DSQL_DSN alone names a genuine Aurora DSQL cluster, as
// docs/aurora-dsql.md documents. FORMA_DSQL_TARGET=ministack says the same DSN
// reaches MiniStack's strict SQL proxy (docs/dsql-ministack.md) instead.
const (
	DSQLDSNEnv    = "FORMA_DSQL_DSN"
	DSQLTargetEnv = "FORMA_DSQL_TARGET"

	targetMiniStack = "ministack"
)

// DSQLKind says what an aurora-dsql test target is. The difference decides
// which assertions run: PostgreSQL's lock waits exist behind MiniStack's proxy
// and do not exist on a cluster, so a test that needs one skips only there.
type DSQLKind int

const (
	// DSQLEmulated is no named target: the integration PostgreSQL at
	// REPEATABLE READ.
	DSQLEmulated DSQLKind = iota
	// DSQLCluster is a genuine Aurora DSQL cluster.
	DSQLCluster
	// DSQLMiniStack is MiniStack's strict SQL proxy over PostgreSQL.
	DSQLMiniStack
)

// MiniStackTarget reports whether the run says it targets MiniStack's proxy.
func MiniStackTarget() bool { return os.Getenv(DSQLTargetEnv) == targetMiniStack }

// ResolveDSQL answers the aurora-dsql test target the environment names. A
// target it cannot honor fails the test rather than falling back: a misspelt
// FORMA_DSQL_TARGET would run the lane as a cluster and skip its lock-wait
// tests, and MiniStack without a DSN would run on the integration PostgreSQL
// and still report a pass.
func ResolveDSQL(t testing.TB) (dsn string, kind DSQLKind) {
	t.Helper()
	dsn, kind, err := resolveDSQL(os.Getenv(DSQLDSNEnv), os.Getenv(DSQLTargetEnv))
	if err != nil {
		t.Fatal(err)
	}
	return dsn, kind
}

func resolveDSQL(dsn, target string) (string, DSQLKind, error) {
	switch {
	case target != "" && target != targetMiniStack:
		return "", DSQLEmulated, fmt.Errorf("%s=%q is not a known target: the only value is %q", DSQLTargetEnv, target, targetMiniStack)
	case target == targetMiniStack && dsn == "":
		return "", DSQLEmulated, fmt.Errorf("%s=%s needs %s to name the proxy", DSQLTargetEnv, targetMiniStack, DSQLDSNEnv)
	case target == targetMiniStack:
		return dsn, DSQLMiniStack, nil
	case dsn != "":
		return dsn, DSQLCluster, nil
	}
	return "", DSQLEmulated, nil
}

// NameSession answers a connection hook that names a session after it opens.
// MiniStack's proxy authenticates the client itself and opens its backend
// connection with only a user and a database, so an application_name sent as
// a startup parameter never reaches PostgreSQL, and a test that finds its own
// sessions by name in pg_stat_activity would never see a lock wait.
func NameSession(app string) func(context.Context, *pgx.Conn) error {
	return func(ctx context.Context, conn *pgx.Conn) error {
		if _, err := conn.Exec(ctx, fmt.Sprintf("SET application_name = '%s'", app)); err != nil {
			return fmt.Errorf("name the session %s: %w", app, err)
		}
		return nil
	}
}

// proxyRefusal is what the proxy answers a plain CREATE INDEX with. PostgreSQL
// builds that index, and nothing listens on a metadata-only cluster's port.
var proxyRefusal = regexp.MustCompile(`CREATE INDEX ASYNC`)

// ProveMiniStackStrictProxy returns nil only when dsn reaches MiniStack's
// strict SQL proxy with a working backend at DSQL's isolation level. It waits
// until ctx ends for a connection that answers a query, because a cluster's
// ACTIVE status does not say SQL works: MiniStack reports ACTIVE for a
// metadata-only cluster too, including one whose backend failed to start.
//
// A statement DSQL refuses must be refused over both wire protocols and a
// statement only DSQL accepts must succeed. A direct PostgreSQL endpoint fails
// on both counts, whatever port it is published on.
func ProveMiniStackStrictProxy(ctx context.Context, dsn string) error {
	conn, err := connectWhenAnswering(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()

	table := fmt.Sprintf("forma_strict_guard_%d", time.Now().UnixNano())
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (k INT PRIMARY KEY, v TEXT)", table)); err != nil {
		return fmt.Errorf("a statement DSQL supports failed, so this is no working SQL data plane: %w", err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = conn.Exec(cleanup, "DROP TABLE IF EXISTS "+table)
	}()

	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeCacheStatement, pgx.QueryExecModeSimpleProtocol} {
		if err := requireRefused(ctx, conn, table, mode); err != nil {
			return err
		}
	}
	if err := requireDSQLOnlyDDL(ctx, conn, table); err != nil {
		return err
	}
	if err := requireRoundTrip(ctx, conn, table); err != nil {
		return err
	}
	var isolation string
	if err := conn.QueryRow(ctx, "SHOW transaction_isolation").Scan(&isolation); err != nil {
		return fmt.Errorf("read the session's isolation level: %w", err)
	}
	if isolation != "repeatable read" {
		return fmt.Errorf("the backend runs at %q, not DSQL's fixed repeatable read: a guard-row conflict would wait and then commit instead of failing with 40001", isolation)
	}
	return nil
}

// connectWhenAnswering opens a connection that has answered a query, retrying
// until ctx ends. A published port with nothing behind it accepts and then
// drops the connection, so every failure is worth another attempt.
func connectWhenAnswering(ctx context.Context, dsn string) (*pgx.Conn, error) {
	var last error
	for {
		conn, err := pgx.Connect(ctx, dsn)
		if err == nil {
			var one int
			if err = conn.QueryRow(ctx, "SELECT 1").Scan(&one); err == nil {
				return conn, nil
			}
			_ = conn.Close(context.Background())
		}
		last = err
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("no SQL connection answered before the deadline, so the cluster has no data plane (a metadata-only MiniStack cluster reports ACTIVE with nothing listening): %w", errors.Join(ctx.Err(), last))
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// requireRefused checks that a plain CREATE INDEX on a real table is refused
// the way the proxy refuses it. The table exists so that PostgreSQL would
// succeed rather than fail for an unrelated reason.
func requireRefused(ctx context.Context, conn *pgx.Conn, table string, mode pgx.QueryExecMode) error {
	_, err := conn.Exec(ctx, fmt.Sprintf("CREATE INDEX %s_plain ON %s (v)", table, table), mode)
	if err == nil {
		return fmt.Errorf("plain CREATE INDEX succeeded (query mode %s): this is a direct PostgreSQL endpoint, not the strict SQL proxy", mode)
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "0A000" || !proxyRefusal.MatchString(pgErr.Message) {
		return fmt.Errorf("plain CREATE INDEX failed (query mode %s) but not with the strict proxy's 0A000 refusal: %w", mode, err)
	}
	return nil
}

// requireDSQLOnlyDDL builds an index with CREATE INDEX ASYNC and waits for its
// job. PostgreSQL has neither that statement nor sys.wait_for_job.
func requireDSQLOnlyDDL(ctx context.Context, conn *pgx.Conn, table string) error {
	var jobID string
	stmt := fmt.Sprintf("CREATE INDEX ASYNC %s_async ON %s (v)", table, table)
	if err := conn.QueryRow(ctx, stmt).Scan(&jobID); err != nil {
		return fmt.Errorf("CREATE INDEX ASYNC failed: this is no strict proxy, or it cannot run DSQL's DDL: %w", err)
	}
	if err := WaitForDSQLJob(ctx, conn, jobID); err != nil {
		return fmt.Errorf("the strict guard's index: %w", err)
	}
	return nil
}

// jobQuerier is what WaitForDSQLJob needs of a connection or a pool.
type jobQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// WaitForDSQLJob waits for an asynchronous DDL job and fails unless it
// completed.
func WaitForDSQLJob(ctx context.Context, q jobQuerier, jobID string) error {
	var done bool
	if err := q.QueryRow(ctx, "SELECT sys.wait_for_job($1)", jobID).Scan(&done); err != nil {
		return fmt.Errorf("wait for DDL job %s: %w", jobID, err)
	}
	if !done {
		return fmt.Errorf("DDL job %s did not complete", jobID)
	}
	return nil
}

// requireRoundTrip writes and reads a row through the proxy, so the answers
// above came from a data plane with a backend behind it.
func requireRoundTrip(ctx context.Context, conn *pgx.Conn, table string) error {
	if _, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s (k, v) VALUES ($1, $2)", table), 1, "proxied"); err != nil {
		return fmt.Errorf("insert through the proxy: %w", err)
	}
	var v string
	if err := conn.QueryRow(ctx, fmt.Sprintf("SELECT v FROM %s WHERE k = $1", table), 1).Scan(&v); err != nil {
		return fmt.Errorf("read back through the proxy: %w", err)
	}
	if v != "proxied" {
		return fmt.Errorf("read back %q through the proxy, wrote %q", v, "proxied")
	}
	return nil
}
