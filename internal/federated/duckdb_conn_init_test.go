package federated

// Per-connection initialization tests (issues #245/#285): session-scoped
// SET/INSTALL/LOAD must reach every pooled physical connection via the
// connector init hook, with the log-and-skip failure contract.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/duckdbinit"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

// useTestGlobalLogger routes zap's globals into the test log for the test's
// duration: NewDuckDBClientContext's connection init logs through zap.S(), and
// init is fail-open — a degraded init (e.g. an INSTALL timing out) surfaces
// only as a logged warning. Without this, a failure shows just the downstream
// NULL-scan error with zero trace of the real cause (#487).
func useTestGlobalLogger(t *testing.T) {
	t.Cleanup(zap.ReplaceGlobals(zaptest.NewLogger(t)))
}

// The ping issued during construction opens (and thereby initializes) the first
// pooled connection, so a freshly constructed client must already expose the S3
// session settings and extensions on that connection.
func TestNewDuckDBClientContext_FirstConnectionConfigured(t *testing.T) {
	useTestGlobalLogger(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	duck, err := NewDuckDBClientContext(ctx, newS3ConfiguredDuckDBConfig())
	require.NoError(t, err)
	require.NotNil(t, duck)
	defer duck.Close()

	var region string
	require.NoError(t, duck.DB.QueryRowContext(ctx, "SELECT current_setting('s3_region');").Scan(&region))
	require.Equal(t, "us-test-1", region)

	var loaded int
	require.NoError(t, duck.DB.QueryRowContext(ctx,
		"SELECT count(*) FROM duckdb_extensions() WHERE extension_name IN ('httpfs','parquet') AND loaded;").Scan(&loaded))
	require.Equal(t, 2, loaded)
}

// Invalid S3 credentials must fail construction (before any connection opens), not
// surface as per-connection init warnings.
func TestNewDuckDBClientContext_InvalidS3CredentialFailsFast(t *testing.T) {
	cfg := newS3ConfiguredDuckDBConfig()
	cfg.S3SecretKey = "bad'key"

	_, err := NewDuckDBClient(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "forbidden character")
}

// newS3ConfiguredDuckDBConfig returns a multi-connection S3-enabled config for the
// issue #245 tests. SET statements never touch the network, so no real S3
// endpoint is required to observe per-connection session settings.
func newS3ConfiguredDuckDBConfig() forma.DuckDBConfig {
	return forma.DuckDBConfig{
		Enabled:        true,
		DBPath:         ":memory:",
		EnableS3:       true,
		EnableParquet:  true,
		S3Region:       "us-test-1",
		S3Endpoint:     "127.0.0.1:9000",
		S3AccessKey:    "test-access-key",
		S3SecretKey:    "test-secret-key",
		MemoryLimitMB:  256,
		MaxParallelism: 2,
		MaxConnections: 4,
		QueryTimeout:   5 * time.Second,
	}
}

// Issue #245: session-scoped SET statements (s3_region etc.) must reach every pooled
// connection, not just the one that happened to serve the configuration calls.
func TestNewDuckDBClientContext_AllConnectionsConfigured(t *testing.T) {
	useTestGlobalLogger(t)
	duck, err := NewDuckDBClient(newS3ConfiguredDuckDBConfig())
	require.NoError(t, err)
	defer duck.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Hold the first physical connection while checking out a second, forcing the
	// pool to open a fresh, lazily initialized connection.
	conns := make([]*sql.Conn, 0, 2)
	defer func() {
		for _, conn := range conns {
			_ = conn.Close()
		}
	}()
	for i := 0; i < 2; i++ {
		conn, err := duck.DB.Conn(ctx)
		require.NoError(t, err, "checkout pooled connection %d", i)
		conns = append(conns, conn)
	}

	for i, conn := range conns {
		var region string
		err := conn.QueryRowContext(ctx, "SELECT current_setting('s3_region');").Scan(&region)
		require.NoError(t, err, "connection %d: query s3_region", i)
		require.Equal(t, "us-test-1", region, "connection %d: s3_region not configured", i)
	}
}

// Issue #245: mirrors the real failure shape — concurrent federated queries spread
// across the pool must all see the configured S3 session settings.
func TestNewDuckDBClientContext_ConcurrentConnectionsConfigured(t *testing.T) {
	useTestGlobalLogger(t)
	duck, err := NewDuckDBClient(newS3ConfiguredDuckDBConfig())
	require.NoError(t, err)
	defer duck.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const workers = 8
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			var region string
			if err := duck.DB.QueryRowContext(ctx, "SELECT current_setting('s3_region');").Scan(&region); err != nil {
				errs <- fmt.Errorf("worker %d: query s3_region: %w", worker, err)
				return
			}
			if region != "us-test-1" {
				errs <- fmt.Errorf("worker %d: s3_region = %q, want %q", worker, region, "us-test-1")
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// recordingExecer is a driver.ExecerContext fake that records every attempted
// statement and fails the one matching failOn.
type recordingExecer struct {
	executed []string
	failOn   string
}

func (r *recordingExecer) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	r.executed = append(r.executed, query)
	if query == r.failOn {
		return nil, errors.New("injected init failure")
	}
	return driver.RowsAffected(0), nil
}

// A failed INSTALL must skip that extension's LOAD (the pre-#245 log-and-skip
// contract), while later init steps still run.
func TestMakeConnInit_FailedInstallSkipsLoad(t *testing.T) {
	cfg := newS3ConfiguredDuckDBConfig()
	cfg.Extensions = []string{"bad_ext"}

	steps, err := buildInitSteps(cfg)
	require.NoError(t, err)

	execer := &recordingExecer{failOn: "INSTALL bad_ext;"}
	require.NoError(t, duckdbinit.MakeConnInit(steps, zap.NewNop().Sugar(), duckdbinit.DefaultInitTimeout)(execer))

	require.Contains(t, execer.executed, "INSTALL bad_ext;")
	require.NotContains(t, execer.executed, "LOAD bad_ext;")
	require.Contains(t, execer.executed, "LOAD httpfs;", "later steps must still run after a failed step")
	require.Contains(t, execer.executed, "SET s3_region='us-test-1';")
}
