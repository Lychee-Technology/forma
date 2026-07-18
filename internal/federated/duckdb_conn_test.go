package federated

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/stretchr/testify/require"
)

func TestRenderS3ParquetPath(t *testing.T) {
	tmpl := "s3://bucket/path/schema_{{.SchemaID}}/data.parquet"
	got, err := sqlgen.RenderS3ParquetPath(tmpl, 42)
	if err != nil {
		t.Fatalf("sqlgen.RenderS3ParquetPath error: %v", err)
	}
	want := "s3://bucket/path/schema_42/data.parquet"
	if got != want {
		t.Fatalf("unexpected path, got=%s want=%s", got, want)
	}
}

func TestGenerateDuckDBWhereClause_SimpleKv(t *testing.T) {
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{
			Condition: &forma.KvCondition{
				Attr:  "username",
				Value: "equals:alice",
			},
		},
	}
	where, args, err := sqlgen.BuildDuckClause(q.Condition, nil)
	if err != nil {
		t.Fatalf("sqlgen.BuildDuckClause error: %v", err)
	}
	if where != "username = ?" {
		t.Fatalf("unexpected where clause: %s", where)
	}
	if len(args) != 1 || args[0] != "alice" {
		t.Fatalf("unexpected args: %#v", args)
	}
}

func TestNewDuckDBClient_Disabled(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled:       false,
		DBPath:        ":memory:",
		MemoryLimitMB: 0,
	}
	_, err := NewDuckDBClient(cfg)
	if err == nil {
		t.Fatalf("expected error when duckdb disabled, got nil")
	}
}

// ============================================================================
// TC-5: DuckDB Client Lifecycle Tests
// ============================================================================

func TestNewDuckDBClient_InMemoryMode(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled:        true,
		DBPath:         ":memory:",
		MemoryLimitMB:  256,
		MaxParallelism: 1,
		MaxConnections: 1,
		QueryTimeout:   5 * time.Second,
	}

	duck, err := NewDuckDBClient(cfg)
	require.NoError(t, err)
	require.NotNil(t, duck)
	require.NotNil(t, duck.DB)
	defer duck.Close()
}

func TestNewDuckDBClientContext_Canceled(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled:        true,
		DBPath:         ":memory:",
		MemoryLimitMB:  256,
		MaxParallelism: 1,
		MaxConnections: 1,
		QueryTimeout:   5 * time.Second,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := NewDuckDBClientContext(ctx, cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ping duckdb")
}

func TestNewDuckDBClientContext_DeadlineExceeded(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled:        true,
		DBPath:         ":memory:",
		MemoryLimitMB:  256,
		MaxParallelism: 1,
		MaxConnections: 1,
		QueryTimeout:   5 * time.Second,
	}

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	_, err := NewDuckDBClientContext(ctx, cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ping duckdb")
}

// The ping issued during construction opens (and thereby initializes) the first
// pooled connection, so a freshly constructed client must already expose the S3
// session settings and extensions on that connection.
func TestNewDuckDBClientContext_FirstConnectionConfigured(t *testing.T) {
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
	require.NoError(t, makeConnInit(steps)(execer))

	require.Contains(t, execer.executed, "INSTALL bad_ext;")
	require.NotContains(t, execer.executed, "LOAD bad_ext;")
	require.Contains(t, execer.executed, "LOAD httpfs;", "later steps must still run after a failed step")
	require.Contains(t, execer.executed, "SET s3_region='us-test-1';")
}

func TestNewDuckDBClient_DisabledConfig(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled: false,
	}

	_, err := NewDuckDBClient(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "disabled in config")
}

func TestDuckDBClientHealthCheck_Success(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled:        true,
		DBPath:         ":memory:",
		MemoryLimitMB:  256,
		MaxParallelism: 1,
		MaxConnections: 1,
		QueryTimeout:   5 * time.Second,
	}

	duck, err := NewDuckDBClient(cfg)
	require.NoError(t, err)
	defer duck.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err = duck.HealthCheck(ctx)
	require.NoError(t, err)
}

func TestDuckDBClientHealthCheck_MemoryLimit(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled:        true,
		DBPath:         ":memory:",
		MemoryLimitMB:  512,
		MaxParallelism: 1,
		MaxConnections: 1,
		QueryTimeout:   5 * time.Second,
	}

	duck, err := NewDuckDBClient(cfg)
	require.NoError(t, err)
	defer duck.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err = duck.HealthCheck(ctx)
	require.NoError(t, err)
}

func TestDuckDBClientHealthCheck_Parallelism(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled:        true,
		DBPath:         ":memory:",
		MemoryLimitMB:  256,
		MaxParallelism: 4,
		MaxConnections: 1,
		QueryTimeout:   5 * time.Second,
	}

	duck, err := NewDuckDBClient(cfg)
	require.NoError(t, err)
	defer duck.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err = duck.HealthCheck(ctx)
	require.NoError(t, err)
}

func TestDuckDBClientClose(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled:        true,
		DBPath:         ":memory:",
		MemoryLimitMB:  256,
		MaxParallelism: 1,
		MaxConnections: 1,
		QueryTimeout:   5 * time.Second,
	}

	duck, err := NewDuckDBClient(cfg)
	require.NoError(t, err)

	err = duck.Close()
	require.NoError(t, err)
}

func TestDuckDBClientCloseNil(t *testing.T) {
	var duck *DuckDBClient
	err := duck.Close()
	require.NoError(t, err)
}

func TestValidateDuckDBConfig_AllValid(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled:        true,
		DBPath:         ":memory:",
		MemoryLimitMB:  256,
		MaxParallelism: 2,
		MaxConnections: 4,
		QueryTimeout:   5 * time.Second,
	}

	err := ValidateDuckDBConfig(cfg)
	require.NoError(t, err)
}

func TestValidateDuckDBConfig_EmptyDBPath(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled:        true,
		DBPath:         "", // Empty is OK (defaults to in-memory)
		MemoryLimitMB:  256,
		MaxParallelism: 2,
		MaxConnections: 1,
		QueryTimeout:   5 * time.Second,
	}

	err := ValidateDuckDBConfig(cfg)
	require.NoError(t, err)
}

// ============================================================================
// TC-4: Routing Policy Tests (Enhanced)
// ============================================================================

func TestEvaluateRoutingPolicy_HybridDefault(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled: true,
		Routing: forma.RoutingPolicy{
			Strategy:          forma.RoutingStrategyHybrid,
			MaxDuckDBScanRows: 5000,
			AllowS3Fallback:   true,
		},
	}

	dec := EvaluateRoutingPolicy(cfg, nil, nil)
	require.True(t, dec.UseDuckDB)
}

func TestEvaluateRoutingPolicy_PreferHotOverride(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled: true,
		Routing: forma.RoutingPolicy{
			Strategy:          forma.RoutingStrategyHybrid,
			MaxDuckDBScanRows: 5000,
		},
	}

	fq := &model.FederatedAttributeQuery{PreferHot: true}
	dec := EvaluateRoutingPolicy(cfg, fq, nil)
	require.False(t, dec.UseDuckDB)
}

func TestEvaluateRoutingPolicy_GlobalDisabled(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled: false,
	}

	dec := EvaluateRoutingPolicy(cfg, nil, nil)
	require.False(t, dec.UseDuckDB)
}

func TestEvaluateRoutingPolicy_CostFirstStrategy(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled: true,
		Routing: forma.RoutingPolicy{
			Strategy:          forma.RoutingStrategyCostFirst,
			MaxDuckDBScanRows: 1000,
		},
	}

	// Small scan: cost-first doesn't explicitly disable duckdb for small rows
	// (so it stays enabled by default)
	dec := EvaluateRoutingPolicy(cfg, nil, &model.FederatedQueryOptions{MaxRows: 500})
	require.True(t, dec.UseDuckDB) // remains enabled

	// Large scan should explicitly enable duckdb
	dec = EvaluateRoutingPolicy(cfg, nil, &model.FederatedQueryOptions{MaxRows: 100000})
	require.True(t, dec.UseDuckDB)
}

func TestValidateDuckDBConfig_InvalidMemoryLimit(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled:       true,
		MemoryLimitMB: -1,
	}

	err := ValidateDuckDBConfig(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid memory_limit_mb")
}

func TestValidateDuckDBConfig_InvalidParallelism(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled:        true,
		MemoryLimitMB:  256,
		MaxParallelism: -1,
	}

	err := ValidateDuckDBConfig(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid max_parallelism")
}

func TestValidateDuckDBConfig_InvalidMaxConnections(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled:        true,
		MemoryLimitMB:  256,
		MaxParallelism: 2,
		MaxConnections: 0,
	}

	err := ValidateDuckDBConfig(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "max_connections must be >= 1")
}

func TestValidateDuckDBConfig_InvalidQueryTimeout(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled:        true,
		MemoryLimitMB:  256,
		MaxParallelism: 2,
		MaxConnections: 1,
		QueryTimeout:   0,
	}

	err := ValidateDuckDBConfig(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "query_timeout must be > 0")
}

func TestValidateDuckDBConfig_DisabledIsValid(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled: false,
	}

	err := ValidateDuckDBConfig(cfg)
	require.NoError(t, err)
}

func TestValidateDuckDBConfig_ValidConfig(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled:        true,
		MemoryLimitMB:  256,
		MaxParallelism: 2,
		MaxConnections: 1,
		QueryTimeout:   5 * time.Second,
		DBPath:         ":memory:",
	}

	err := ValidateDuckDBConfig(cfg)
	require.NoError(t, err)
}
