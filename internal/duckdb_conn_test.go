package internal

import (
	"context"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

func TestRenderS3ParquetPath(t *testing.T) {
	tmpl := "s3://bucket/path/schema_{{.SchemaID}}/data.parquet"
	got, err := RenderS3ParquetPath(tmpl, 42)
	if err != nil {
		t.Fatalf("RenderS3ParquetPath error: %v", err)
	}
	want := "s3://bucket/path/schema_42/data.parquet"
	if got != want {
		t.Fatalf("unexpected path, got=%s want=%s", got, want)
	}
}

func TestGenerateDuckDBWhereClause_SimpleKv(t *testing.T) {
	q := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{
			Condition: &forma.KvCondition{
				Attr:  "username",
				Value: "equals:alice",
			},
		},
	}
	where, args, err := GenerateDuckDBWhereClause(q)
	if err != nil {
		t.Fatalf("GenerateDuckDBWhereClause error: %v", err)
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

	fq := &FederatedAttributeQuery{PreferHot: true}
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
	dec := EvaluateRoutingPolicy(cfg, nil, &FederatedQueryOptions{MaxRows: 500})
	require.True(t, dec.UseDuckDB) // remains enabled

	// Large scan should explicitly enable duckdb
	dec = EvaluateRoutingPolicy(cfg, nil, &FederatedQueryOptions{MaxRows: 100000})
	require.True(t, dec.UseDuckDB)
}
