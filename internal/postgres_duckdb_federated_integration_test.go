package internal

import (
	"context"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// Tests covering federated routing and basic DuckDB execution path handling.

func TestEvaluateRoutingPolicy_VariousStrategies(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled: true,
		Routing: forma.RoutingPolicy{
			Strategy:          "hybrid",
			MaxDuckDBScanRows: 5000,
			AllowS3Fallback:   true,
		},
	}

	// hybrid default => use duckdb
	dec := EvaluateRoutingPolicy(cfg, nil, nil)
	require.True(t, dec.UseDuckDB, "hybrid should use duckdb by default")

	// prefer hot via query hint
	fq := &FederatedAttributeQuery{PreferHot: true}
	dec = EvaluateRoutingPolicy(cfg, fq, nil)
	require.False(t, dec.UseDuckDB, "PreferHot should disable duckdb")

	// cost-first with large MaxRows should prefer duckdb
	cfg.Routing.Strategy = "cost-first"
	dec = EvaluateRoutingPolicy(cfg, nil, &FederatedQueryOptions{MaxRows: 100000})
	require.True(t, dec.UseDuckDB, "cost-first large scan should enable duckdb")

	// disabled globally
	cfg.Enabled = false
	dec = EvaluateRoutingPolicy(cfg, nil, nil)
	require.False(t, dec.UseDuckDB, "disabled config should not use duckdb")
}

func TestExecuteDuckDBFederatedQuery_ClientUnavailable(t *testing.T) {
	env := setupIntegrationEnv(t)

	// Ensure no global DuckDB client is set
	repo := NewDBPersistentRecordRepository(env.postgresPool, env.metadata, nil)

	// Build a minimal federated query
	q := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{
			SchemaID: 100,
			Limit:    10,
			Offset:   0,
		},
	}

	// Call should error when DuckDB client not available
	_, _, err := repo.ExecuteDuckDBFederatedQuery(context.Background(), env.tables, q, q.Limit, q.Offset, nil, nil)
	require.Error(t, err)
}

// A small smoke test to exercise DuckDB client creation and health check.
func TestNewDuckDBClient_HealthCheck(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled:        true,
		DBPath:         ":memory:",
		MemoryLimitMB:  16,
		EnableParquet:  false,
		Extensions:     []string{},
		MaxConnections: 1,
		QueryTimeout:   5 * time.Second,
		MaxParallelism: 1,
		Routing: forma.RoutingPolicy{
			Strategy:          "hybrid",
			MaxDuckDBScanRows: 1000,
			AllowS3Fallback:   true,
		},
	}

	duck, err := NewDuckDBClient(cfg)
	require.NoError(t, err)
	defer duck.Close()

	// Basic health check
	require.NoError(t, duck.HealthCheck(context.Background()))
}
