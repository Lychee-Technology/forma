package factory

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/schemameta"
)

// TestNewEntityManagerWithConfig_CloseReleasesDuckDBClient pins the #302
// lifecycle fix: the DuckDB client the factory constructs is owned by the
// returned manager and released by Close — previously it leaked until
// process exit (mitigated in e2e only via MaxConnections=1).
func TestNewEntityManagerWithConfig_CloseReleasesDuckDBClient(t *testing.T) {
	t.Parallel()

	// Base fixture: same stubbed deps + config as
	// TestNewEntityManagerWithConfig_Unit_Success, with DuckDB enabled and
	// the client constructor capturing the real client it builds.
	var captured *federated.DuckDBClient
	deps := unitEntityManagerDeps(schemameta.NewMetadataCache())
	deps.newDuckDBClient = func(ctx context.Context, cfg forma.DuckDBConfig) (*federated.DuckDBClient, error) {
		c, err := federated.NewDuckDBClientContext(ctx, cfg)
		captured = c
		return c, err
	}
	config := unitEntityManagerConfig(t)
	config.DuckDB.Enabled = true
	config.DuckDB.MaxConnections = 1

	em, err := newEntityManagerWithConfigContext(context.Background(), config, nil, deps)
	if err != nil {
		t.Fatalf("build entity manager: %v", err)
	}
	if captured == nil || captured.DB == nil {
		t.Fatal("test wiring: DuckDB client was not constructed; the assertion below would be vacuous")
	}
	if err := captured.DB.Ping(); err != nil {
		t.Fatalf("DuckDB client must be live before Close: %v", err)
	}

	if err := em.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := captured.DB.Ping(); err == nil {
		t.Fatal("DuckDB client still answers Ping after manager Close; the factory did not register it")
	}
	if err := em.Close(); err != nil {
		t.Fatalf("second Close must be a no-op: %v", err)
	}
}
