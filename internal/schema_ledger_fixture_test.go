package internal

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma/internal/schemaledger"
	"github.com/lychee-technology/forma/internal/schemaledger/ledgertest"
	"github.com/stretchr/testify/require"
)

// seedSchemaLedger records generation 1 of every schema registered in
// registry from its declaration in dir, as init-db does for a schema it
// registers, so the gated registry and loader admit the fixture (#733).
func seedSchemaLedger(t *testing.T, ctx context.Context, pool *pgxpool.Pool, registry, dir string) {
	t.Helper()
	require.NoError(t, ledgertest.Seed(ctx, pool, registry, dir))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		tables := schemaledger.TablesFor(registry)
		_, _ = pool.Exec(cleanupCtx, "DROP TABLE IF EXISTS "+tables.Attribute)
		_, _ = pool.Exec(cleanupCtx, "DROP TABLE IF EXISTS "+tables.Generation)
	})
}

// applySchemaLedger records the declaration in dir as the next generation,
// as apply-schema does before a server restarts on edited files.
func applySchemaLedger(t *testing.T, ctx context.Context, pool *pgxpool.Pool, registry, dir string) {
	t.Helper()
	require.NoError(t, ledgertest.Apply(ctx, pool, registry, dir))
}
