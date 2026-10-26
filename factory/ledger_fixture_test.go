package factory

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma/internal/schemaledger"
	"github.com/lychee-technology/forma/internal/schemaledger/ledgertest"
	"github.com/stretchr/testify/require"
)

// seedLedger records generation 1 of every schema registered in registry
// from its declaration in dir, as init-db does for a schema it registers, so
// the gated loaders admit the fixture (#733).
func seedLedger(t *testing.T, ctx context.Context, pool *pgxpool.Pool, registry, dir string) {
	t.Helper()
	require.NoError(t, ledgertest.Seed(ctx, pool, registry, dir))
	t.Cleanup(func() {
		tables := schemaledger.TablesFor(registry)
		_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+tables.Attribute)
		_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+tables.Generation)
	})
}
