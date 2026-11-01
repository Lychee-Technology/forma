package federated

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/lychee-technology/forma/internal/testdb"
	"github.com/stretchr/testify/require"
)

// The hot leg attaches PostgreSQL ahead of every query (#752). On a pool of
// several connections the first queries race to attach the same catalog, and
// DuckDB's ATTACH IF NOT EXISTS is not atomic: unserialized, all but one fail
// with "database with name ... already exists". The engine's attach path
// must let every query through to the one attachment.
func TestHotLegAttach_ConcurrentFirstQueriesShareOneCatalog(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool := testdb.Connect(t, ctx)
	table := "hot_leg_attach_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	qt := pgx.Identifier{table}.Sanitize()
	_, err := pool.Exec(ctx, "CREATE TABLE "+qt+" (n bigint); INSERT INTO "+qt+" SELECT generate_series(1, 5)")
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP TABLE "+qt) })

	duck, err := NewDuckDBClientContext(ctx, forma.DuckDBConfig{
		Enabled: true, MaxConnections: 4, QueryTimeout: 30 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = duck.Close() })

	conn := DuckDBPostgresConnStringFromPool(pool)
	engine := &DBFederatedQueryEngine{duck: NewDuckDBClientQueryExecutor(duck), pgConnString: conn}
	query := engine.withPostgresAttach("SELECT sum(n) FROM " +
		sqlgen.PostgresTableRef(sqlgen.PostgresCatalogAlias(conn), "public", table))

	const workers = 8
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			if err := engine.ensurePostgresAttached(ctx); err != nil {
				errs <- fmt.Errorf("worker %d: attach: %w", worker, err)
				return
			}
			var sum int64
			if err := duck.DB.QueryRowContext(ctx, query).Scan(&sum); err != nil {
				errs <- fmt.Errorf("worker %d: %w", worker, err)
				return
			}
			if sum != 15 {
				errs <- fmt.Errorf("worker %d: sum = %d, want 15", worker, sum)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// DuckDB's attach error quotes the connection string, password included;
// the engine's attach path must fail without it (#306, #752).
func TestHotLegAttach_FailureHidesThePassword(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool := testdb.Connect(t, ctx)
	duck, err := NewDuckDBClientContext(ctx, forma.DuckDBConfig{Enabled: true, MaxConnections: 1, QueryTimeout: 30 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = duck.Close() })

	const secret = "not-the-password-752"
	cfg := pool.Config().Copy()
	cfg.ConnConfig.Password = secret
	badPool, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(badPool.Close)

	engine := &DBFederatedQueryEngine{duck: NewDuckDBClientQueryExecutor(duck), pgConnString: DuckDBPostgresConnStringFromPool(badPool)}
	err = engine.ensurePostgresAttached(ctx)
	if err == nil {
		t.Skip("the integration PostgreSQL accepts any password, so no attach fails")
	}
	require.NotContains(t, err.Error(), secret)
}
