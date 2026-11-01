package federated

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"database/sql/driver"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/lychee-technology/forma/internal/testdb"
	"github.com/stretchr/testify/require"
)

// hotLegTable creates a PostgreSQL table holding n = 1..5 for the test and
// returns its name.
func hotLegTable(t *testing.T, ctx context.Context, pool *pgxpool.Pool) string {
	t.Helper()
	table := "hot_leg_attach_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	qt := pgx.Identifier{table}.Sanitize()
	_, err := pool.Exec(ctx, "CREATE TABLE "+qt+" (n bigint); INSERT INTO "+qt+" SELECT generate_series(1, 5)")
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP TABLE "+qt) })
	return table
}

// hotLegSum runs the engine's hot-leg query path over query and returns its
// one value.
func hotLegSum(ctx context.Context, engine *DBFederatedQueryEngine, query string) (int64, error) {
	rows, err := engine.queryDuckDB(ctx, query, nil)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, fmt.Errorf("no row: %v", rows.Err())
	}
	var sum int64
	if err := rows.Scan(&sum); err != nil {
		return 0, err
	}
	return sum, rows.Close()
}

// runHotLegWave runs workers concurrent hot-leg queries and reports every
// one that failed or summed wrong.
func runHotLegWave(t *testing.T, ctx context.Context, engine *DBFederatedQueryEngine, query string, workers int) {
	t.Helper()
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			sum, err := hotLegSum(ctx, engine, query)
			if err != nil {
				errs <- fmt.Errorf("worker %d: %w", worker, err)
			} else if sum != 15 {
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

// The hot leg attaches PostgreSQL ahead of every query (#752). On a pool of
// several connections the first queries race to attach the same catalog, and
// DuckDB's ATTACH IF NOT EXISTS is not atomic: unserialized, all but one fail
// with "database with name ... already exists". The engine's attach path
// must let every query through to the one attachment.
func TestHotLegAttach_ConcurrentFirstQueriesShareOneCatalog(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool := testdb.Connect(t, ctx)
	table := hotLegTable(t, ctx, pool)

	duck, err := NewFederatedDuckDBClientContext(ctx, forma.DuckDBConfig{
		Enabled: true, MaxConnections: 4, QueryTimeout: 30 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = duck.Close() })

	conn := DuckDBPostgresConnStringFromPool(pool)
	engine := &DBFederatedQueryEngine{duck: NewDuckDBClientQueryExecutor(duck), pgConnString: conn}
	runHotLegWave(t, ctx, engine, "SELECT sum(n) FROM "+
		sqlgen.PostgresTableRef(sqlgen.PostgresCatalogAlias(conn), "public", table), 8)
}

// A database DuckDB invalidates while requests sit between their attach and
// their query must not leave those requests, or the ones after them, to
// attach the reopened database unguarded and at once (#702, #752). Every
// worker of the first wave attaches on the first database and waits in that
// window; the test invalidates the database and has the connector reopen
// it before letting them go. The first wave's queries run on the database
// their attach reached, and the second wave's, all first queries of the
// reopened database, attach it one at a time.
func TestHotLegAttach_ReopenBetweenAttachAndQueryKeepsAttachesGuarded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool := testdb.Connect(t, ctx)
	table := hotLegTable(t, ctx, pool)

	var opens atomic.Int32
	swapOpenDuckDBConnector(t, func(dsn string, init func(driver.ExecerContext) error) (driver.Connector, error) {
		opens.Add(1)
		return openFatalInjecting(dsn, init)
	})
	const workers = 6
	duck, err := NewFederatedDuckDBClientContext(ctx, forma.DuckDBConfig{
		Enabled: true, MaxConnections: workers + 1, QueryTimeout: 30 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = duck.Close() })
	conn := DuckDBPostgresConnStringFromPool(pool)
	engine := &DBFederatedQueryEngine{duck: NewDuckDBClientQueryExecutor(duck), pgConnString: conn}
	query := "SELECT sum(n) FROM " + sqlgen.PostgresTableRef(sqlgen.PostgresCatalogAlias(conn), "public", table)

	var attached sync.WaitGroup
	attached.Add(workers)
	release := make(chan struct{})
	afterPostgresAttach = func() {
		attached.Done()
		<-release
	}
	t.Cleanup(func() { afterPostgresAttach = nil })

	waveDone := make(chan struct{})
	go func() {
		defer close(waveDone)
		runHotLegWave(t, ctx, engine, query, workers)
	}()
	attached.Wait()

	_, err = duck.DB.ExecContext(ctx, injectFatalSQL)
	require.Error(t, err)
	require.NoError(t, duck.DB.PingContext(ctx), "a connection of the reopened database")
	require.Equal(t, int32(2), opens.Load(), "the database was reopened inside the window")
	close(release)
	<-waveDone
	afterPostgresAttach = nil

	runHotLegWave(t, ctx, engine, query, workers)
	require.Equal(t, int32(2), opens.Load(), "the second wave ran on the reopened database")
}

// DuckDB's attach error quotes the connection string, password included;
// the engine's attach path must fail without it (#306, #752).
func TestHotLegAttach_FailureHidesThePassword(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool := testdb.Connect(t, ctx)
	duck, err := NewFederatedDuckDBClientContext(ctx, forma.DuckDBConfig{Enabled: true, MaxConnections: 1, QueryTimeout: 30 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = duck.Close() })

	const secret = "not-the-password-752"
	cfg := pool.Config().Copy()
	cfg.ConnConfig.Password = secret
	badPool, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(badPool.Close)

	engine := &DBFederatedQueryEngine{duck: NewDuckDBClientQueryExecutor(duck), pgConnString: DuckDBPostgresConnStringFromPool(badPool)}
	_, err = hotLegSum(ctx, engine, "SELECT 1")
	if err == nil {
		t.Skip("the integration PostgreSQL accepts any password, so no attach fails")
	}
	require.NotContains(t, err.Error(), secret)
}
