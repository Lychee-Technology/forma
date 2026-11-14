package federated

import (
	"context"
	"database/sql/driver"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

// injectFatalSQL is the statement a fatalInjectingConn fails with a FATAL
// DuckDB error, the error after which DuckDB has invalidated the database
// and the recovering connector reopens it (#702).
const injectFatalSQL = "SELECT 'forma_test_inject_fatal'"

// duckDriverConn is the driver surface of a DuckDB connection that the
// recovering connector requires.
type duckDriverConn interface {
	driver.Conn
	driver.QueryerContext
	driver.ExecerContext
	driver.ConnPrepareContext
	driver.ConnBeginTx
	driver.NamedValueChecker
}

// fatalInjectingConnector wraps a real DuckDB connector so a test can
// invalidate its database on demand: injectFatalSQL fails FATAL on its
// connections, and every other statement runs on the real database.
type fatalInjectingConnector struct {
	driver.Connector
}

func (c fatalInjectingConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return fatalInjectingConn{duckDriverConn: conn.(duckDriverConn)}, nil
}

func (c fatalInjectingConnector) Close() error {
	if closer, ok := c.Connector.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

type fatalInjectingConn struct {
	duckDriverConn
}

func (c fatalInjectingConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if query == injectFatalSQL {
		return nil, &duckdb.Error{Type: duckdb.ErrorTypeFatal, Msg: "FATAL Error: injected by the test"}
	}
	return c.duckDriverConn.ExecContext(ctx, query, args)
}

// swapOpenDuckDBConnector makes the DuckDB clients the test constructs open
// every database, the first and each reopen, through open.
func swapOpenDuckDBConnector(t *testing.T, open func(dsn string, init func(driver.ExecerContext) error) (driver.Connector, error)) {
	t.Helper()
	prev := openDuckDBConnector
	openDuckDBConnector = open
	t.Cleanup(func() { openDuckDBConnector = prev })
}

// openFatalInjecting opens a real DuckDB database wrapped in a
// fatalInjectingConnector.
func openFatalInjecting(dsn string, init func(driver.ExecerContext) error) (driver.Connector, error) {
	c, err := duckdb.NewConnector(dsn, init)
	if err != nil {
		return nil, err
	}
	return fatalInjectingConnector{Connector: c}, nil
}

// Every fresh physical connection of the engine's client runs the init
// steps, and so does a connection of a database the recovering connector
// reopened after a FATAL error (#702, #752). No connection, fresh or
// reopened, loads the postgres_scanner extension (#858).
func TestNewDuckDBClient_InitOnFreshAndReopenedConnections(t *testing.T) {
	useTestGlobalLogger(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var opens atomic.Int32
	swapOpenDuckDBConnector(t, func(dsn string, init func(driver.ExecerContext) error) (driver.Connector, error) {
		opens.Add(1)
		return openFatalInjecting(dsn, init)
	})

	cfg := newS3ConfiguredDuckDBConfig()
	cfg.MaxConnections = 2
	duck, err := NewDuckDBClientContext(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = duck.Close() })
	duck.DB.SetMaxIdleConns(0) // every query opens a new physical connection

	requireInitialized := func(what string) {
		t.Helper()
		var region string
		require.NoError(t, duck.DB.QueryRowContext(ctx, "SELECT current_setting('s3_region')").Scan(&region), what)
		require.Equal(t, "us-test-1", region, what)
		var scanner int
		require.NoError(t, duck.DB.QueryRowContext(ctx,
			"SELECT count(*) FROM duckdb_extensions() WHERE extension_name = 'postgres_scanner' AND loaded").Scan(&scanner), what)
		require.Zero(t, scanner, "%s: postgres_scanner must not be loaded", what)
	}
	for i := 0; i < 3; i++ {
		requireInitialized("fresh connection")
	}

	_, err = duck.DB.ExecContext(ctx, injectFatalSQL)
	require.Error(t, err)
	requireInitialized("connection of the reopened database")
	require.Equal(t, int32(2), opens.Load(), "the query reached a reopened database")
}
