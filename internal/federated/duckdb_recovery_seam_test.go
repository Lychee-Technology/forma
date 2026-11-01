package federated

import (
	"context"
	"database/sql/driver"
	"io"
	"testing"

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
