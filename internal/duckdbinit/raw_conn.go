package duckdbinit

import (
	"database/sql/driver"
	"fmt"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

// NewAppender opens a duckdb-go Appender on driverConn, the connection
// database/sql hands to (*sql.Conn).Raw. duckdb-go's own constructor accepts
// only its *duckdb.Conn, which a RecoveringConnector wraps, so the wrapper
// is unwrapped first. The CDC export stages PostgreSQL rows through it
// (#785).
//
// The Appender bypasses ExecContext, so its errors are not watched for
// invalidation on their own: pass an error it returns to ObserveRaw.
func NewAppender(driverConn any, catalog, schema, table string) (*duckdb.Appender, error) {
	conn, err := unwrapRaw(driverConn)
	if err != nil {
		return nil, err
	}
	app, err := duckdb.NewAppender(conn, catalog, schema, table)
	if err != nil {
		ObserveRaw(driverConn, err)
		return nil, fmt.Errorf("open duckdb appender on %s.%s.%s: %w", catalog, schema, table, err)
	}
	return app, nil
}

// ObserveRaw watches err, returned by a driver API used through
// (*sql.Conn).Raw, for invalidation, as ExecContext and QueryContext do for
// their own errors. A connection the RecoveringConnector did not open is
// left alone.
func ObserveRaw(driverConn any, err error) {
	if c, ok := driverConn.(*instanceConn); ok {
		c.observe(err)
	}
}

func unwrapRaw(driverConn any) (driver.Conn, error) {
	switch c := driverConn.(type) {
	case *instanceConn:
		return c.duckConn, nil
	case *duckdb.Conn:
		return c, nil
	default:
		return nil, fmt.Errorf("duckdb appender needs a duckdb-go connection, got %T", driverConn)
	}
}
