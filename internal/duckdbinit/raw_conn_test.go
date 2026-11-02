package duckdbinit

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// The recovering connector wraps every DuckDB connection, and duckdb-go's
// Appender accepts only its own: NewAppender unwraps the connection a pooled
// *sql.Conn hands to Raw, so the CDC export can stage rows into a temp table
// on the pool's connection (#785).
func TestNewAppender_UnwrapsRecoveringConnection(t *testing.T) {
	ctx := context.Background()
	connector, err := NewRecoveringConnector(func() (driver.Connector, error) {
		return duckdb.NewConnector("", nil)
	}, zap.NewNop().Sugar())
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close() })
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	_, err = conn.ExecContext(ctx, "CREATE TEMP TABLE staged (id INTEGER, name VARCHAR)")
	require.NoError(t, err)
	require.NoError(t, conn.Raw(func(driverConn any) error {
		app, err := NewAppender(driverConn, "temp", "main", "staged")
		if err != nil {
			return err
		}
		if err := app.AppendRow(int32(1), "a"); err != nil {
			return err
		}
		if err := app.AppendRow(int32(2), nil); err != nil {
			return err
		}
		return app.Close()
	}))
	var rows, names int
	require.NoError(t, conn.QueryRowContext(ctx, "SELECT count(*), count(name) FROM temp.main.staged").Scan(&rows, &names))
	require.Equal(t, []int{2, 1}, []int{rows, names})

	_, err = NewAppender(struct{}{}, "temp", "main", "staged")
	require.ErrorContains(t, err, "needs a duckdb-go connection")
}
