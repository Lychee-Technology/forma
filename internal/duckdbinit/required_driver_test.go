package duckdbinit

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync/atomic"
	"testing"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// openRecovering opens a *sql.DB over a RecoveringConnector whose every
// instance runs the steps open returns for it: open(n) is called for the
// n-th database instance, 1 for the first.
func openRecovering(t *testing.T, steps func(instance int) []Step) (*sql.DB, *RecoveringConnector) {
	t.Helper()
	var opens atomic.Int32
	connector, err := NewRecoveringConnector(func() (driver.Connector, error) {
		n := int(opens.Add(1))
		return duckdb.NewConnector(":memory:", MakeConnInit(steps(n), zap.NewNop().Sugar(), DefaultInitTimeout))
	}, zap.NewNop().Sugar())
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close() })
	return db, connector
}

// A required extension that cannot be loaded fails the ping a constructor
// uses as its open check, instead of a ping on SELECT 1 succeeding over a
// connection that lacks it (#752).
func TestRequiredExtensionMissingFailsThePing(t *testing.T) {
	db, _ := openRecovering(t, func(int) []Step {
		return []Step{RequiredExtensionStep("parquet"), {
			Stmts:    []Stmt{{SQL: "LOAD forma_missing_ext_752;", Label: "load forma_missing_ext_752"}},
			Required: true,
		}}
	})
	err := db.PingContext(context.Background())
	var stepErr *RequiredStepError
	require.ErrorAs(t, err, &stepErr)
	require.Equal(t, "load forma_missing_ext_752", stepErr.Label)
}

// Every fresh physical connection runs and verifies the required steps,
// after pool churn and after DuckDB invalidated the database and the
// connector reopened it (#702, #717). A reopened instance whose required
// step fails fails the query that needed the connection.
func TestRequiredStepsRunOnEveryFreshConnectionAndAfterReopen(t *testing.T) {
	ctx := context.Background()
	db, connector := openRecovering(t, func(instance int) []Step {
		steps := []Step{
			RequiredExtensionStep("parquet"),
			SingleStmtStep("SET threads=3;", "set threads").AsRequired(),
		}
		if instance > 1 {
			steps = append(steps, Step{
				Stmts:    []Stmt{{SQL: "LOAD forma_missing_ext_752;", Label: "load forma_missing_ext_752"}},
				Required: true,
			})
		}
		return steps
	})
	db.SetMaxIdleConns(0) // every query opens a new physical connection

	for i := 0; i < 3; i++ {
		var threads int64
		var loaded bool
		require.NoError(t, db.QueryRowContext(ctx,
			"SELECT current_setting('threads'), (SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'parquet')").
			Scan(&threads, &loaded), "fresh connection %d", i)
		require.Equal(t, int64(3), threads)
		require.True(t, loaded)
	}

	connector.cur.invalid.Store(true)
	var one int
	err := db.QueryRowContext(ctx, "SELECT 1").Scan(&one)
	var stepErr *RequiredStepError
	require.ErrorAs(t, err, &stepErr, "the reopened database's connection must run the required steps")
	require.Equal(t, "load forma_missing_ext_752", stepErr.Label)
}
