package federated

import (
	"context"
	"database/sql"
	"fmt"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/duckdbinit"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// StageDirtySet writes ids into the temp table the parquet-only template
// anti-joins against (sqlgen.StagedDirtyIDsTable), on conn. The template must
// run on the same connection: a temp table is visible only to the connection
// that created it, which is also what keeps concurrent passes apart. The
// table is created or replaced first, so a set staged by an earlier pass
// that failed before DropStagedDirtySet never leaks into this one.
//
// The Appender, not a bound UUID[] parameter, carries the set: plan step 1
// measured it at 14 ms against 128 ms for 100,000 IDs (#849), and the
// statement text stays the same whatever the set's size.
func StageDirtySet(ctx context.Context, conn *sql.Conn, ids []uuid.UUID) error {
	if _, err := conn.ExecContext(ctx, sqlgen.CreateStagedDirtyIDsSQL); err != nil {
		return fmt.Errorf("create staged dirty set %s: %w", sqlgen.StagedDirtyIDsTable, err)
	}
	if len(ids) == 0 {
		return nil
	}
	err := conn.Raw(func(driverConn any) error {
		app, err := duckdbinit.NewAppender(driverConn, "temp", "main", sqlgen.StagedDirtyIDsTable)
		if err != nil {
			return fmt.Errorf("open appender: %w", err)
		}
		for _, id := range ids {
			if err := app.AppendRow(duckdb.UUID(id)); err != nil {
				_ = app.Close()
				duckdbinit.ObserveRaw(driverConn, err)
				return fmt.Errorf("append dirty id %s: %w", id, err)
			}
		}
		if err := app.Close(); err != nil {
			duckdbinit.ObserveRaw(driverConn, err)
			return fmt.Errorf("flush appender: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("stage %d dirty ids into %s: %w", len(ids), sqlgen.StagedDirtyIDsTable, err)
	}
	return nil
}

// DropStagedDirtySet drops the staged dirty set at the end of a pass, so a
// pooled connection does not carry it into an unrelated statement.
func DropStagedDirtySet(ctx context.Context, conn *sql.Conn) error {
	if _, err := conn.ExecContext(ctx, sqlgen.DropStagedDirtyIDsSQL); err != nil {
		return fmt.Errorf("drop staged dirty set %s: %w", sqlgen.StagedDirtyIDsTable, err)
	}
	return nil
}
