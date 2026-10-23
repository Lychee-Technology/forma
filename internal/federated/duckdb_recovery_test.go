package federated

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/lychee-technology/forma/internal/duckdbinit"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// fakeDuck stands in for duckdb.NewConnector. Each open is one database
// instance, numbered by open attempt; a query named "id" answers with that
// number. "fatal" invalidates the instance and fails FATAL, "internal" fails
// INTERNAL and invalidates it, "internal-benign" fails INTERNAL and leaves it
// usable. Once invalidated, every query fails FATAL, as DuckDB's do.
type fakeDuck struct {
	opens     int
	failOpen  map[int]bool
	instances []*fakeInstance
}

func (f *fakeDuck) open() (driver.Connector, error) {
	f.opens++
	if f.failOpen[f.opens] {
		return nil, fmt.Errorf("open attempt %d failed", f.opens)
	}
	inst := &fakeInstance{id: f.opens}
	f.instances = append(f.instances, inst)
	return inst, nil
}

type fakeInstance struct {
	id          int
	invalidated atomic.Bool
	closed      atomic.Bool
}

func (i *fakeInstance) Connect(context.Context) (driver.Conn, error) {
	return &fakeDuckConn{inst: i}, nil
}
func (i *fakeInstance) Driver() driver.Driver { return duckdb.Driver{} }
func (i *fakeInstance) Close() error          { i.closed.Store(true); return nil }

type fakeDuckConn struct{ inst *fakeInstance }

func (c *fakeDuckConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if c.inst.invalidated.Load() {
		return nil, &duckdb.Error{Type: duckdb.ErrorTypeFatal, Msg: "FATAL Error: Failed: database has been invalidated"}
	}
	switch query {
	case "fatal":
		c.inst.invalidated.Store(true)
		return nil, &duckdb.Error{Type: duckdb.ErrorTypeFatal, Msg: "FATAL Error: boom"}
	case "internal":
		c.inst.invalidated.Store(true)
		return nil, &duckdb.Error{Type: duckdb.ErrorTypeInternal, Msg: "INTERNAL Error: index 0 of size 0"}
	case "internal-benign":
		return nil, &duckdb.Error{Type: duckdb.ErrorTypeInternal, Msg: "INTERNAL Error: recoverable"}
	}
	return &fakeIDRows{id: c.inst.id}, nil
}

func (c *fakeDuckConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if _, err := c.QueryContext(ctx, query, args); err != nil {
		return nil, err
	}
	return driver.RowsAffected(0), nil
}

func (c *fakeDuckConn) PrepareContext(context.Context, string) (driver.Stmt, error) {
	return nil, errors.New("fake: prepare unsupported")
}
func (c *fakeDuckConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("fake: prepare unsupported")
}
func (c *fakeDuckConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return nil, errors.New("fake: transactions unsupported")
}
func (c *fakeDuckConn) Begin() (driver.Tx, error) {
	return nil, errors.New("fake: transactions unsupported")
}
func (c *fakeDuckConn) CheckNamedValue(*driver.NamedValue) error { return nil }
func (c *fakeDuckConn) Close() error                             { return nil }

type fakeIDRows struct {
	id   int
	done bool
}

func (r *fakeIDRows) Columns() []string { return []string{"id"} }
func (r *fakeIDRows) Close() error      { return nil }
func (r *fakeIDRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0] = int64(r.id)
	return nil
}

func openRecoveringDB(t *testing.T, fake *fakeDuck, maxConns int) *sql.DB {
	t.Helper()
	connector, err := newRecoveringConnector(fake.open)
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(maxConns)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func queryInstanceID(t *testing.T, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) int {
	t.Helper()
	var id int
	require.NoError(t, q.QueryRowContext(context.Background(), "id").Scan(&id))
	return id
}

func TestRecoveringConnectorReopensAfterFatalError(t *testing.T) {
	fake := &fakeDuck{}
	db := openRecoveringDB(t, fake, 1)

	require.Equal(t, 1, queryInstanceID(t, db))
	_, err := db.ExecContext(context.Background(), "fatal")
	require.Error(t, err, "the statement that hit the error still fails")

	require.Equal(t, 2, queryInstanceID(t, db), "the next query must run on a reopened database")
	require.Equal(t, 2, queryInstanceID(t, db))
	require.True(t, fake.instances[0].closed.Load(), "the invalidated database is closed")
	require.Equal(t, 2, fake.opens)
}

func TestRecoveringConnectorReopensAfterInternalErrorThatInvalidates(t *testing.T) {
	fake := &fakeDuck{}
	db := openRecoveringDB(t, fake, 1)

	_, err := db.QueryContext(context.Background(), "internal")
	require.Error(t, err)

	require.Equal(t, 2, queryInstanceID(t, db))
}

func TestRecoveringConnectorKeepsDatabaseAfterInternalErrorThatDoesNotInvalidate(t *testing.T) {
	fake := &fakeDuck{}
	db := openRecoveringDB(t, fake, 1)

	_, err := db.QueryContext(context.Background(), "internal-benign")
	require.Error(t, err)

	require.Equal(t, 1, queryInstanceID(t, db), "a database that still answers SELECT 1 is kept")
	require.Equal(t, 1, fake.opens)
}

// An error on a connection of an instance that was already replaced must not
// invalidate the replacement.
func TestRecoveringConnectorIgnoresErrorsFromReplacedDatabase(t *testing.T) {
	ctx := context.Background()
	fake := &fakeDuck{}
	db := openRecoveringDB(t, fake, 3)

	first, err := db.Conn(ctx)
	require.NoError(t, err)
	stale, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stale.Close() })
	require.Equal(t, 1, queryInstanceID(t, stale))

	_, err = first.ExecContext(ctx, "fatal")
	require.Error(t, err)
	require.NoError(t, first.Close())
	require.Equal(t, 2, queryInstanceID(t, db))

	_, err = stale.ExecContext(ctx, "id")
	require.Error(t, err, "the replaced database stays invalid for its open connection")

	require.Equal(t, 2, queryInstanceID(t, db))
	require.Equal(t, 2, fake.opens, "a stale connection's error does not reopen the live database")
}

func TestRecoveringConnectorRetriesReopenAfterFailure(t *testing.T) {
	fake := &fakeDuck{failOpen: map[int]bool{2: true}}
	db := openRecoveringDB(t, fake, 1)

	_, err := db.ExecContext(context.Background(), "fatal")
	require.Error(t, err)

	_, err = db.QueryContext(context.Background(), "id")
	require.ErrorContains(t, err, "reopen invalidated duckdb database")

	require.Equal(t, 3, queryInstanceID(t, db), "a failed reopen is retried by the next connection")
}

// The reopened database is a new DuckDB instance that runs the same
// per-connection init hook.
func TestRecoveringConnectorReopensRealDuckDBWithConnectionInit(t *testing.T) {
	ctx := context.Background()
	steps := []duckdbinit.Step{duckdbinit.SingleStmtStep("PRAGMA threads=3;", "set threads")}
	connInit := duckdbinit.MakeConnInit(steps, zap.S(), duckdbinit.DefaultInitTimeout)
	connector, err := newRecoveringConnector(func() (driver.Connector, error) {
		return duckdb.NewConnector(":memory:", connInit)
	})
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.ExecContext(ctx, "CREATE TABLE marker AS SELECT 1 AS x")
	require.NoError(t, err)
	connector.cur.invalid.Store(true)

	var tables int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT count(*) FROM duckdb_tables() WHERE table_name = 'marker'").Scan(&tables))
	require.Zero(t, tables, "the query must run on a fresh database instance")

	var threads int64
	require.NoError(t, db.QueryRowContext(ctx, "SELECT current_setting('threads')").Scan(&threads))
	require.Equal(t, int64(3), threads, "the init hook runs on the reopened database's connections")
}
