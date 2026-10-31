package cdc

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// fatalStatement is the export statement the stubbed DuckDB fails FATAL on,
// invalidating its database as a real FATAL error does.
const fatalStatement = "COPY (SELECT 'fatal') TO 'unreachable'"

// invalidatingDuckConn is the driver surface the recovering connector
// requires of a DuckDB connection.
type invalidatingDuckConn interface {
	driver.Conn
	driver.QueryerContext
	driver.ExecerContext
	driver.ConnPrepareContext
	driver.ConnBeginTx
	driver.NamedValueChecker
}

// invalidatingInstance is a real DuckDB database that fails fatalStatement
// FATAL and, from then on, every statement on any of its connections, the
// way DuckDB behaves once it has invalidated a database. No SQL reaches a
// real FATAL error on the pinned DuckDB, so the error is injected here.
type invalidatingInstance struct {
	driver.Connector
	invalidated atomic.Bool
	closed      atomic.Bool
}

func (i *invalidatingInstance) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := i.Connector.Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("connect stub duckdb: %w", err)
	}
	dc, ok := conn.(invalidatingDuckConn)
	if !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("duckdb connection %T lacks the context-aware driver interfaces", conn)
	}
	return &invalidatingConn{invalidatingDuckConn: dc, inst: i}, nil
}

func (i *invalidatingInstance) Close() error {
	i.closed.Store(true)
	if closer, ok := i.Connector.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

type invalidatingConn struct {
	invalidatingDuckConn
	inst *invalidatingInstance
}

func (c *invalidatingConn) fault(query string) error {
	if c.inst.invalidated.Load() {
		return &duckdb.Error{Type: duckdb.ErrorTypeFatal, Msg: "FATAL Error: Failed: database has been invalidated because of a previous fatal error"}
	}
	if query == fatalStatement {
		c.inst.invalidated.Store(true)
		return &duckdb.Error{Type: duckdb.ErrorTypeFatal, Msg: "FATAL Error: injected"}
	}
	return nil
}

func (c *invalidatingConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := c.fault(query); err != nil {
		return nil, err
	}
	return c.invalidatingDuckConn.QueryContext(ctx, query, args)
}

func (c *invalidatingConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if err := c.fault(query); err != nil {
		return nil, err
	}
	return c.invalidatingDuckConn.ExecContext(ctx, query, args)
}

// stubInvalidatingDuckConnector routes the exporter's database opens to real
// DuckDB instances that can be invalidated, and returns an accessor for every
// instance opened so far. No t.Parallel: the seam is process-global.
func stubInvalidatingDuckConnector(t *testing.T) func() []*invalidatingInstance {
	t.Helper()
	previous := newDuckConnectorFn
	var mu sync.Mutex
	var opened []*invalidatingInstance
	newDuckConnectorFn = func(dsn string, connInit func(driver.ExecerContext) error) (driver.Connector, error) {
		real, err := duckdb.NewConnector(dsn, connInit)
		if err != nil {
			return nil, fmt.Errorf("open stub duckdb: %w", err)
		}
		inst := &invalidatingInstance{Connector: real}
		mu.Lock()
		opened = append(opened, inst)
		mu.Unlock()
		return inst, nil
	}
	t.Cleanup(func() { newDuckConnectorFn = previous })
	return func() []*invalidatingInstance {
		mu.Lock()
		defer mu.Unlock()
		return append([]*invalidatingInstance(nil), opened...)
	}
}

// A long-lived Runner caches one exporter per config group (#331). After a
// FATAL error invalidates that exporter's database, the next run for the
// group must export on a reopened database through the same cached
// exporter, without a process restart (#717).
func TestRunnerCachedDuckExporterReopensAfterFatalError(t *testing.T) {
	ctx := context.Background()
	opened := stubInvalidatingDuckConnector(t)
	core, logs := observer.New(zapcore.WarnLevel)
	runner := NewRunner(zap.New(core))
	t.Cleanup(func() { _ = runner.Close() })

	cfg := CDCConfig{DuckDBPath: "", DuckThreads: 3, QueryTimeout: 30 * time.Second}
	s3Runtime := &cachedS3Runtime{region: "us-east-1"}

	// First run: the export statement invalidates the database.
	exporter, release, err := runner.getOrCreateDuckExporter(ctx, cfg, s3Runtime)
	require.NoError(t, err)
	_, err = exporter.DB.ExecContext(ctx, "CREATE TABLE instance_marker AS SELECT 1 AS x")
	require.NoError(t, err)
	err = exporter.executeExportSQL(ctx, cfg, "duckdb export sql", "duckdb copy exec", fatalStatement, "", "", "")
	require.ErrorContains(t, err, "FATAL Error", "the statement that hit the error still fails")
	release()

	// Next run: same cached exporter, reopened database.
	again, releaseAgain, err := runner.getOrCreateDuckExporter(ctx, cfg, s3Runtime)
	require.NoError(t, err)
	defer releaseAgain()
	require.Same(t, exporter, again, "the runner keeps its cached exporter")

	out := filepath.Join(t.TempDir(), "export.csv")
	exportSQL := fmt.Sprintf("COPY (SELECT 42 AS x) TO '%s' (FORMAT CSV)", out)
	require.NoError(t, again.executeExportSQL(ctx, cfg, "duckdb export sql", "duckdb copy exec", exportSQL, "", "", ""),
		"the next run exports on a reopened database")

	instances := opened()
	require.Len(t, instances, 2, "the invalidated database is reopened once")
	require.True(t, instances[0].closed.Load(), "the invalidated database is closed")
	require.False(t, instances[1].invalidated.Load())

	var tables int
	require.NoError(t, again.DB.QueryRowContext(ctx,
		"SELECT count(*) FROM duckdb_tables() WHERE table_name = 'instance_marker'").Scan(&tables))
	require.Zero(t, tables, "the export ran on a new DuckDB instance")
	var threads int64
	require.NoError(t, again.DB.QueryRowContext(ctx, "SELECT current_setting('threads')").Scan(&threads))
	require.Equal(t, int64(3), threads, "the exporter's init hook runs on the reopened database")

	require.Equal(t, 1, logs.FilterMessageSnippet("the database was invalidated").Len())
	require.Equal(t, 1, logs.FilterMessageSnippet("reopened the database").Len())
}
