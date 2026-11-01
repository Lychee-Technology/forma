package duckdbinit

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync"
	"sync/atomic"
	"testing"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// cachingDuck models duckdb-go's instance cache for a file-backed database:
// open hands back the current database while anything still references it
// (an open connector or connection), and opens a new one only once nothing
// does. Every connector runs init on each new connection, as
// duckdb.NewConnector's connection init hook does, and closes the connection
// when init fails.
type cachingDuck struct {
	init  func(driver.ExecerContext) error
	opens int
	cur   *cachedDB
}

type cachedDB struct {
	fakeInstance
	refs atomic.Int32
}

func (f *cachingDuck) open() (driver.Connector, error) {
	f.opens++
	if f.cur == nil || f.cur.refs.Load() == 0 {
		f.cur = &cachedDB{fakeInstance: fakeInstance{id: f.opens}}
	}
	f.cur.refs.Add(1)
	return &cachedConnector{db: f.cur, init: f.init}, nil
}

type cachedConnector struct {
	db        *cachedDB
	init      func(driver.ExecerContext) error
	closeOnce sync.Once
}

func (c *cachedConnector) Connect(context.Context) (driver.Conn, error) {
	c.db.refs.Add(1)
	conn := &cachedConn{fakeDuckConn: fakeDuckConn{inst: &c.db.fakeInstance}, db: c.db}
	if err := c.init(conn); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

func (c *cachedConnector) Driver() driver.Driver { return duckdb.Driver{} }
func (c *cachedConnector) Close() error {
	c.closeOnce.Do(func() { c.db.refs.Add(-1) })
	return nil
}

type cachedConn struct {
	fakeDuckConn
	db        *cachedDB
	closeOnce sync.Once
}

func (c *cachedConn) Close() error {
	c.closeOnce.Do(func() { c.db.refs.Add(-1) })
	return nil
}

// A file-backed reopen while an older connection still holds the invalidated
// database gets that database back from the instance cache, and its required
// init step fails FATAL inside Connect. The caller still sees that failure,
// and the connector must mark the replacement invalid, so the reopen after
// the older connection is gone reaches a new database (#702, #752).
func TestRecoveringConnectorRecoversWhenRequiredInitHitsCachedInvalidDatabase(t *testing.T) {
	ctx := context.Background()
	steps := []Step{SingleStmtStep("SET threads=3;", "set threads").AsRequired()}
	fake := &cachingDuck{init: MakeConnInit(steps, zap.NewNop().Sugar(), DefaultInitTimeout)}
	connector, err := NewRecoveringConnector(fake.open, zap.NewNop().Sugar())
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(2)
	t.Cleanup(func() { _ = db.Close() })

	older, err := db.Conn(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, queryInstanceID(t, older))

	_, err = db.ExecContext(ctx, "fatal")
	require.Error(t, err)
	require.True(t, connector.cur.invalid.Load())

	// The reopen hands back the invalidated database: older still holds it.
	for attempt := 1; attempt <= 2; attempt++ {
		_, err = db.QueryContext(ctx, "id")
		var stepErr *RequiredStepError
		require.ErrorAs(t, err, &stepErr, "attempt %d: the required init failure reaches the caller", attempt)
		require.Equal(t, "set threads", stepErr.Label)
		var duckErr *duckdb.Error
		require.ErrorAs(t, err, &duckErr)
		require.Equal(t, duckdb.ErrorTypeFatal, duckErr.Type)
		require.Equal(t, 1+attempt, fake.opens, "attempt %d: a failed Connect marks the replacement invalid", attempt)
	}

	require.NoError(t, older.Close())
	require.Equal(t, 4, queryInstanceID(t, db), "once nothing holds it, the reopen reaches a new database")
	require.Equal(t, 4, queryInstanceID(t, db))
	require.Equal(t, 4, fake.opens)
}
