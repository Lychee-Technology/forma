package federated

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"go.uber.org/zap"
)

// invalidationProbeTimeout bounds the SELECT 1 that decides whether an
// INTERNAL error invalidated the database.
const invalidationProbeTimeout = 3 * time.Second

// duckConn is the driver surface database/sql uses on a DuckDB connection.
// instanceConn embeds it so every context-aware method stays promoted.
type duckConn interface {
	driver.Conn
	driver.QueryerContext
	driver.ExecerContext
	driver.ConnPrepareContext
	driver.ConnBeginTx
	driver.NamedValueChecker
}

// duckInstance is one opened DuckDB database and whether DuckDB has
// invalidated it.
type duckInstance struct {
	connector driver.Connector
	invalid   atomic.Bool

	// Guarded by recoveringConnector.mu. duckdb-go does not synchronize
	// Connector.Connect with Connector.Close, so a retired connector is
	// closed only once no Connect on it is in flight.
	connecting int
	retired    bool
	closed     bool
}

// recoveringConnector keeps one *sql.DB usable across DuckDB database
// invalidation (#702). A FATAL error, or an INTERNAL error after which the
// connection can no longer run SELECT 1, invalidates the whole DuckDB
// database: every later statement fails until the database is reopened.
// The connector marks the instance invalid, database/sql drops its pooled
// connections through ResetSession and IsValid, and the next Connect opens
// a fresh instance with the same DSN and connection init hook.
//
// The statement that hit the error still fails; nothing is retried. A
// :memory: instance loses only what was created in it, and the federated
// engine creates nothing there. A file-backed instance reopens through
// duckdb-go's instance cache, which hands back the invalidated instance
// while any of its connections is still open; such a reopen fails again and
// repeats until database/sql has dropped the stale connections.
type recoveringConnector struct {
	open func() (driver.Connector, error)

	mu     sync.Mutex
	cur    *duckInstance
	closed bool
}

func newRecoveringConnector(open func() (driver.Connector, error)) (*recoveringConnector, error) {
	first, err := open()
	if err != nil {
		return nil, fmt.Errorf("open duckdb database: %w", err)
	}
	return &recoveringConnector{open: open, cur: &duckInstance{connector: first}}, nil
}

// Connect opens a connection on the live instance, reopening the database
// first when the current instance has been invalidated.
func (r *recoveringConnector) Connect(ctx context.Context) (driver.Conn, error) {
	inst, err := r.acquire()
	if err != nil {
		return nil, err
	}
	conn, err := inst.connector.Connect(ctx)
	r.release(inst)
	if err != nil {
		return nil, fmt.Errorf("connect to duckdb: %w", err)
	}
	dc, ok := conn.(duckConn)
	if !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("duckdb connection %T lacks the context-aware driver interfaces", conn)
	}
	return &instanceConn{duckConn: dc, inst: inst}, nil
}

// acquire returns the live instance, reopening it if it was invalidated,
// and holds its connector open until release.
func (r *recoveringConnector) acquire() (*duckInstance, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errors.New("duckdb connector is closed")
	}
	if r.cur.invalid.Load() {
		if err := r.replaceLocked(); err != nil {
			return nil, err
		}
	}
	r.cur.connecting++
	return r.cur, nil
}

func (r *recoveringConnector) release(inst *duckInstance) {
	r.mu.Lock()
	defer r.mu.Unlock()
	inst.connecting--
	if err := closeIfDrainedLocked(inst); err != nil {
		zap.S().Warnw("duckdb: closing the invalidated database failed", "error", err)
	}
}

// replaceLocked opens a replacement for the invalidated instance and retires
// the old one. Connections still open on it keep it alive in DuckDB until
// database/sql discards them. If the reopen fails, the invalid instance stays
// current, so the next Connect retries.
func (r *recoveringConnector) replaceLocked() error {
	next, err := r.open()
	if err != nil {
		return fmt.Errorf("reopen invalidated duckdb database: %w", err)
	}
	stale := r.cur
	r.cur = &duckInstance{connector: next}
	stale.retired = true
	if err := closeIfDrainedLocked(stale); err != nil {
		zap.S().Warnw("duckdb: closing the invalidated database failed", "error", err)
	}
	zap.S().Warnw("duckdb: reopened the database after it was invalidated")
	return nil
}

// closeIfDrainedLocked closes a retired instance's connector once no Connect
// on it is in flight; the last release closes it otherwise.
func closeIfDrainedLocked(inst *duckInstance) error {
	if !inst.retired || inst.connecting > 0 || inst.closed {
		return nil
	}
	inst.closed = true
	return closeConnector(inst.connector)
}

// Driver returns the DuckDB driver.
func (r *recoveringConnector) Driver() driver.Driver {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cur.connector.Driver()
}

// Close closes the live instance; database/sql calls it from DB.Close. A
// Connect still in flight closes it when it finishes.
func (r *recoveringConnector) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	r.cur.retired = true
	if err := closeIfDrainedLocked(r.cur); err != nil {
		return fmt.Errorf("close duckdb database: %w", err)
	}
	return nil
}

func closeConnector(c driver.Connector) error {
	if closer, ok := c.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// instanceConn is a DuckDB connection that knows its instance, so an error
// on a connection of an already-replaced instance cannot invalidate the live
// one, and database/sql stops reusing it once its instance is invalid.
type instanceConn struct {
	duckConn
	inst *duckInstance
}

// QueryContext runs the query and watches its error for invalidation. A
// DuckDB result is materialized here, so execution errors surface now and
// not from the rows.
func (c *instanceConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.duckConn.QueryContext(ctx, query, args)
	c.observe(err)
	return rows, err
}

// ExecContext runs the statement and watches its error for invalidation.
func (c *instanceConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	res, err := c.duckConn.ExecContext(ctx, query, args)
	c.observe(err)
	return res, err
}

// ResetSession runs before database/sql reuses a pooled connection.
func (c *instanceConn) ResetSession(context.Context) error {
	if c.inst.invalid.Load() {
		return driver.ErrBadConn
	}
	return nil
}

// IsValid runs when database/sql returns a connection to the pool.
func (c *instanceConn) IsValid() bool {
	return !c.inst.invalid.Load()
}

func (c *instanceConn) observe(err error) {
	var duckErr *duckdb.Error
	if !errors.As(err, &duckErr) {
		return
	}
	switch duckErr.Type {
	case duckdb.ErrorTypeFatal:
	case duckdb.ErrorTypeInternal:
		if !c.probeInvalidated() {
			return
		}
	default:
		return
	}
	if c.inst.invalid.CompareAndSwap(false, true) {
		zap.S().Errorw("duckdb: the database was invalidated; the next connection reopens it", "error", err)
	}
}

// probeInvalidated reports whether the database refuses a trivial query
// with a FATAL error, the state DuckDB enters after an INTERNAL error.
func (c *instanceConn) probeInvalidated() bool {
	ctx, cancel := context.WithTimeout(context.Background(), invalidationProbeTimeout)
	defer cancel()
	rows, err := c.duckConn.QueryContext(ctx, "SELECT 1", nil)
	if err == nil {
		_ = rows.Close()
		return false
	}
	var duckErr *duckdb.Error
	return errors.As(err, &duckErr) && duckErr.Type == duckdb.ErrorTypeFatal
}
