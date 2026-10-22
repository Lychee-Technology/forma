package cdc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemalease"
)

// ErrSchemaLockContended reports that another holder (flusher, cdc-init, or
// manifest-reconcile) owns the per-schema lock.
var ErrSchemaLockContended = errors.New("schema lock contended")

// ErrSchemaLockLost is the cause of a held context that ended because its
// schema lock did: on Aurora DSQL, a lease the holder could not renew in
// time, or one another holder took over after it expired.
var ErrSchemaLockLost = errors.New("schema lock lost")

// SchemaLocker takes the per-schema lock that keeps cdc-flush, cdc-init and
// manifest-reconcile apart (#290). How it locks depends on the database
// engine (NewSchemaLocker), so every holder of one database must run the
// same engine.
type SchemaLocker interface {
	// TryLock takes schemaID's lock without waiting, and answers ok false
	// when another holder has it. When ok, held derives from ctx and ends if
	// the lock is lost, with ErrSchemaLockLost as its cause: work done under
	// the lock runs on held. release ends held and gives the lock up; it is
	// non-nil exactly when ok.
	TryLock(ctx context.Context, schemaID int16) (held context.Context, release func(), ok bool, err error)
}

// SchemaLockerFunc adapts a function to SchemaLocker.
type SchemaLockerFunc func(ctx context.Context, schemaID int16) (context.Context, func(), bool, error)

// TryLock calls f.
func (f SchemaLockerFunc) TryLock(ctx context.Context, schemaID int16) (context.Context, func(), bool, error) {
	return f(ctx, schemaID)
}

// NewSchemaLocker answers the schema lock of engine over db (#633): the
// session advisory lock on PostgreSQL, and on Aurora DSQL, which has no
// advisory locks, a lease row in entityMain's schema-lease table
// (internal/schemalease). An unknown engine is refused rather than run with
// PostgreSQL's lock, which DSQL would reject at its first statement.
func NewSchemaLocker(db *sql.DB, engine forma.DatabaseEngine, entityMain string) (SchemaLocker, error) {
	if err := engine.Validate(); err != nil {
		return nil, fmt.Errorf("select the schema lock of database engine %q: %w", engine, err)
	}
	if engine.Resolved() == forma.EnginePostgreSQL {
		return advisorySchemaLocker{db: db}, nil
	}
	if entityMain == "" {
		entityMain = "entity_main"
	}
	return newLeaseSchemaLocker(db, schemalease.Table(entityMain), defaultLeaseTTL, defaultLeaseRenewEvery), nil
}

// advisorySchemaLocker is the PostgreSQL engine's schema lock, TrySchemaLock.
// A session lock cannot be seen to end before its holder releases it, so
// held ends only on release or with ctx.
type advisorySchemaLocker struct {
	db *sql.DB
}

func (l advisorySchemaLocker) TryLock(ctx context.Context, schemaID int16) (context.Context, func(), bool, error) {
	locked, unlock, err := TrySchemaLock(ctx, l.db, schemaID)
	if err != nil || !locked {
		return nil, nil, false, err
	}
	held, cancel := context.WithCancel(ctx)
	return held, func() {
		cancel()
		unlock()
	}, true, nil
}

// TrySchemaLock pins one physical connection and takes
// pg_try_advisory_lock(schemaID, schemaID). The lock is session-scoped: on a
// pool, acquire and release could land on different connections, and a lock
// released on the wrong session silently fails — so unlock runs on the same
// pinned conn and closes it to end the session. unlock is non-nil iff locked;
// it uses a background context so the lock is released even after ctx cancel.
func TrySchemaLock(ctx context.Context, db *sql.DB, schemaID int16) (bool, func(), error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return false, nil, fmt.Errorf("pin connection for schema %d advisory lock: %w", schemaID, err)
	}
	var locked bool
	row := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1, $2)", int32(schemaID), int32(schemaID))
	if err := row.Scan(&locked); err != nil {
		_ = conn.Close()
		return false, nil, fmt.Errorf("acquire schema %d advisory lock: %w", schemaID, err)
	}
	if !locked {
		_ = conn.Close()
		return false, nil, nil
	}
	unlock := func() {
		_, _ = conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1, $2)", int32(schemaID), int32(schemaID))
		_ = conn.Close()
	}
	return true, unlock, nil
}

// WithLockLoss names the lost lock as the cause of err, the result of work
// done on a SchemaLocker's held context: work cut short by the loss otherwise
// reports only context.Canceled. A loss is reported even when err is nil,
// because work that skips a step it could not do (a best-effort delete, a
// probe it logs and passes over) returns nil after the loss cancelled that
// step, and its result is then incomplete, not clean.
func WithLockLoss(held context.Context, err error) error {
	cause := context.Cause(held)
	if !errors.Is(cause, ErrSchemaLockLost) {
		return err
	}
	if err == nil {
		return fmt.Errorf("%w; the work under it reported no error but may have skipped steps the loss cancelled", cause)
	}
	if errors.Is(err, ErrSchemaLockLost) {
		return err
	}
	return fmt.Errorf("%w: %w", cause, err)
}
