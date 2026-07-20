package cdc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// ErrSchemaLockContended reports that another holder (flusher, cdc-init, or
// manifest-reconcile) owns the per-schema advisory lock.
var ErrSchemaLockContended = errors.New("schema advisory lock contended")

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
