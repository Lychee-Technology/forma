package reconcile

import (
	"context"
	"database/sql"
	"fmt"

	"strings"

	"github.com/google/uuid"

	"github.com/lychee-technology/forma/internal/sqlutil"
)

// RegistrySchemaEnumerator enumerates schema IDs from the schema registry
// table, mirroring cdc-init's getSchemaIDsToInit: every registered schema
// reconciles, not just those with pending CDC work.
type RegistrySchemaEnumerator struct {
	DB             *sql.DB
	Table          string
	SchemaIDFilter int // 0 = all schemas
}

func (e *RegistrySchemaEnumerator) SchemaIDs(ctx context.Context) ([]int16, error) {
	table := sqlutil.SanitizeIdentifier(e.Table)
	var rows *sql.Rows
	var err error
	if e.SchemaIDFilter > 0 {
		rows, err = e.DB.QueryContext(ctx,
			fmt.Sprintf("SELECT schema_id FROM %s WHERE schema_id = $1", table), e.SchemaIDFilter)
	} else {
		rows, err = e.DB.QueryContext(ctx,
			fmt.Sprintf("SELECT schema_id FROM %s ORDER BY schema_id", table))
	}
	if err != nil {
		return nil, fmt.Errorf("query schema ids from %s: %w", table, err)
	}
	defer func() { _ = rows.Close() }()

	var ids []int16
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan schema id: %w", err)
		}
		if id < -32768 || id > 32767 {
			return nil, fmt.Errorf("schema id %d from %s overflows int16", id, table)
		}
		ids = append(ids, int16(id))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate schema ids from %s: %w", table, err)
	}
	if e.SchemaIDFilter > 0 && len(ids) == 0 {
		// A silent empty run would report "consistent" for a schema that
		// was never inspected (typo'd --schema-id, or a deregistered
		// schema whose S3 objects linger).
		return nil, fmt.Errorf("schema %d is not registered in %s; nothing was inspected", e.SchemaIDFilter, table)
	}
	return ids, nil
}

// PGAdvisoryLocker takes the flusher's per-schema advisory lock
// (pg_try_advisory_lock(schemaID, schemaID), cdc.AcquireSchemaLock). Unlike
// the flusher it pins one physical connection for the lock's lifetime: on a
// pool, acquire and release could land on different connections, and a
// session-scoped lock released on the wrong session silently fails.
type PGAdvisoryLocker struct {
	DB *sql.DB
}

func (l *PGAdvisoryLocker) TryLock(ctx context.Context, schemaID int16) (bool, func(), error) {
	conn, err := l.DB.Conn(ctx)
	if err != nil {
		return false, nil, fmt.Errorf("pin connection for schema %d lock: %w", schemaID, err)
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
		// Background context: the unlock must run even when the reconcile
		// context is already cancelled; closing the pinned connection
		// releases the session-scoped lock regardless.
		_, _ = conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1, $2)", int32(schemaID), int32(schemaID))
		_ = conn.Close()
	}
	return true, unlock, nil
}

// PGLiveRows implements LiveRowChecker over the entity main table. Deletes
// are physical (postgres_persistent_repository issues DELETE), so liveness
// is row existence under the (schema_id, row_id) primary key.
type PGLiveRows struct {
	DB    *sql.DB
	Table string // entity_main table name
}

func (p *PGLiveRows) MissingLiveRows(ctx context.Context, schemaID int16, rowIDs []string) ([]string, error) {
	if len(rowIDs) == 0 {
		return nil, nil
	}
	for _, id := range rowIDs {
		if err := uuid.Validate(id); err != nil {
			return nil, fmt.Errorf("row id %q from parquet is not a UUID: %w", id, err)
		}
	}
	query := fmt.Sprintf(
		"SELECT ltbase_row_id::text FROM %s WHERE ltbase_schema_id = $1 AND ltbase_row_id = ANY(string_to_array($2, ',')::uuid[])",
		sqlutil.SanitizeIdentifier(p.Table))
	rows, err := p.DB.QueryContext(ctx, query, schemaID, strings.Join(rowIDs, ","))
	if err != nil {
		return nil, fmt.Errorf("query live rows of schema %d from %s: %w", schemaID, p.Table, err)
	}
	defer func() { _ = rows.Close() }()

	live := make(map[string]bool, len(rowIDs))
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan live row id: %w", err)
		}
		live[strings.ToLower(id)] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate live rows of schema %d: %w", schemaID, err)
	}

	var missing []string
	for _, id := range rowIDs {
		if !live[strings.ToLower(id)] {
			missing = append(missing, id)
		}
	}
	return missing, nil
}

var (
	_ SchemaEnumerator = (*RegistrySchemaEnumerator)(nil)
	_ Locker           = (*PGAdvisoryLocker)(nil)
	_ LiveRowChecker   = (*PGLiveRows)(nil)
)
