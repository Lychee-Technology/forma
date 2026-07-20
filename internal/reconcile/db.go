package reconcile

import (
	"context"
	"database/sql"
	"fmt"

	"strings"

	"github.com/google/uuid"

	"github.com/lychee-technology/forma/internal/cdc"
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
// (pg_try_advisory_lock(schemaID, schemaID)). It delegates to cdc.TrySchemaLock,
// the single source of truth for the pinned-connection acquire/release dance:
// on a pool, acquire and release could land on different connections, and a
// session-scoped lock released on the wrong session silently fails, so the lock
// pins one physical connection for its lifetime and closes it to unlock.
type PGAdvisoryLocker struct {
	DB *sql.DB
}

func (l *PGAdvisoryLocker) TryLock(ctx context.Context, schemaID int16) (bool, func(), error) {
	return cdc.TrySchemaLock(ctx, l.DB, schemaID)
}

// PGLiveRows implements LiveRowChecker over the entity main table. Liveness
// mirrors cdc-init's export filter (init.go): the row exists under the
// (schema_id, row_id) primary key AND ltbase_deleted_at IS NULL — writes can
// soft-delete by setting the column, and treating such rows as live would
// let repair re-append data whose tombstone compaction already dropped.
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
		"SELECT ltbase_row_id::text FROM %s WHERE ltbase_schema_id = $1 AND ltbase_deleted_at IS NULL AND ltbase_row_id = ANY(string_to_array($2, ',')::uuid[])",
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
