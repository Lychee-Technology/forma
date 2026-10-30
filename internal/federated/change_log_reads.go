package federated

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// The engine's Postgres change_log reads.

// DirtyIDFetcher retrieves row IDs from the change log that are newer than
// the flushed Parquet tiers and must be excluded from DuckDB results.
type DirtyIDFetcher interface {
	FetchDirtyRowIDs(ctx context.Context, changeLogTable string, schemaID int16) ([]uuid.UUID, error)
	// HasParquetHistory reports whether PostgreSQL records that parquet was
	// published for schemaID (#720). See
	// PostgresDirtyIDFetcher.HasParquetHistory.
	HasParquetHistory(ctx context.Context, tables model.StorageTables, schemaID int16) (bool, error)
}

type dirtyIDPool interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

type DirtyIDPool = dirtyIDPool

// PostgresDirtyIDFetcher reads unflushed row IDs from the change_log table,
// satisfying the DirtyIDFetcher seam with a live Postgres pool.
type PostgresDirtyIDFetcher struct {
	pool dirtyIDPool
}

// NewPostgresDirtyIDFetcher wraps pool as a DirtyIDFetcher.
func NewPostgresDirtyIDFetcher(pool dirtyIDPool) *PostgresDirtyIDFetcher {
	return &PostgresDirtyIDFetcher{pool: pool}
}

// FetchDirtyRowIDs returns the row IDs in changeLogTable for schemaID that
// have not been flushed to Parquet yet.
func (f *PostgresDirtyIDFetcher) FetchDirtyRowIDs(ctx context.Context, changeLogTable string, schemaID int16) ([]uuid.UUID, error) {
	if changeLogTable == "" {
		return nil, fmt.Errorf("change log table name cannot be empty")
	}
	if f == nil || f.pool == nil {
		return nil, fmt.Errorf("postgres dirty id fetcher is not available")
	}
	query := fmt.Sprintf(`SELECT row_id FROM %s WHERE schema_id = $1 AND flushed_at = 0`, sqlutil.SanitizeIdentifier(changeLogTable))
	rows, err := f.pool.Query(ctx, query, schemaID)
	if err != nil {
		return nil, fmt.Errorf("query dirty row ids: %w", err)
	}
	defer rows.Close()

	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan dirty row id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dirty row ids: %w", err)
	}
	return ids, nil
}

// HasParquetHistory reports whether PostgreSQL records that parquet was
// published for schemaID. Two records qualify, and neither depends on a
// clock:
//
//   - a change_log entry with flushed_at <> 0. Flushed entries are never
//     deleted (a later write adds a new flushed_at = 0 entry beside them), so
//     once a schema has been flushed the answer stays true;
//   - a live entity_main row with no change_log entry. Every write leaves an
//     entry, so such a row is one a cdc-init export holds after the onboarding
//     handoff cleared change_log: it lives in PostgreSQL and in base parquet,
//     and the hot leg, which joins entity_main through change_log, does not
//     serve it.
//
// A false answer means every change_log entry is unflushed and every live row
// has one. The dirty barrier then shadows every row_id that has a change_log
// entry, so no flushed version can reach the answer and the hot leg's answer
// is the whole answer. It does not mean no parquet was ever written: a
// cdc-init export whose rows have all been written again since the handoff
// leaves no record here. Each of those rows has an entry, so the barrier
// shadows the export row for row, and losing it changes no answer.
func (f *PostgresDirtyIDFetcher) HasParquetHistory(ctx context.Context, tables model.StorageTables, schemaID int16) (bool, error) {
	if tables.ChangeLog == "" || tables.EntityMain == "" {
		return false, fmt.Errorf("change log and entity main table names cannot be empty")
	}
	if f == nil || f.pool == nil {
		return false, fmt.Errorf("postgres dirty id fetcher is not available")
	}
	changeLog := sqlutil.SanitizeIdentifier(tables.ChangeLog)
	query := fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM %[1]s WHERE schema_id = $1 AND flushed_at <> 0)
	OR EXISTS (SELECT 1 FROM %[2]s m WHERE m.ltbase_schema_id = $1 AND NOT EXISTS (
		SELECT 1 FROM %[1]s c WHERE c.schema_id = $1 AND c.row_id = m.ltbase_row_id))`,
		changeLog, sqlutil.SanitizeIdentifier(tables.EntityMain))
	rows, err := f.pool.Query(ctx, query, schemaID)
	if err != nil {
		return false, fmt.Errorf("query parquet history of schema %d: %w", schemaID, err)
	}
	defer rows.Close()
	// EXISTS always answers one row; reading none means the read failed, and
	// the answer must not default to "nothing to read".
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return false, fmt.Errorf("read parquet history of schema %d: %w", schemaID, err)
		}
		return false, fmt.Errorf("read parquet history of schema %d: no row returned", schemaID)
	}
	var found bool
	if err := rows.Scan(&found); err != nil {
		return false, fmt.Errorf("scan parquet history of schema %d: %w", schemaID, err)
	}
	return found, nil
}
