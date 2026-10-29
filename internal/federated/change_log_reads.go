package federated

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// The engine's Postgres change_log reads.

// DirtyIDFetcher retrieves row IDs from the change log that are newer than
// the flushed Parquet tiers and must be excluded from DuckDB results.
type DirtyIDFetcher interface {
	FetchDirtyRowIDs(ctx context.Context, changeLogTable string, schemaID int16) ([]uuid.UUID, error)
	// HasFlushedRows reports whether any change_log entry of schemaID was
	// marked flushed before cutoffMs: a version the dirty barrier hands to
	// the parquet tiers rather than to the hot leg (#720).
	HasFlushedRows(ctx context.Context, changeLogTable string, schemaID int16, cutoffMs int64) (bool, error)
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

// HasFlushedRows reports whether changeLogTable holds an entry of schemaID
// with 0 < flushed_at < cutoffMs. Flushed entries are never deleted (a later
// write adds a new flushed_at = 0 entry beside them), so once a schema has
// been flushed the answer stays true.
func (f *PostgresDirtyIDFetcher) HasFlushedRows(ctx context.Context, changeLogTable string, schemaID int16, cutoffMs int64) (bool, error) {
	if changeLogTable == "" {
		return false, fmt.Errorf("change log table name cannot be empty")
	}
	if f == nil || f.pool == nil {
		return false, fmt.Errorf("postgres dirty id fetcher is not available")
	}
	query := fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM %s WHERE schema_id = $1 AND flushed_at <> 0 AND flushed_at < $2)`,
		sqlutil.SanitizeIdentifier(changeLogTable))
	rows, err := f.pool.Query(ctx, query, schemaID, cutoffMs)
	if err != nil {
		return false, fmt.Errorf("query flushed change_log entries of schema %d: %w", schemaID, err)
	}
	defer rows.Close()
	// EXISTS always answers one row; reading none means the read failed, and
	// the answer must not default to "never flushed".
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return false, fmt.Errorf("read flushed change_log entries of schema %d: %w", schemaID, err)
		}
		return false, fmt.Errorf("read flushed change_log entries of schema %d: no row returned", schemaID)
	}
	var flushed bool
	if err := rows.Scan(&flushed); err != nil {
		return false, fmt.Errorf("scan flushed change_log entries of schema %d: %w", schemaID, err)
	}
	return flushed, nil
}
