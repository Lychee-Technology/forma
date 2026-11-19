package federated

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// The engine's Postgres change_log reads.

// changeLogQuerier sends one change_log read: the pool, or the snapshot
// transaction ReadChangeLogSnapshot holds.
type changeLogQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// dirtyIDPool is the pool the fetcher reads through. BeginTx opens the
// snapshot ReadChangeLogSnapshot reads in; *pgxpool.Pool satisfies it.
type dirtyIDPool interface {
	changeLogQuerier
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
}

type DirtyIDPool = dirtyIDPool

// PostgresDirtyIDFetcher reads a federated pass's change_log answers in a
// snapshot of its own (ReadChangeLogSnapshot), outside any pass: the init-db
// index test and the production harness use it. A pass reads the same
// statements in its own snapshot (readChangeLogSnapshot).
type PostgresDirtyIDFetcher struct {
	pool dirtyIDPool
}

// NewPostgresDirtyIDFetcher wraps pool.
func NewPostgresDirtyIDFetcher(pool dirtyIDPool) *PostgresDirtyIDFetcher {
	return &PostgresDirtyIDFetcher{pool: pool}
}

// strictDirtySetQuery reads the row IDs of schema $1 with an unflushed
// change_log entry. The primary key (schema_id, row_id, flushed_at) holds at
// most one such entry per row, so the IDs come back distinct.
func strictDirtySetQuery(changeLogTable string) string {
	return fmt.Sprintf(`SELECT row_id FROM %s WHERE schema_id = $1 AND flushed_at = 0`, sqlutil.SanitizeIdentifier(changeLogTable))
}

// queryRowIDs runs a query whose one column is a row ID and collects it.
func queryRowIDs(ctx context.Context, q changeLogQuerier, query string, args ...any) ([]uuid.UUID, error) {
	rows, err := q.Query(ctx, query, args...)
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
