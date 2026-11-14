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

// parquetHistoryQuery answers one boolean for schema $1: whether it has a
// flushed change_log entry or a live entity_main row without an entry.
func parquetHistoryQuery(tables model.StorageTables) string {
	return fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM %[1]s WHERE schema_id = $1 AND flushed_at <> 0)
	OR EXISTS (SELECT 1 FROM %[2]s m WHERE m.ltbase_schema_id = $1 AND NOT EXISTS (
		SELECT 1 FROM %[1]s c WHERE c.schema_id = $1 AND c.row_id = m.ltbase_row_id))`,
		sqlutil.SanitizeIdentifier(tables.ChangeLog), sqlutil.SanitizeIdentifier(tables.EntityMain))
}

// queryParquetHistory reports whether PostgreSQL records that parquet was
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
//
// It runs parquetHistoryQuery for schemaID on q.
func queryParquetHistory(ctx context.Context, q changeLogQuerier, tables model.StorageTables, schemaID int16) (bool, error) {
	rows, err := q.Query(ctx, parquetHistoryQuery(tables), schemaID)
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
