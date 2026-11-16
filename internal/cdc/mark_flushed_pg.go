package cdc

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"
)

// markFlushedSkipLocked is the flush mark of the PostgreSQL engine (#686).
// It marks what MarkFlushedVersions marks, a listed row whose slot-0 entry
// is still unflushed at exactly its listed version, except an entry another
// transaction holds a row lock on: that entry stays dirty for the next flush.
//
// MarkFlushedVersions locks the entries in the order it reaches them and
// waits at each one an open writer holds, while keeping those it already
// marked. A writer that wrote one entry of the batch therefore stalled every
// other writer of an already-marked entry for as long as it stayed open, and
// a writer of two entries taken in the opposite order deadlocked with it
// (40P01). Here the claimable CTE takes its row locks with SKIP LOCKED, so it
// never waits, and the UPDATE touches only the entries the CTE locked, so it
// waits for no one either. Both run in one statement, so no lock outlives it.
//
// A skipped entry is a rewritten one as far as the flush is concerned: the
// delta the manifest lists may hold its row, and the dirty anti-join
// discards that copy while flushed_at = 0, as for any row the mark did not
// take. FOR UPDATE re-reads an entry a writer committed since the
// statement's snapshot and keeps it only if it still matches its listed
// version, so the mark stays exact.
//
// A writer that reaches an entry after this statement locked it still waits
// for the statement to finish, which takes no other transaction's lock.
func markFlushedSkipLocked(ctx context.Context, db *sql.DB, table string, schemaID int16, rowIDs []uuid.UUID, versions map[uuid.UUID]int64, flushedAt int64) ([]uuid.UUID, error) {
	if table == "" {
		table = "change_log"
	}
	if len(rowIDs) == 0 {
		return nil, nil
	}
	valueRows, args, err := listedVersionValues(rowIDs, versions, flushedAt, schemaID)
	if err != nil {
		return nil, err
	}
	tbl := sanitizeIdentifier(table)
	query := fmt.Sprintf(
		`WITH listed (row_id, version) AS (VALUES %s),
		 claimable AS (
		   SELECT cl.schema_id, cl.row_id, cl.changed_at
		   FROM %s cl JOIN listed v ON cl.row_id = v.row_id AND cl.changed_at = v.version
		   WHERE cl.schema_id = $2 AND cl.flushed_at = 0
		   FOR UPDATE OF cl SKIP LOCKED
		 )
		 UPDATE %s cl SET flushed_at = $1
		 FROM claimable c
		 WHERE cl.schema_id = c.schema_id AND cl.row_id = c.row_id
		   AND cl.flushed_at = 0 AND cl.changed_at = c.changed_at
		 RETURNING cl.row_id`,
		valueRows, tbl, tbl,
	)
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("mark flushed by listed versions, skipping locked entries: %w", err)
	}
	return scanMarkedRowIDs(rows)
}
