package cdcv2

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// Entry is one change_log entry a generation holds: the row and the version
// (changed_at) of its unflushed entry. Marks are exact against it: an entry
// rewritten since has another version, is not marked and stays dirty.
type Entry struct {
	RowID   uuid.UUID
	Version int64
}

// selectSQL lists a schema's unflushed entries, oldest first. Versions are
// per-row and monotonic, so the order is the order of the writes.
const selectSQL = `SELECT row_id, changed_at FROM %s
	WHERE schema_id = $1 AND flushed_at = 0
	ORDER BY changed_at, row_id
	LIMIT $2`

// selectEntries reads up to limit unflushed entries of schemaID.
func (w *Writer) selectEntries(ctx context.Context, schemaID int16, limit int) ([]Entry, error) {
	rows, err := w.cfg.DB.QueryContext(ctx, fmt.Sprintf(selectSQL, w.changeLog), schemaID, limit)
	if err != nil {
		return nil, fmt.Errorf("select the unflushed entries of schema %d in %s: %w", schemaID, w.changeLog, err)
	}
	defer rows.Close()
	var entries []Entry
	for rows.Next() {
		var e Entry
		if err := rows.Scan(&e.RowID, &e.Version); err != nil {
			return nil, fmt.Errorf("scan an unflushed entry of schema %d: %w", schemaID, err)
		}
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the unflushed entries of schema %d: %w", schemaID, err)
	}
	return entries, nil
}

// markSkipLockedSQL is the PostgreSQL mark, cdc-flush's
// markFlushedSkipLocked (#686) on the publication's transaction: it marks a
// listed entry still unflushed at exactly its listed version, and skips one
// an open writer holds a row lock on, which stays dirty for the next flush.
// It never waits on a writer. The entries it locks stay locked until the
// publication commits, which T_pub bounds.
const markSkipLockedSQL = `WITH listed (row_id, version) AS (VALUES %s),
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
	RETURNING cl.row_id`

// markPlainSQL is the Aurora DSQL mark, the plain exact-version statement.
// DSQL takes no row locks: a writer that changed a listed entry concurrently
// aborts the whole transaction with 40001, and the publication is sent again
// with the same token.
const markPlainSQL = `UPDATE %s cl SET flushed_at = $1
	FROM (VALUES %s) AS v(row_id, version)
	WHERE cl.schema_id = $2 AND cl.flushed_at = 0
	  AND cl.row_id = v.row_id AND cl.changed_at = v.version
	RETURNING cl.row_id`

// mark marks entries of schemaID flushed at flushedAt on tx, in the
// engine's form, and answers how many it marked. It is one statement: a
// caller bounds entries to the engine's transaction budget.
func (w *Writer) mark(ctx context.Context, tx *sql.Tx, schemaID int16, entries []Entry, flushedAt int64) (int, error) {
	if len(entries) == 0 {
		return 0, nil
	}
	values, args := listedValues(entries, flushedAt, schemaID)
	query := fmt.Sprintf(markSkipLockedSQL, values, w.changeLog, w.changeLog)
	if w.dsql {
		query = fmt.Sprintf(markPlainSQL, w.changeLog, values)
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("mark %d entries of schema %d flushed at their listed versions: %w", len(entries), schemaID, err)
	}
	defer rows.Close()
	marked := 0
	for rows.Next() {
		marked++
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("mark %d entries of schema %d flushed at their listed versions: %w", len(entries), schemaID, err)
	}
	return marked, nil
}

// listedValues renders entries as a VALUES list whose placeholders follow
// $1 = flushedAt and $2 = schemaID.
func listedValues(entries []Entry, flushedAt int64, schemaID int16) (string, []any) {
	values := make([]string, len(entries))
	args := make([]any, 0, 2*len(entries)+2)
	args = append(args, flushedAt, schemaID)
	for i, e := range entries {
		values[i] = fmt.Sprintf("($%d::uuid, $%d::bigint)", len(args)+1, len(args)+2)
		args = append(args, e.RowID, e.Version)
	}
	return strings.Join(values, ","), args
}
