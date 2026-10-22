package cdc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lychee-technology/forma"
)

// The flush mark on Aurora DSQL (#633). MarkFlushedVersions marks a whole
// batch in one statement, but DSQL limits a transaction to 3,000 modified
// rows, and the default batch is 10,000. So on DSQL the mark is sent in
// statements of at most markRowsPerStatement rows, each its own transaction.
//
// A statement there conflicts with a writer that changed one of its rows
// concurrently, and DSQL aborts one of the two with 40001. A mark statement
// that loses is sent again: the mark is exact against the listed version,
// so a retry marks only the rows still at it, and a row the writer advanced
// stays dirty, as it would on PostgreSQL.
const (
	markRowsPerStatement = 1000
	markConflictAttempts = 5
	markConflictBackoff  = 20 * time.Millisecond
)

// markFlushedFunc is MarkFlushedVersions' signature, the flush executor's
// mark seam.
type markFlushedFunc func(context.Context, *sql.DB, string, int16, []uuid.UUID, map[uuid.UUID]int64, int64) ([]uuid.UUID, error)

// markFlushedFor answers the flush mark of engine.
func markFlushedFor(engine forma.DatabaseEngine) markFlushedFunc {
	if engine.Resolved() == forma.EngineAuroraDSQL {
		return markInChunks(MarkFlushedVersions)
	}
	return MarkFlushedVersions
}

// markInChunks answers mark sent in statements of at most
// markRowsPerStatement rows, each retried on 40001. When a statement fails
// for good, the rows earlier statements marked stay marked: they are in the
// delta the manifest already lists, and the rest stay dirty for the next
// flush, the same outcome as a mark that fails after the append.
func markInChunks(mark markFlushedFunc) markFlushedFunc {
	return func(ctx context.Context, db *sql.DB, table string, schemaID int16, rowIDs []uuid.UUID, versions map[uuid.UUID]int64, flushedAt int64) ([]uuid.UUID, error) {
		var marked []uuid.UUID
		for chunk := range slices.Chunk(rowIDs, markRowsPerStatement) {
			ids, err := retryConflicts(ctx, func() ([]uuid.UUID, error) {
				return mark(ctx, db, table, schemaID, chunk, versions, flushedAt)
			})
			if err != nil {
				return marked, fmt.Errorf("mark %d of %d rows flushed: %w", len(chunk), len(rowIDs), err)
			}
			marked = append(marked, ids...)
		}
		return marked, nil
	}
}

// retryConflicts runs mark until it succeeds, fails with an error other than
// 40001, or has failed markConflictAttempts times.
func retryConflicts(ctx context.Context, mark func() ([]uuid.UUID, error)) ([]uuid.UUID, error) {
	for attempt := 1; ; attempt++ {
		ids, err := mark()
		if err == nil || !isSerializationFailure(err) || attempt == markConflictAttempts {
			return ids, err
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("retry a conflicting mark: %w", ctx.Err())
		case <-time.After(time.Duration(attempt) * markConflictBackoff):
		}
	}
}

// isSerializationFailure reports SQLSTATE 40001, DSQL's answer to a
// statement that lost a write conflict.
func isSerializationFailure(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "40001"
}
