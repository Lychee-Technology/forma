package internal

import (
	"context"
	"errors"
	"fmt"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// lockRowInTx takes the lock a writer of (schemaID, rowID) takes, in a
// writer's order, and checks guard on the locked row, writing nothing (#650):
// the per-row advisory lock first, then the entity_main row lock. Taking them
// in the writers' order adds no deadlock two writers could not already
// produce.
//
// A missing row is forma.ErrNotFound and locks no entity_main row, but the
// advisory lock stays held, which serializes it with a concurrent create of
// that RowID. A row that fails guard is forma.ErrPreconditionFailed. Neither
// fails a statement, so neither poisons a Tx. guard was rendered before any
// statement was sent; nil requires only that the row exists.
//
// On PostgreSQL the advisory lock alone already excludes every Forma writer
// until the transaction ends. The row lock is what the Aurora DSQL contract
// rests on (#633): there FOR UPDATE takes no lock but registers the row, so a
// transaction that changed it and committed first makes this one's COMMIT
// fail with 40001. The probe runs once the advisory lock is granted, so under
// READ COMMITTED its snapshot postdates the previous holder's commit (#457),
// and guard judges the row that holder left.
func lockRowInTx(ctx context.Context, tx pgx.Tx, tables model.StorageTables, schemaID int16, rowID uuid.UUID, guard *renderedGuard) error {
	if err := lockRowVersion(ctx, tx, schemaID, rowID); err != nil {
		return fmt.Errorf("lock row version for %s: %w", rowID, err)
	}
	clause, args := "true", []any{schemaID, rowID}
	if guard != nil {
		clause = guard.clause
		args = append(args, guard.args...)
	}
	probe := fmt.Sprintf(
		"SELECT COALESCE((%s), false) FROM %s m WHERE m.ltbase_schema_id = $1 AND m.ltbase_row_id = $2 FOR UPDATE OF m",
		clause, sanitizeIdentifier(tables.EntityMain))
	var holds bool
	if err := tx.QueryRow(ctx, probe, args...).Scan(&holds); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return rowNotFound(schemaID, rowID)
		}
		return fmt.Errorf("lock entity row %s: %w", rowID, err)
	}
	if !holds {
		return rowPreconditionFailed(schemaID, rowID)
	}
	return nil
}
