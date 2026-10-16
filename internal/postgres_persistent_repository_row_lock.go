package internal

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// lockRowInTx takes the lock a writer of (schemaID, rowID) takes, in a
// writer's order, and checks guard on the locked row, writing nothing (#650):
// the per-row write lock first, then, on PostgreSQL, the entity_main row
// lock. Taking them in the writers' order adds no deadlock two writers could
// not already produce.
//
// A missing row is forma.ErrNotFound and locks no entity_main row, but the
// per-row lock stays held, which serializes it with a concurrent create of
// that RowID. A row that fails guard is forma.ErrPreconditionFailed. Neither
// fails a statement, so neither poisons a Tx. guard was rendered before any
// statement was sent; nil requires only that the row exists.
//
// On PostgreSQL the advisory lock alone already excludes every Forma writer
// until the transaction ends. The probe runs once it is granted, so under
// READ COMMITTED its snapshot postdates the previous holder's commit (#457),
// and guard judges the row that holder left. On Aurora DSQL (#633) the
// per-row lock is a write of the row's guard row, which every writer of the
// row writes too, so a writer that commits first makes this transaction's
// COMMIT fail with 40001, and so does a second asserter; the probe reads the
// snapshot and needs no locking clause. The guard row exists whether or not
// the entity row does, which is what keeps a missing-row assertion
// serialized with a create there.
func lockRowInTx(ctx context.Context, tx pgx.Tx, locks rowLocking, tables model.StorageTables, schemaID int16, rowID uuid.UUID, guard *renderedGuard) error {
	if err := locks.lockRow(ctx, tx, tables, schemaID, rowID); err != nil {
		return fmt.Errorf("lock row version for %s: %w", rowID, err)
	}
	clause, args := "true", []any{schemaID, rowID}
	if guard != nil {
		clause = guard.clause
		args = append(args, guard.args...)
	}
	probe := strings.TrimSpace(fmt.Sprintf(
		"SELECT COALESCE((%s), false) FROM %s m WHERE m.ltbase_schema_id = $1 AND m.ltbase_row_id = $2 %s",
		clause, sanitizeIdentifier(tables.EntityMain), locks.recheckClause(false)))
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
