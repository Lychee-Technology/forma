package internal

import (
	"context"
	"errors"
	"fmt"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sqlgen"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma"
)

// renderedGuard is a RowGuard's precondition rendered to SQL: a predicate
// over the entity_main row aliased m, whose placeholders start at $3.
type renderedGuard struct {
	clause string
	args   []any
}

// renderRowGuard renders guard's precondition with the renderer
// QueryPersistentRecords uses (buildHybridConditions), anchored on the
// entity_main row so that one statement both finds the row and evaluates the
// condition on it. Every write renders before it sends its first statement,
// so a malformed condition fails as the DSL's own ErrInvalidInput without
// touching the database, and without poisoning a Tx (#649). A nil answer
// means the guard requires nothing.
//
// The anchor does not change which rows match. Query anchors on eav_data
// when no leaf reads entity_main, but every eav_data leaf is an EXISTS
// correlated on the row, and the one leaf an entity without eav_data rows
// can satisfy, is_null, already forces the entity_main anchor
// (hasMainTableCondition).
func (r *DBPersistentRecordRepository) renderRowGuard(tables model.StorageTables, schemaID int16, guard model.RowGuard) (*renderedGuard, error) {
	if guard.Precondition == nil {
		return nil, nil
	}
	clause, args, err := r.buildHybridConditions(
		sanitizeIdentifier(tables.EAVData),
		sanitizeIdentifier(tables.EntityMain),
		model.AttributeQuery{SchemaID: schemaID, Condition: guard.Precondition},
		2, // $1 and $2 are the probe's schema and row
		true,
	)
	if err != nil {
		return nil, fmt.Errorf("render precondition for schema %d: %w", schemaID, err)
	}
	if clause == "" {
		return nil, fmt.Errorf("render precondition for schema %d: the condition rendered no predicate", schemaID)
	}
	return &renderedGuard{clause: clause, args: args}, nil
}

// checkRowGuard evaluates a rendered precondition on tx once the row's lock
// is granted: under READ COMMITTED the statement's snapshot then postdates
// the previous holder's commit (#457), so a writer that waited judges the
// row the winner left. COALESCE(p, false) is true exactly when WHERE p
// selects the row, so the answer is a hot-tier Query's for the same row and
// condition. It reports whether the row exists; a row that exists and does
// not satisfy the condition is forma.ErrPreconditionFailed. Neither outcome
// fails a statement, so neither poisons a Tx.
func checkRowGuard(ctx context.Context, tx pgx.Tx, tables model.StorageTables, schemaID int16, rowID uuid.UUID, guard *renderedGuard) (bool, error) {
	probe := fmt.Sprintf("SELECT COALESCE((%s), false) FROM %s m WHERE m.ltbase_schema_id = $1 AND m.ltbase_row_id = $2",
		guard.clause, sanitizeIdentifier(tables.EntityMain))
	args := append([]any{schemaID, rowID}, guard.args...)
	var holds bool
	if err := tx.QueryRow(ctx, probe, args...).Scan(&holds); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("evaluate precondition: %w", err)
	}
	if !holds {
		return true, forma.WithOperatorDetail(
			forma.PreconditionFailedf("precondition does not hold for entity (row=%s)", rowID),
			fmt.Errorf("schema=%d", schemaID))
	}
	return true, nil
}

// txClock is the TxClock of tx. It reads the clock through the spelling the
// $now operand renders, so a server_time value and a $now comparison in one
// transaction name the same instant. PostgreSQL's now() is fixed for the
// transaction, so every read answers the same value and nothing is cached.
func txClock(tx pgx.Tx) model.TxClock {
	return func(ctx context.Context) (int64, error) {
		var ms int64
		if err := tx.QueryRow(ctx, "SELECT "+sqlgen.ServerTimeMillisSQL).Scan(&ms); err != nil {
			return 0, fmt.Errorf("read transaction clock: %w", err)
		}
		return ms, nil
	}
}

// rowNotFound is the error a write answers for a row that does not exist.
func rowNotFound(schemaID int16, rowID uuid.UUID) error {
	return forma.WithOperatorDetail(forma.NotFoundf("entity not found (row=%s)", rowID),
		fmt.Errorf("schema=%d", schemaID))
}
