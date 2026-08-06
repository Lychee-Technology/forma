package internal

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

// classifyPgError converts well-known PostgreSQL error codes to sentinel errors.
// Currently handles:
//   - 23505 unique_violation → forma.ErrConflict
//
// The published message is a curated summary: pgErr.Detail names physical
// columns and constraint layout ("Key (schema_id, row_id)=(…) already
// exists"), which must not cross a public transport (#313). The whole driver
// error — Detail included — stays in the chain as operator detail.
func classifyPgError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		// Detail is prefixed into the operator text explicitly: PgError.Error()
		// renders severity/message/SQLSTATE only, so without this the offending
		// key would vanish from the log line too.
		return forma.WithOperatorDetail(
			forma.Conflictf("the write conflicts with a row that already exists"),
			fmt.Errorf("%s: %w", pgErr.Detail, err))
	}
	return err
}

func sortedColumnKeys[T any](source map[string]T, allowed map[string]struct{}) ([]string, error) {
	if len(source) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(source))
	for key := range source {
		if _, ok := allowed[key]; !ok {
			return nil, fmt.Errorf("unsupported column %q", key)
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, nil
}

func appendInsertColumnsAndArgs[T any](columns *[]string, args *[]any, source map[string]T, allowed map[string]struct{}) error {
	keys, err := sortedColumnKeys(source, allowed)
	if err != nil {
		return err
	}
	for _, key := range keys {
		*columns = append(*columns, key)
		*args = append(*args, source[key])
	}
	return nil
}

func appendUpdateAssignmentsAndArgs[T any](assignments *[]string, args *[]any, source map[string]T, allowed map[string]struct{}) error {
	keys, err := sortedColumnKeys(source, allowed)
	if err != nil {
		return err
	}
	for _, key := range keys {
		*assignments = append(*assignments, fmt.Sprintf("%s = $%d", key, len(*args)+1))
		*args = append(*args, source[key])
	}
	return nil
}

func buildInsertMainStatement(table string, record *model.PersistentRecord) (string, []any, error) {
	columns := []string{"ltbase_schema_id", "ltbase_row_id", "ltbase_created_at", "ltbase_updated_at"}
	args := []any{record.SchemaID, record.RowID, record.CreatedAt, record.UpdatedAt}

	if record.DeletedAt != nil {
		columns = append(columns, "ltbase_deleted_at")
		args = append(args, *record.DeletedAt)
	}

	if err := appendInsertColumnsAndArgs(&columns, &args, record.TextItems, model.AllowedTextColumns); err != nil {
		return "", nil, err
	}
	if err := appendInsertColumnsAndArgs(&columns, &args, record.Int16Items, model.AllowedSmallintColumns); err != nil {
		return "", nil, err
	}
	if err := appendInsertColumnsAndArgs(&columns, &args, record.Int32Items, model.AllowedIntegerColumns); err != nil {
		return "", nil, err
	}
	if err := appendInsertColumnsAndArgs(&columns, &args, record.Int64Items, model.AllowedBigintColumns); err != nil {
		return "", nil, err
	}
	if err := appendInsertColumnsAndArgs(&columns, &args, record.Float64Items, model.AllowedDoubleColumns); err != nil {
		return "", nil, err
	}
	if err := appendInsertColumnsAndArgs(&columns, &args, record.UUIDItems, model.AllowedUUIDColumns); err != nil {
		return "", nil, err
	}

	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		sanitizeIdentifier(table),
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	return query, args, nil
}

func buildUpdateMainStatement(table string, record *model.PersistentRecord) (string, []any, error) {
	assignments := make([]string, 0, len(record.TextItems)+len(record.Int16Items)+len(record.Int32Items)+len(record.Int64Items)+len(record.Float64Items)+2)
	args := make([]any, 0, cap(assignments)+2)

	// Strictly-ordered per-row versions (#274): the cross-tier LWW rank has no
	// discriminator beyond the version timestamp, so two serialized
	// same-millisecond updates must not tie. GREATEST advances the stored
	// version past the row's previous one even when the wall clock has not
	// moved (or jumped backwards); the effective value is RETURNING'd so
	// change_log gets the identical stamp in the same transaction (#210
	// same-source contract).
	assignments = append(assignments, fmt.Sprintf("ltbase_updated_at = GREATEST($%d, ltbase_updated_at + 1)", len(args)+1))
	args = append(args, record.UpdatedAt)

	var deleted any
	if record.DeletedAt != nil {
		deleted = *record.DeletedAt
	}
	assignments = append(assignments, fmt.Sprintf("ltbase_deleted_at = $%d", len(args)+1))
	args = append(args, deleted)

	if err := appendUpdateAssignmentsAndArgs(&assignments, &args, record.TextItems, model.AllowedTextColumns); err != nil {
		return "", nil, fmt.Errorf("collect text update assignments: %w", err)
	}
	if err := appendUpdateAssignmentsAndArgs(&assignments, &args, record.Int16Items, model.AllowedSmallintColumns); err != nil {
		return "", nil, fmt.Errorf("collect smallint update assignments: %w", err)
	}
	if err := appendUpdateAssignmentsAndArgs(&assignments, &args, record.Int32Items, model.AllowedIntegerColumns); err != nil {
		return "", nil, fmt.Errorf("collect integer update assignments: %w", err)
	}
	if err := appendUpdateAssignmentsAndArgs(&assignments, &args, record.Int64Items, model.AllowedBigintColumns); err != nil {
		return "", nil, fmt.Errorf("collect bigint update assignments: %w", err)
	}
	if err := appendUpdateAssignmentsAndArgs(&assignments, &args, record.Float64Items, model.AllowedDoubleColumns); err != nil {
		return "", nil, fmt.Errorf("collect double update assignments: %w", err)
	}
	if err := appendUpdateAssignmentsAndArgs(&assignments, &args, record.UUIDItems, model.AllowedUUIDColumns); err != nil {
		return "", nil, fmt.Errorf("collect uuid update assignments: %w", err)
	}

	if len(assignments) == 0 {
		return "", nil, fmt.Errorf("no columns to update")
	}

	args = append(args, record.SchemaID, record.RowID)
	whereSchemaIdx := len(args) - 1
	whereRowIdx := len(args)

	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE ltbase_schema_id = $%d AND ltbase_row_id = $%d RETURNING ltbase_updated_at",
		sanitizeIdentifier(table),
		strings.Join(assignments, ", "),
		whereSchemaIdx,
		whereRowIdx,
	)

	return query, args, nil
}

func (r *DBPersistentRecordRepository) insertMainRow(ctx context.Context, tx pgx.Tx, table string, record *model.PersistentRecord) error {
	query, args, err := buildInsertMainStatement(table, record)
	if err != nil {
		return fmt.Errorf("build insert statement: %w", err)
	}
	zap.S().Debugw("insert main row", "query", query, "args", args)
	if _, err := tx.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("insert entity_main: %w", classifyPgError(err))
	}
	return nil
}

// updateMainRow applies the update and overwrites record.UpdatedAt with the
// EFFECTIVE version PostgreSQL computed (GREATEST over the row's previous
// version, #274) — callers must stamp change_log from record.UpdatedAt only
// after this returns, so both stores carry the identical version.
func (r *DBPersistentRecordRepository) updateMainRow(ctx context.Context, tx pgx.Tx, table string, record *model.PersistentRecord) error {
	query, args, err := buildUpdateMainStatement(table, record)
	if err != nil {
		return fmt.Errorf("build update statement: %w", err)
	}
	var effectiveUpdatedAt int64
	if err := tx.QueryRow(ctx, query, args...).Scan(&effectiveUpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return forma.WithOperatorDetail(forma.NotFoundf("entity not found (row=%s)", record.RowID),
				fmt.Errorf("schema=%d", record.SchemaID))
		}
		return fmt.Errorf("update entity_main: %w", classifyPgError(err))
	}
	record.UpdatedAt = effectiveUpdatedAt
	return nil
}

func (r *DBPersistentRecordRepository) loadMainRecord(ctx context.Context, table string, schemaID int16, rowID uuid.UUID) (*model.PersistentRecord, error) {
	query := fmt.Sprintf(
		"SELECT %s FROM %s WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2",
		model.EntityMainProjection,
		sanitizeIdentifier(table),
	)

	row := r.pool.QueryRow(ctx, query, schemaID, rowID)

	// Reuse the column scan buffers from postgres_row_scanner.go
	scanBuffers := newColumnScanBuffers()
	scanArgs := buildScanArgs(scanBuffers)

	if err := row.Scan(scanArgs...); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("select entity_main row: %w", err)
	}

	record := buildRecordFromScanBuffers(scanBuffers)
	model.CleanupEmptyMaps(record)

	return record, nil
}
