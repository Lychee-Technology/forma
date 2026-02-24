package internal

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

// classifyPgError converts well-known PostgreSQL error codes to sentinel errors.
// Currently handles:
//   - 23505 unique_violation → forma.ErrConflict
func classifyPgError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return fmt.Errorf("%s: %w", pgErr.Detail, forma.ErrConflict)
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

func buildInsertMainStatement(table string, record *PersistentRecord) (string, []any, error) {
	columns := []string{"ltbase_schema_id", "ltbase_row_id", "ltbase_created_at", "ltbase_updated_at"}
	args := []any{record.SchemaID, record.RowID, record.CreatedAt, record.UpdatedAt}

	if record.DeletedAt != nil {
		columns = append(columns, "ltbase_deleted_at")
		args = append(args, *record.DeletedAt)
	}

	if err := appendInsertColumnsAndArgs(&columns, &args, record.TextItems, allowedTextColumns); err != nil {
		return "", nil, err
	}
	if err := appendInsertColumnsAndArgs(&columns, &args, record.Int16Items, allowedSmallintColumns); err != nil {
		return "", nil, err
	}
	if err := appendInsertColumnsAndArgs(&columns, &args, record.Int32Items, allowedIntegerColumns); err != nil {
		return "", nil, err
	}
	if err := appendInsertColumnsAndArgs(&columns, &args, record.Int64Items, allowedBigintColumns); err != nil {
		return "", nil, err
	}
	if err := appendInsertColumnsAndArgs(&columns, &args, record.Float64Items, allowedDoubleColumns); err != nil {
		return "", nil, err
	}
	if err := appendInsertColumnsAndArgs(&columns, &args, record.UUIDItems, allowedUUIDColumns); err != nil {
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

func buildUpdateMainStatement(table string, record *PersistentRecord) (string, []any, error) {
	assignments := make([]string, 0, len(record.TextItems)+len(record.Int16Items)+len(record.Int32Items)+len(record.Int64Items)+len(record.Float64Items)+2)
	args := make([]any, 0, cap(assignments)+2)

	assignments = append(assignments, fmt.Sprintf("ltbase_updated_at = $%d", len(args)+1))
	args = append(args, record.UpdatedAt)

	var deleted any
	if record.DeletedAt != nil {
		deleted = *record.DeletedAt
	}
	assignments = append(assignments, fmt.Sprintf("ltbase_deleted_at = $%d", len(args)+1))
	args = append(args, deleted)

	if err := appendUpdateAssignmentsAndArgs(&assignments, &args, record.TextItems, allowedTextColumns); err != nil {
		return "", nil, err
	}
	if err := appendUpdateAssignmentsAndArgs(&assignments, &args, record.Int16Items, allowedSmallintColumns); err != nil {
		return "", nil, err
	}
	if err := appendUpdateAssignmentsAndArgs(&assignments, &args, record.Int32Items, allowedIntegerColumns); err != nil {
		return "", nil, err
	}
	if err := appendUpdateAssignmentsAndArgs(&assignments, &args, record.Int64Items, allowedBigintColumns); err != nil {
		return "", nil, err
	}
	if err := appendUpdateAssignmentsAndArgs(&assignments, &args, record.Float64Items, allowedDoubleColumns); err != nil {
		return "", nil, err
	}
	if err := appendUpdateAssignmentsAndArgs(&assignments, &args, record.UUIDItems, allowedUUIDColumns); err != nil {
		return "", nil, err
	}

	if len(assignments) == 0 {
		return "", nil, fmt.Errorf("no columns to update")
	}

	args = append(args, record.SchemaID, record.RowID)
	whereSchemaIdx := len(args) - 1
	whereRowIdx := len(args)

	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE ltbase_schema_id = $%d AND ltbase_row_id = $%d",
		sanitizeIdentifier(table),
		strings.Join(assignments, ", "),
		whereSchemaIdx,
		whereRowIdx,
	)

	return query, args, nil
}

func (r *DBPersistentRecordRepository) insertMainRow(ctx context.Context, tx pgx.Tx, table string, record *PersistentRecord) error {
	query, args, err := buildInsertMainStatement(table, record)
	if err != nil {
		return err
	}
	zap.S().Debugw("insert main row", "query", query, "args", args)
	if _, err := tx.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("insert entity_main: %w", classifyPgError(err))
	}
	return nil
}

func (r *DBPersistentRecordRepository) updateMainRow(ctx context.Context, tx pgx.Tx, table string, record *PersistentRecord) error {
	query, args, err := buildUpdateMainStatement(table, record)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("update entity_main: %w", err)
	}
	return nil
}

func (r *DBPersistentRecordRepository) loadMainRecord(ctx context.Context, table string, schemaID int16, rowID uuid.UUID) (*PersistentRecord, error) {
	query := fmt.Sprintf(
		"SELECT %s FROM %s WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2",
		entityMainProjection,
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
	cleanupEmptyMaps(record)

	return record, nil
}
