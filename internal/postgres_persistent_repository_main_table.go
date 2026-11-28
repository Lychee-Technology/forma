package internal

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

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

	// ON CONFLICT DO NOTHING makes a live row with the same ID a zero-row
	// insert rather than a 23505 (#651): inside a Tx a failed statement
	// aborts the transaction, so a duplicate caller-supplied RowID could not
	// be the expected, non-poisoning ErrUniqueConflict otherwise.
	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (ltbase_schema_id, ltbase_row_id) DO NOTHING",
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
	if err := checkClearedColumns(record); err != nil {
		return "", nil, fmt.Errorf("collect cleared columns: %w", err)
	}
	for _, column := range record.ClearedColumns {
		assignments = append(assignments, column+" = NULL")
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

// retainStoredColumns copies into record every main column stored holds
// that record does not set, except the ones record clears. buildUpdateMainStatement
// assigns only the columns record sets, and NULL to the ones it clears, so
// the UPDATE leaves any other column as stored; with it copied in, record is
// the row as the update stores it. A nil stored, a row the merge found
// missing, adds nothing.
func retainStoredColumns(record, stored *model.PersistentRecord) {
	if stored == nil {
		return
	}
	cleared := record.ClearedColumns
	retainColumns(&record.TextItems, stored.TextItems, cleared)
	retainColumns(&record.Int16Items, stored.Int16Items, cleared)
	retainColumns(&record.Int32Items, stored.Int32Items, cleared)
	retainColumns(&record.Int64Items, stored.Int64Items, cleared)
	retainColumns(&record.Float64Items, stored.Float64Items, cleared)
	retainColumns(&record.UUIDItems, stored.UUIDItems, cleared)
}

func retainColumns[T any](record *map[string]T, stored map[string]T, cleared []string) {
	for column, value := range stored {
		if _, set := (*record)[column]; set || slices.Contains(cleared, column) {
			continue
		}
		if *record == nil {
			*record = make(map[string]T, len(stored))
		}
		(*record)[column] = value
	}
}

// insertMainRow inserts record's main row and reports whether it did: false
// means a row with its (schema, row ID) already exists, and nothing was
// written.
func (r *DBPersistentRecordRepository) insertMainRow(ctx context.Context, tx pgx.Tx, table string, record *model.PersistentRecord) (bool, error) {
	query, args, err := buildInsertMainStatement(table, record)
	if err != nil {
		return false, fmt.Errorf("build insert statement: %w", err)
	}
	zap.S().Debugw("insert main row", "query", query, "args", args)
	tag, err := tx.Exec(ctx, query, args...)
	if err != nil {
		return false, fmt.Errorf("insert entity_main: %w", err)
	}
	return tag.RowsAffected() == 1, nil
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
		return fmt.Errorf("update entity_main: %w", err)
	}
	record.UpdatedAt = effectiveUpdatedAt
	return nil
}

// checkClearedColumns checks the columns record clears (ClearedColumns,
// #938): each must be a writable entity_main column, never a system one, and
// none may also be set, since the record would then ask for two values.
// buildUpdateMainStatement interpolates the names into the statement, so
// the allowlists are what keep it safe.
func checkClearedColumns(record *model.PersistentRecord) error {
	for _, column := range record.ClearedColumns {
		if !model.IsMainTableColumn(column) || model.IsSystemColumn(column) {
			return fmt.Errorf("unsupported cleared column %q", column)
		}
		if recordSetsColumn(record, column) {
			return fmt.Errorf("row %s both sets and clears column %q", record.RowID, column)
		}
	}
	return nil
}

// recordSetsColumn reports whether record assigns column a value.
func recordSetsColumn(record *model.PersistentRecord, column string) bool {
	_, text := record.TextItems[column]
	_, int16Set := record.Int16Items[column]
	_, int32Set := record.Int32Items[column]
	_, int64Set := record.Int64Items[column]
	_, float64Set := record.Float64Items[column]
	_, uuidSet := record.UUIDItems[column]
	return text || int16Set || int32Set || int64Set || float64Set || uuidSet
}
