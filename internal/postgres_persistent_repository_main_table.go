package internal

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
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

func buildInsertMainStatement(table string, record *PersistentRecord) (string, []any, error) {
	columns := []string{"ltbase_schema_id", "ltbase_row_id", "ltbase_created_at", "ltbase_updated_at"}
	args := []any{record.SchemaID, record.RowID, record.CreatedAt, record.UpdatedAt}

	if record.DeletedAt != nil {
		columns = append(columns, "ltbase_deleted_at")
		args = append(args, *record.DeletedAt)
	}

	if keys, err := sortedColumnKeys(record.TextItems, allowedTextColumns); err != nil {
		return "", nil, err
	} else {
		for _, key := range keys {
			columns = append(columns, key)
			args = append(args, record.TextItems[key])
		}
	}

	if keys, err := sortedColumnKeys(record.Int16Items, allowedSmallintColumns); err != nil {
		return "", nil, err
	} else {
		for _, key := range keys {
			columns = append(columns, key)
			args = append(args, record.Int16Items[key])
		}
	}

	if keys, err := sortedColumnKeys(record.Int32Items, allowedIntegerColumns); err != nil {
		return "", nil, err
	} else {
		for _, key := range keys {
			columns = append(columns, key)
			args = append(args, record.Int32Items[key])
		}
	}

	if keys, err := sortedColumnKeys(record.Int64Items, allowedBigintColumns); err != nil {
		return "", nil, err
	} else {
		for _, key := range keys {
			columns = append(columns, key)
			args = append(args, record.Int64Items[key])
		}
	}

	if keys, err := sortedColumnKeys(record.Float64Items, allowedDoubleColumns); err != nil {
		return "", nil, err
	} else {
		for _, key := range keys {
			columns = append(columns, key)
			args = append(args, record.Float64Items[key])
		}
	}

	if keys, err := sortedColumnKeys(record.UUIDItems, allowedUUIDColumns); err != nil {
		return "", nil, err
	} else {
		for _, key := range keys {
			columns = append(columns, key)
			args = append(args, record.UUIDItems[key])
		}
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

	var deleted interface{}
	if record.DeletedAt != nil {
		deleted = *record.DeletedAt
	}
	assignments = append(assignments, fmt.Sprintf("ltbase_deleted_at = $%d", len(args)+1))
	args = append(args, deleted)

	if keys, err := sortedColumnKeys(record.TextItems, allowedTextColumns); err != nil {
		return "", nil, err
	} else {
		for _, key := range keys {
			assignments = append(assignments, fmt.Sprintf("%s = $%d", key, len(args)+1))
			args = append(args, record.TextItems[key])
		}
	}

	if keys, err := sortedColumnKeys(record.Int16Items, allowedSmallintColumns); err != nil {
		return "", nil, err
	} else {
		for _, key := range keys {
			assignments = append(assignments, fmt.Sprintf("%s = $%d", key, len(args)+1))
			args = append(args, record.Int16Items[key])
		}
	}

	if keys, err := sortedColumnKeys(record.Int32Items, allowedIntegerColumns); err != nil {
		return "", nil, err
	} else {
		for _, key := range keys {
			assignments = append(assignments, fmt.Sprintf("%s = $%d", key, len(args)+1))
			args = append(args, record.Int32Items[key])
		}
	}

	if keys, err := sortedColumnKeys(record.Int64Items, allowedBigintColumns); err != nil {
		return "", nil, err
	} else {
		for _, key := range keys {
			assignments = append(assignments, fmt.Sprintf("%s = $%d", key, len(args)+1))
			args = append(args, record.Int64Items[key])
		}
	}

	if keys, err := sortedColumnKeys(record.Float64Items, allowedDoubleColumns); err != nil {
		return "", nil, err
	} else {
		for _, key := range keys {
			assignments = append(assignments, fmt.Sprintf("%s = $%d", key, len(args)+1))
			args = append(args, record.Float64Items[key])
		}
	}

	if keys, err := sortedColumnKeys(record.UUIDItems, allowedUUIDColumns); err != nil {
		return "", nil, err
	} else {
		for _, key := range keys {
			assignments = append(assignments, fmt.Sprintf("%s = $%d", key, len(args)+1))
			args = append(args, record.UUIDItems[key])
		}
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

func (r *PostgresPersistentRecordRepository) insertMainRow(ctx context.Context, tx pgx.Tx, table string, record *PersistentRecord) error {
	query, args, err := buildInsertMainStatement(table, record)
	if err != nil {
		return err
	}
	zap.S().Debugw("insert main row", "query", query, "args", args)
	if _, err := tx.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("insert entity_main: %w", err)
	}
	return nil
}

func (r *PostgresPersistentRecordRepository) updateMainRow(ctx context.Context, tx pgx.Tx, table string, record *PersistentRecord) error {
	query, args, err := buildUpdateMainStatement(table, record)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("update entity_main: %w", err)
	}
	return nil
}

func (r *PostgresPersistentRecordRepository) loadMainRecord(ctx context.Context, table string, schemaID int16, rowID uuid.UUID) (*PersistentRecord, error) {
	query := fmt.Sprintf(
		"SELECT %s FROM %s WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2",
		entityMainProjection,
		sanitizeIdentifier(table),
	)

	row := r.pool.QueryRow(ctx, query, schemaID, rowID)

	// Count columns by kind to allocate proper buffer sizes
	textCount, smallCount, intCount, bigCount, doubleCount, uuidCount := 0, 0, 0, 0, 0, 0
	for _, desc := range entityMainColumnDescriptors {
		switch desc.kind {
		case columnKindText:
			textCount++
		case columnKindSmallint:
			smallCount++
		case columnKindInteger:
			intCount++
		case columnKindBigint:
			bigCount++
		case columnKindDouble:
			doubleCount++
		case columnKindUUID:
			uuidCount++
		}
	}

	textVals := make([]pgtype.Text, textCount)
	smallVals := make([]pgtype.Int2, smallCount)
	intVals := make([]pgtype.Int4, intCount)
	bigVals := make([]pgtype.Int8, bigCount)
	doubleVals := make([]pgtype.Float8, doubleCount)
	uuidVals := make([]pgtype.UUID, uuidCount)

	scanArgs := make([]any, 0, len(entityMainColumnDescriptors))
	typeIndex := make([]int, len(entityMainColumnDescriptors))
	textIdx, smallIdx, intIdx, bigIdx, doubleIdx, uuidIdx := 0, 0, 0, 0, 0, 0

	for i, desc := range entityMainColumnDescriptors {
		switch desc.kind {
		case columnKindText:
			scanArgs = append(scanArgs, &textVals[textIdx])
			typeIndex[i] = textIdx
			textIdx++
		case columnKindSmallint:
			scanArgs = append(scanArgs, &smallVals[smallIdx])
			typeIndex[i] = smallIdx
			smallIdx++
		case columnKindInteger:
			scanArgs = append(scanArgs, &intVals[intIdx])
			typeIndex[i] = intIdx
			intIdx++
		case columnKindBigint:
			scanArgs = append(scanArgs, &bigVals[bigIdx])
			typeIndex[i] = bigIdx
			bigIdx++
		case columnKindDouble:
			scanArgs = append(scanArgs, &doubleVals[doubleIdx])
			typeIndex[i] = doubleIdx
			doubleIdx++
		case columnKindUUID:
			scanArgs = append(scanArgs, &uuidVals[uuidIdx])
			typeIndex[i] = uuidIdx
			uuidIdx++
		}
	}

	if err := row.Scan(scanArgs...); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("select entity_main row: %w", err)
	}

	record := &PersistentRecord{
		TextItems:    make(map[string]string),
		Int16Items:   make(map[string]int16),
		Int32Items:   make(map[string]int32),
		Int64Items:   make(map[string]int64),
		Float64Items: make(map[string]float64),
		UUIDItems:    make(map[string]uuid.UUID),
	}

	for i, desc := range entityMainColumnDescriptors {
		switch desc.kind {
		case columnKindText:
			val := textVals[typeIndex[i]]
			if val.Valid {
				record.TextItems[desc.name] = val.String
			}
		case columnKindSmallint:
			val := smallVals[typeIndex[i]]
			if val.Valid {
				// Handle system fields specially
				if desc.name == "ltbase_schema_id" {
					record.SchemaID = val.Int16
				} else {
					record.Int16Items[desc.name] = val.Int16
				}
			}
		case columnKindInteger:
			val := intVals[typeIndex[i]]
			if val.Valid {
				record.Int32Items[desc.name] = val.Int32
			}
		case columnKindBigint:
			val := bigVals[typeIndex[i]]
			if val.Valid {
				// Handle system fields specially
				switch desc.name {
				case "ltbase_created_at":
					record.CreatedAt = val.Int64
				case "ltbase_updated_at":
					record.UpdatedAt = val.Int64
				case "ltbase_deleted_at":
					record.DeletedAt = &val.Int64
				default:
					record.Int64Items[desc.name] = val.Int64
				}
			}
		case columnKindDouble:
			val := doubleVals[typeIndex[i]]
			if val.Valid {
				record.Float64Items[desc.name] = val.Float64
			}
		case columnKindUUID:
			val := uuidVals[typeIndex[i]]
			if val.Valid {
				// Handle system field specially
				if desc.name == "ltbase_row_id" {
					record.RowID = uuid.UUID(val.Bytes)
				} else {
					record.UUIDItems[desc.name] = uuid.UUID(val.Bytes)
				}
			}
		}
	}

	// Remove empty maps to avoid nil-map checks elsewhere
	if len(record.TextItems) == 0 {
		record.TextItems = nil
	}
	if len(record.Int16Items) == 0 {
		record.Int16Items = nil
	}
	if len(record.Int32Items) == 0 {
		record.Int32Items = nil
	}
	if len(record.Int64Items) == 0 {
		record.Int64Items = nil
	}
	if len(record.Float64Items) == 0 {
		record.Float64Items = nil
	}
	if len(record.UUIDItems) == 0 {
		record.UUIDItems = nil
	}

	return record, nil
}
