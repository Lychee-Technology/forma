package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

type PostgresPersistentRecordRepository struct {
	pool          *pgxpool.Pool
	metadataCache *MetadataCache
	nowFunc       func() time.Time
}

func NewPostgresPersistentRecordRepository(pool *pgxpool.Pool, metadataCache *MetadataCache) *PostgresPersistentRecordRepository {
	return &PostgresPersistentRecordRepository{
		pool:          pool,
		metadataCache: metadataCache,
		nowFunc:       time.Now,
	}
}

func (r *PostgresPersistentRecordRepository) withClock(now func() time.Time) {
	if now == nil {
		return
	}
	r.nowFunc = now
}

var (
	textColumns = []string{
		"text_01", "text_02", "text_03", "text_04", "text_05",
		"text_06", "text_07", "text_08", "text_09", "text_10",
	}
	smallintColumns = []string{"smallint_01", "smallint_02", "smallint_03"}
	integerColumns  = []string{"integer_01", "integer_02", "integer_03"}
	bigintColumns   = []string{"bigint_01", "bigint_02", "bigint_03"}
	doubleColumns   = []string{"double_01", "double_02", "double_03"}
	uuidColumns     = []string{"uuid_01", "uuid_02"}
)

var (
	allowedTextColumns     = makeColumnSet(textColumns)
	allowedSmallintColumns = makeColumnSet(smallintColumns)
	allowedIntegerColumns  = makeColumnSet(integerColumns)
	allowedBigintColumns   = makeColumnSet(bigintColumns)
	allowedDoubleColumns   = makeColumnSet(doubleColumns)
	allowedUUIDColumns     = makeColumnSet(uuidColumns)
)

func makeColumnSet(columns []string) map[string]struct{} {
	set := make(map[string]struct{}, len(columns))
	for _, col := range columns {
		set[col] = struct{}{}
	}
	return set
}

type columnKind int

const (
	columnKindText columnKind = iota
	columnKindSmallint
	columnKindInteger
	columnKindBigint
	columnKindDouble
	columnKindUUID
)

type columnDescriptor struct {
	name string
	kind columnKind
}

var entityMainColumnDescriptors = []columnDescriptor{}
var entityMainProjection string

func init() {
	projection := make([]string, 0, 5+len(textColumns)+len(smallintColumns)+len(integerColumns)+len(bigintColumns)+len(doubleColumns)+len(uuidColumns))

	// Add system fields first
	entityMainColumnDescriptors = append(entityMainColumnDescriptors, columnDescriptor{name: "ltbase_schema_id", kind: columnKindSmallint})
	projection = append(projection, "ltbase_schema_id")

	entityMainColumnDescriptors = append(entityMainColumnDescriptors, columnDescriptor{name: "ltbase_row_id", kind: columnKindUUID})
	projection = append(projection, "ltbase_row_id")

	entityMainColumnDescriptors = append(entityMainColumnDescriptors, columnDescriptor{name: "ltbase_created_at", kind: columnKindBigint})
	projection = append(projection, "ltbase_created_at")

	entityMainColumnDescriptors = append(entityMainColumnDescriptors, columnDescriptor{name: "ltbase_updated_at", kind: columnKindBigint})
	projection = append(projection, "ltbase_updated_at")

	entityMainColumnDescriptors = append(entityMainColumnDescriptors, columnDescriptor{name: "ltbase_deleted_at", kind: columnKindBigint})
	projection = append(projection, "ltbase_deleted_at")

	// Add remaining text columns (skip text_01 as it's already added)
	for _, col := range textColumns {
		entityMainColumnDescriptors = append(entityMainColumnDescriptors, columnDescriptor{name: col, kind: columnKindText})
		projection = append(projection, col)
	}
	for _, col := range smallintColumns {
		entityMainColumnDescriptors = append(entityMainColumnDescriptors, columnDescriptor{name: col, kind: columnKindSmallint})
		projection = append(projection, col)
	}
	for _, col := range integerColumns {
		entityMainColumnDescriptors = append(entityMainColumnDescriptors, columnDescriptor{name: col, kind: columnKindInteger})
		projection = append(projection, col)
	}
	for _, col := range bigintColumns {
		entityMainColumnDescriptors = append(entityMainColumnDescriptors, columnDescriptor{name: col, kind: columnKindBigint})
		projection = append(projection, col)
	}
	for _, col := range doubleColumns {
		entityMainColumnDescriptors = append(entityMainColumnDescriptors, columnDescriptor{name: col, kind: columnKindDouble})
		projection = append(projection, col)
	}
	for _, col := range uuidColumns {
		entityMainColumnDescriptors = append(entityMainColumnDescriptors, columnDescriptor{name: col, kind: columnKindUUID})
		projection = append(projection, col)
	}
	entityMainProjection = strings.Join(projection, ", ")
}

func (r *PostgresPersistentRecordRepository) nowMillis() int64 {
	if r.nowFunc == nil {
		return time.Now().UnixMilli()
	}
	return r.nowFunc().UnixMilli()
}

func validateTables(tables StorageTables) error {
	if tables.EntityMain == "" {
		return fmt.Errorf("entity main table name cannot be empty")
	}
	if tables.EAVData == "" {
		return fmt.Errorf("eav table name cannot be empty")
	}
	return nil
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

func (r *PostgresPersistentRecordRepository) insertEAVAttributes(ctx context.Context, tx pgx.Tx, table string, attributes []EAVRecord) error {
	if len(attributes) == 0 {
		return nil
	}

	const batchSize = 500
	for i := 0; i < len(attributes); i += batchSize {
		end := i + batchSize
		if end > len(attributes) {
			end = len(attributes)
		}

		valuesClause, args, err := buildAttributeValuesClause(attributes[i:end])
		if err != nil {
			return err
		}

		query := fmt.Sprintf(
			"INSERT INTO %s (schema_id, row_id, attr_id, array_indices, value_text, value_numeric) VALUES %s",
			sanitizeIdentifier(table),
			valuesClause,
		)
		zap.S().Debugw("insert EAV attributes", "query", query, "args", args)
		if _, err := tx.Exec(ctx, query, args...); err != nil {
			return fmt.Errorf("insert eav attributes: %w", err)
		}
	}

	return nil
}

func (r *PostgresPersistentRecordRepository) replaceEAVAttributes(ctx context.Context, tx pgx.Tx, table string, schemaID int16, rowID uuid.UUID, attributes []EAVRecord) error {
	deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE schema_id = $1 AND row_id = $2", sanitizeIdentifier(table))
	if _, err := tx.Exec(ctx, deleteQuery, schemaID, rowID); err != nil {
		return fmt.Errorf("delete existing eav attributes: %w", err)
	}
	return r.insertEAVAttributes(ctx, tx, table, attributes)
}

const attributesCount = 6

func buildAttributeValuesClause(attributes []EAVRecord) (string, []any, error) {
	if len(attributes) == 0 {
		return "", nil, nil
	}
	var values []string
	args := make([]any, 0, len(attributes)*attributesCount)
	for idx, attr := range attributes {
		placeholderBase := idx*attributesCount + 1
		placeholders := make([]string, attributesCount)
		for i := 0; i < attributesCount; i++ {
			placeholders[i] = fmt.Sprintf("$%d", placeholderBase+i)
		}
		values = append(values, fmt.Sprintf("(%s)", strings.Join(placeholders, ", ")))
		args = append(args,
			attr.SchemaID,
			attr.RowID,
			attr.AttrID,
			attr.ArrayIndices,
			attr.ValueText,
			attr.ValueNumeric,
		)
	}
	return strings.Join(values, ", "), args, nil
}

func (r *PostgresPersistentRecordRepository) fetchAttributes(ctx context.Context, table string, schemaID int16, rowID uuid.UUID) ([]EAVRecord, error) {
	query := fmt.Sprintf(
		"SELECT schema_id, row_id, attr_id, array_indices, value_text, value_numeric FROM %s WHERE schema_id = $1 AND row_id = $2",
		sanitizeIdentifier(table),
	)
	rows, err := r.pool.Query(ctx, query, schemaID, rowID)
	if err != nil {
		return nil, fmt.Errorf("query eav attributes: %w", err)
	}
	defer rows.Close()

	var attributes []EAVRecord
	for rows.Next() {
		var attr EAVRecord
		if err := rows.Scan(
			&attr.SchemaID,
			&attr.RowID,
			&attr.AttrID,
			&attr.ArrayIndices,
			&attr.ValueText,
			&attr.ValueNumeric,
		); err != nil {
			return nil, fmt.Errorf("scan eav attribute: %w", err)
		}
		attributes = append(attributes, attr)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate eav attributes: %w", err)
	}

	return attributes, nil
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

func computeTotalPages(total int64, limit int) int {
	if total == 0 || limit <= 0 {
		return 0
	}
	return int((total + int64(limit) - 1) / int64(limit))
}

// runOptimizedQuery executes an optimized single-query approach that joins entity_main
// with aggregated EAV data, eliminating the N+1 query problem.
func (r *PostgresPersistentRecordRepository) runOptimizedQuery(
	ctx context.Context,
	tables StorageTables,
	schemaID int16,
	clause string,
	args []any,
	limit, offset int,
	attributeOrders []AttributeOrder,
	useMainTableAsAnchor bool,
) ([]*PersistentRecord, int64, error) {
	if clause == "" {
		return nil, 0, fmt.Errorf("query condition cannot be empty")
	}
	if schemaID <= 0 {
		return nil, 0, fmt.Errorf("schema id must be positive")
	}
	if limit <= 0 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}

	sqlParams := map[string]any{
		"EAVTable":             sanitizeIdentifier(tables.EAVData),
		"MainTable":            sanitizeIdentifier(tables.EntityMain),
		"MainProjection":       entityMainProjection,
		"SchemaID":             "$1",
		"UseMainTableAsAnchor": useMainTableAsAnchor,
		"Anchor": map[string]any{
			"Condition": clause,
		},
		"SortKeys": attributeOrders,
		"Limit":    fmt.Sprintf("$%d", len(args)+2),
		"Offset":   fmt.Sprintf("$%d", len(args)+3),
		"PageSize": fmt.Sprintf("$%d", len(args)+2),
	}

	query, err := renderTemplate(optimizedQuerySQLTemplate, sqlParams)
	if err != nil {
		return nil, 0, fmt.Errorf("build optimized query: %w", err)
	}

	queryArgs := make([]any, 0, len(args)+3)
	queryArgs = append(queryArgs, schemaID)
	queryArgs = append(queryArgs, args...)
	queryArgs = append(queryArgs, limit, offset)

	zap.S().Debugw("optimized query", "query", query, "args", queryArgs)

	rows, err := r.pool.Query(ctx, query, queryArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("execute optimized query: %w", err)
	}
	defer rows.Close()

	var records []*PersistentRecord
	var totalRecords int64
	totalSet := false

	for rows.Next() {
		record, total, err := r.scanOptimizedRow(rows)
		if err != nil {
			return nil, 0, err
		}

		if !totalSet {
			totalRecords = total
			totalSet = true
		}

		records = append(records, record)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate optimized query rows: %w", err)
	}

	return records, totalRecords, nil
}

// scanOptimizedRow scans a single row from the optimized query that includes
// entity_main columns plus JSON-aggregated EAV attributes.
func (r *PostgresPersistentRecordRepository) scanOptimizedRow(rows pgx.Rows) (*PersistentRecord, int64, error) {
	var (
		attrsJSON    []byte
		totalRecords int64
		totalPages   int64
		currentPage  int32
	)

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

	// Prepare scan targets for entity_main columns
	textVals := make([]pgtype.Text, textCount)
	smallVals := make([]pgtype.Int2, smallCount)
	intVals := make([]pgtype.Int4, intCount)
	bigVals := make([]pgtype.Int8, bigCount)
	doubleVals := make([]pgtype.Float8, doubleCount)
	uuidVals := make([]pgtype.UUID, uuidCount)

	scanArgs := make([]any, 0, len(entityMainColumnDescriptors)+4)

	// Add all entity_main column scan targets
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

	// Add JSON and pagination info scan targets
	scanArgs = append(scanArgs, &attrsJSON, &totalRecords, &totalPages, &currentPage)

	if err := rows.Scan(scanArgs...); err != nil {
		return nil, 0, fmt.Errorf("scan optimized row: %w", err)
	}

	// Build PersistentRecord
	record := &PersistentRecord{
		TextItems:    make(map[string]string),
		Int16Items:   make(map[string]int16),
		Int32Items:   make(map[string]int32),
		Int64Items:   make(map[string]int64),
		Float64Items: make(map[string]float64),
		UUIDItems:    make(map[string]uuid.UUID),
	}

	// Populate column values
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

	// Parse JSON attributes
	if len(attrsJSON) > 0 && string(attrsJSON) != "[]" {
		var attributes []map[string]interface{}
		if err := json.Unmarshal(attrsJSON, &attributes); err != nil {
			return nil, 0, fmt.Errorf("unmarshal attributes json: %w", err)
		}

		// Convert JSON objects to Attribute structs
		record.OtherAttributes = make([]EAVRecord, 0, len(attributes))
		for _, attrObj := range attributes {
			attr := EAVRecord{
				SchemaID: int16(attrObj["schema_id"].(float64)),
				AttrID:   int16(attrObj["attr_id"].(float64)),
			}

			// Parse row_id
			if rowIDStr, ok := attrObj["row_id"].(string); ok {
				if parsedUUID, err := uuid.Parse(rowIDStr); err == nil {
					attr.RowID = parsedUUID
				}
			}

			// Parse array_indices
			if indices, ok := attrObj["array_indices"].(string); ok {
				attr.ArrayIndices = indices
			}

			// Parse typed values
			if valueText, ok := attrObj["value_text"].(string); ok {
				attr.ValueText = &valueText
			}
			if valueNumeric, ok := attrObj["value_numeric"].(float64); ok {
				attr.ValueNumeric = &valueNumeric
			}
			record.OtherAttributes = append(record.OtherAttributes, attr)
		}
	}

	// Clean up empty maps
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

	return record, totalRecords, nil
}

func (r *PostgresPersistentRecordRepository) InsertPersistentRecord(ctx context.Context, tables StorageTables, record *PersistentRecord) error {
	if record == nil {
		return fmt.Errorf("record cannot be nil")
	}
	if err := validateTables(tables); err != nil {
		return err
	}

	now := r.nowMillis()
	record.CreatedAt = now
	record.UpdatedAt = now

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) // no-op if committed

	if err := r.insertMainRow(ctx, tx, tables.EntityMain, record); err != nil {
		return err
	}

	if err := r.insertEAVAttributes(ctx, tx, tables.EAVData, record.OtherAttributes); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

func (r *PostgresPersistentRecordRepository) UpdatePersistentRecord(ctx context.Context, tables StorageTables, record *PersistentRecord) error {
	if record == nil {
		return fmt.Errorf("record cannot be nil")
	}
	if err := validateTables(tables); err != nil {
		return err
	}

	record.UpdatedAt = r.nowMillis()

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := r.updateMainRow(ctx, tx, tables.EntityMain, record); err != nil {
		return err
	}

	if err := r.replaceEAVAttributes(ctx, tx, tables.EAVData, record.SchemaID, record.RowID, record.OtherAttributes); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

func (r *PostgresPersistentRecordRepository) DeletePersistentRecord(ctx context.Context, tables StorageTables, schemaID int16, rowID uuid.UUID) error {
	if err := validateTables(tables); err != nil {
		return err
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	deleteMain := fmt.Sprintf("DELETE FROM %s WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2", sanitizeIdentifier(tables.EntityMain))
	if _, err := tx.Exec(ctx, deleteMain, schemaID, rowID); err != nil {
		return fmt.Errorf("delete entity_main row: %w", err)
	}

	deleteEAV := fmt.Sprintf("DELETE FROM %s WHERE schema_id = $1 AND row_id = $2", sanitizeIdentifier(tables.EAVData))
	if _, err := tx.Exec(ctx, deleteEAV, schemaID, rowID); err != nil {
		return fmt.Errorf("delete eav attributes: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

func (r *PostgresPersistentRecordRepository) GetPersistentRecord(ctx context.Context, tables StorageTables, schemaID int16, rowID uuid.UUID) (*PersistentRecord, error) {
	if err := validateTables(tables); err != nil {
		return nil, err
	}

	record, err := r.loadMainRecord(ctx, tables.EntityMain, schemaID, rowID)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, nil
	}

	attributes, err := r.fetchAttributes(ctx, tables.EAVData, schemaID, rowID)
	if err != nil {
		return nil, err
	}
	record.OtherAttributes = attributes

	return record, nil
}

func (r *PostgresPersistentRecordRepository) QueryPersistentRecords(ctx context.Context, query *PersistentRecordQuery) (*PersistentRecordPage, error) {
	zap.S().Debugw("query persistent records", "query", query)
	if query == nil {
		return nil, fmt.Errorf("query cannot be nil")
	}
	if err := validateTables(query.Tables); err != nil {
		return nil, err
	}
	if query.SchemaID <= 0 {
		return nil, fmt.Errorf("schema id must be positive")
	}

	limit := query.Limit
	if limit <= 0 {
		limit = 50
	}
	offset := query.Offset
	if offset < 0 {
		offset = 0
	}

	attrQuery := AttributeQuery{
		SchemaID:  query.SchemaID,
		Condition: query.Condition,
	}

	// Get schema cache first for checking main table conditions
	var cache forma.SchemaAttributeCache
	if query.SchemaID > 0 && r.metadataCache != nil {
		if cacheLocal, ok := r.metadataCache.GetSchemaCacheByID(query.SchemaID); ok {
			cache = cacheLocal
		} else {
			return nil, fmt.Errorf("no cache for schema id %d", query.SchemaID)
		}
	}

	useMainTableAsAnchor := hasMainTableCondition(query.Condition, cache)

	conditions, args, err := r.buildHybridConditions(
		sanitizeIdentifier(query.Tables.EAVData),
		sanitizeIdentifier(query.Tables.EntityMain),
		attrQuery,
		1,
		useMainTableAsAnchor,
	)
	if err != nil {
		return nil, fmt.Errorf("build hybrid conditions: %w", err)
	}

	// Use the optimized single-query approach that eliminates N+1 queries
	records, totalRecords, err := r.runOptimizedQuery(
		ctx,
		query.Tables,
		query.SchemaID,
		conditions,
		args,
		limit,
		offset,
		query.AttributeOrders,
		useMainTableAsAnchor,
	)
	if err != nil {
		return nil, err
	}

	currentPage := 1
	if limit > 0 {
		currentPage = offset/limit + 1
	}

	return &PersistentRecordPage{
		Records:      records,
		TotalRecords: totalRecords,
		TotalPages:   computeTotalPages(totalRecords, limit),
		CurrentPage:  currentPage,
	}, nil
}

func isMainTableColumn(name string) bool {
	for _, desc := range entityMainColumnDescriptors {
		if desc.name == name {
			return true
		}
	}
	if _, ok := allowedTextColumns[name]; ok {
		return true
	}
	if _, ok := allowedSmallintColumns[name]; ok {
		return true
	}
	if _, ok := allowedIntegerColumns[name]; ok {
		return true
	}
	if _, ok := allowedBigintColumns[name]; ok {
		return true
	}
	if _, ok := allowedDoubleColumns[name]; ok {
		return true
	}
	if _, ok := allowedUUIDColumns[name]; ok {
		return true
	}
	return false
}

func hasMainTableCondition(cond forma.Condition, cache forma.SchemaAttributeCache) bool {
	if cond == nil {
		// An empty condition implies no filtering; treat as having main table condition
		return true
	}
	switch c := cond.(type) {
	case *forma.CompositeCondition:
		if c == nil {
			return false
		}
		for _, child := range c.Conditions {
			if hasMainTableCondition(child, cache) {
				return true
			}
		}
		return false
	case *forma.KvCondition:
		if c == nil {
			return false
		}
		// Check if it's a raw main table column name
		if isMainTableColumn(c.Attr) {
			return true
		}
		// Check if it's an attribute with column_binding to main table
		if cache != nil {
			if meta, ok := cache[c.Attr]; ok {
				if meta.Location() == forma.AttributeStorageLocationMain {
					return true
				}
			}
		}
		return false
	default:
		return false
	}
}

func getMainColumnDescriptor(name string) *columnDescriptor {
	for _, desc := range entityMainColumnDescriptors {
		if desc.name == name {
			return &desc
		}
	}
	return nil
}

// parseKvConditionForColumnWithMeta parses a KvCondition for a specific column name with metadata
// The meta parameter is used to determine the encoding for date/time values
func parseKvConditionForColumnWithMeta(kv *forma.KvCondition, colName string, meta *forma.AttributeMetadata) (string, any, error) {
	parts := strings.SplitN(kv.Value, ":", 2)
	var opStr, valStr string
	if len(parts) == 1 {
		opStr, valStr = "equals", kv.Value
	} else {
		opStr, valStr = parts[0], parts[1]
	}

	var sqlOp string
	switch opStr {
	case "equals":
		sqlOp = "="
	case "gt":
		sqlOp = ">"
	case "gte":
		sqlOp = ">="
	case "lt":
		sqlOp = "<"
	case "lte":
		sqlOp = "<="
	case "not_equals":
		sqlOp = "!="
	case "starts_with":
		sqlOp = "LIKE"
		valStr = valStr + "%"
	case "contains":
		sqlOp = "LIKE"
		valStr = "%" + valStr + "%"
	default:
		return "", nil, fmt.Errorf("unsupported operator: %s", opStr)
	}

	desc := getMainColumnDescriptor(colName)
	if desc == nil {
		return "", nil, fmt.Errorf("unknown main table column: %s", colName)
	}

	var parsedValue any

	switch desc.kind {
	case columnKindText:
		parsedValue = valStr
	case columnKindSmallint, columnKindInteger, columnKindBigint, columnKindDouble:
		// Check if this is a date/time field that needs conversion
		if meta != nil && (meta.ValueType == forma.ValueTypeDate || meta.ValueType == forma.ValueTypeDateTime) {
			convertedVal, err := convertDateValueForQuery(valStr, meta)
			if err != nil {
				return "", nil, err
			}
			parsedValue = convertedVal
		} else {
			parsedValue = tryParseNumber(valStr)
		}
	case columnKindUUID:
		parsedValue = valStr
	default:
		parsedValue = valStr
	}

	return sqlOp, parsedValue, nil
}

// convertDateValueForQuery converts a date value string to the appropriate format for querying.
// It supports both ISO 8601 format strings and Unix millisecond timestamps as input.
// The output format is determined by the storage encoding in metadata.
func convertDateValueForQuery(valStr string, meta *forma.AttributeMetadata) (any, error) {
	// First, try to parse as ISO 8601 format
	parsedTime, err := time.Parse(time.RFC3339, valStr)
	if err != nil {
		// Try parsing as Unix milliseconds
		parsedInt64, parseErr := strconv.ParseInt(valStr, 10, 64)
		if parseErr != nil {
			return nil, fmt.Errorf("invalid date value: expected ISO 8601 format or unix milliseconds, got '%s'", valStr)
		}
		parsedTime = time.UnixMilli(parsedInt64)
	}

	// Convert based on storage encoding
	if meta.ColumnBinding != nil {
		encoding := meta.ColumnBinding.Encoding
		switch encoding {
		case forma.MainColumnEncodingUnixMs:
			// Return Unix milliseconds as int64 for bigint column
			return parsedTime.UnixMilli(), nil
		case forma.MainColumnEncodingISO8601:
			// Return ISO 8601 string for text column
			return parsedTime.Format(time.RFC3339), nil
		}
	}

	// Default: return as Unix milliseconds
	return parsedTime.UnixMilli(), nil
}

func (r *PostgresPersistentRecordRepository) buildHybridConditions(
	eavTable, mainTable string,
	query AttributeQuery,
	initArgIndex int,
	useMainTableAsAnchor bool,
) (string, []any, error) {
	if query.Condition == nil {
		return "1=1", nil, nil
	}

	var build func(c forma.Condition) (string, []any, error)
	argCounter := initArgIndex

	var cache forma.SchemaAttributeCache
	if query.SchemaID > 0 {
		var schemaName string
		var ok bool
		if r.metadataCache != nil {
			schemaName, ok = r.metadataCache.GetSchemaName(query.SchemaID)
		}
		if ok {
			if cacheLocal, ok := r.metadataCache.GetSchemaCache(schemaName); ok {
				cache = cacheLocal
			}
		}
	}

	build = func(c forma.Condition) (string, []any, error) {
		switch cond := c.(type) {
		case *forma.CompositeCondition:
			if len(cond.Conditions) == 0 {
				return "", nil, nil
			}
			var parts []string
			var args []any
			joiner := " AND "
			if cond.Logic == forma.LogicOr {
				joiner = " OR "
			}

			for _, child := range cond.Conditions {
				p, a, err := build(child)
				if err != nil {
					return "", nil, err
				}
				if p != "" {
					parts = append(parts, fmt.Sprintf("(%s)", p))
					args = append(args, a...)
				}
			}
			if len(parts) == 0 {
				return "", nil, nil
			}
			return strings.Join(parts, joiner), args, nil

		case *forma.KvCondition:
			// Check if it's a raw main table column OR an attribute with column_binding
			var colName string
			if isMainTableColumn(cond.Attr) {
				// Raw column name like text_01, text_02, etc.
				colName = cond.Attr
			} else if cache != nil {
				// Check if attribute has column_binding to main table
				if meta, ok := cache[cond.Attr]; ok {
					if meta.ColumnBinding != nil {
						colName = string(meta.ColumnBinding.ColumnName)
					}
				}
			}

			if colName != "" {
				// Main table column query - pass metadata for date/time conversion
				var attrMeta *forma.AttributeMetadata
				if cache != nil {
					if meta, ok := cache[cond.Attr]; ok {
						attrMeta = &meta
					}
				}
				op, val, err := parseKvConditionForColumnWithMeta(cond, colName, attrMeta)
				if err != nil {
					return "", nil, err
				}

				argCounter++
				placeholder := fmt.Sprintf("$%d", argCounter)

				if useMainTableAsAnchor {
					return fmt.Sprintf("m.%s %s %s", sanitizeIdentifier(colName), op, placeholder), []any{val}, nil
				} else {
					return fmt.Sprintf("EXISTS (SELECT 1 FROM %s m WHERE m.ltbase_row_id = t.row_id AND m.%s %s %s)",
						sanitizeIdentifier(mainTable), sanitizeIdentifier(colName), op, placeholder), []any{val}, nil
				}
			} else {
				// EAV table query
				if cache == nil {
					return "", nil, fmt.Errorf("schema metadata cache not available for schema_id %d", query.SchemaID)
				}
				gen := NewSQLGenerator()
				// SQLGenerator expects paramIndex to be the last used index, and will increment before using
				// So we pass argCounter (which is already the last used index after main table conditions)
				pIdx := argCounter
				clause, args, err := gen.ToSqlClauses(cond, eavTable, query.SchemaID, cache, &pIdx)
				if err != nil {
					return "", nil, err
				}
				// pIdx now holds the last used index after SQLGenerator's operations
				argCounter = pIdx

				if useMainTableAsAnchor {
					clause = strings.ReplaceAll(clause, "e.row_id", "m.ltbase_row_id")
					clause = strings.ReplaceAll(clause, "e.schema_id", "m.ltbase_schema_id")
				} else {
					clause = strings.ReplaceAll(clause, "e.row_id", "t.row_id")
					clause = strings.ReplaceAll(clause, "e.schema_id", "t.schema_id")
				}
				return clause, args, nil
			}
		default:
			return "", nil, fmt.Errorf("unsupported condition type %T", c)
		}
	}

	return build(query.Condition)
}
