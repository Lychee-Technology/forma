package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

func computeTotalPages(total int64, limit int) int {
	if total == 0 || limit <= 0 {
		return 0
	}
	return int((total + int64(limit) - 1) / int64(limit))
}

func (r *PostgresPersistentRecordRepository) StreamOptimizedQuery(
	ctx context.Context,
	tables StorageTables,
	schemaID int16,
	clause string,
	args []any,
	limit, offset int,
	attributeOrders []AttributeOrder,
	useMainTableAsAnchor bool,
	rowHandler func(*PersistentRecord) error,
) (int64, error) {
	if clause == "" {
		return 0, fmt.Errorf("query condition cannot be empty")
	}
	if schemaID <= 0 {
		return 0, fmt.Errorf("schema id must be positive")
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
		"ChangeLogTable":       sanitizeIdentifier(tables.ChangeLog),
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
		return 0, fmt.Errorf("build optimized query: %w", err)
	}

	queryArgs := make([]any, 0, len(args)+3)
	queryArgs = append(queryArgs, schemaID)
	queryArgs = append(queryArgs, args...)
	queryArgs = append(queryArgs, limit, offset)

	zap.S().Debugw("optimized query (stream)", "query", query, "args", queryArgs)

	rows, err := r.pool.Query(ctx, query, queryArgs...)
	if err != nil {
		return 0, fmt.Errorf("execute optimized query: %w", err)
	}
	defer rows.Close()

	var totalRecords int64
	totalSet := false

	for rows.Next() {
		record, total, err := r.scanOptimizedRow(rows)
		if err != nil {
			return 0, err
		}

		if !totalSet {
			totalRecords = total
			totalSet = true
		}

		if rowHandler != nil {
			if err := rowHandler(record); err != nil {
				return totalRecords, err
			}
		}
	}

	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate optimized query rows: %w", err)
	}

	return totalRecords, nil
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

	var records []*PersistentRecord
	totalRecords, appendErr := r.StreamOptimizedQuery(ctx, tables, schemaID, clause, args, limit, offset, attributeOrders, useMainTableAsAnchor, func(rp *PersistentRecord) error {
		records = append(records, rp)
		return nil
	})
	if appendErr != nil {
		return nil, 0, appendErr
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
