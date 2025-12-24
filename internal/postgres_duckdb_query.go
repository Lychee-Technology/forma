package internal

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// ExecuteDuckDBFederatedQuery runs the DuckDB optimized query template using the provided
// FederatedAttributeQuery. It fetches dirty IDs from the Postgres change_log (if available),
// injects exclusions into the DuckDB WHERE clause, executes the query against the global
// DuckDB client, and returns matched PersistentRecords along with the total record count.
//
// Note: This implementation performs a best-effort scan of columns produced by the
// optimized query template. It mirrors the column ordering used by the Postgres template:
//   - main table projection (entity_main columns, order defined by entityMainColumnDescriptors)
//   - attributes_json (TEXT)
//   - total_records (bigint)
//   - total_pages (bigint)
//   - current_page (int)
func (r *PostgresPersistentRecordRepository) ExecuteDuckDBFederatedQuery(
	ctx context.Context,
	tables StorageTables,
	q *FederatedAttributeQuery,
	limit, offset int,
	attributeOrders []AttributeOrder,
	opts *FederatedQueryOptions,
) ([]*PersistentRecord, int64, error) {
	if q == nil {
		return nil, 0, fmt.Errorf("query cannot be nil")
	}

	// Execution plan instrumentation (if requested)
	startTotal := time.Now()
	if opts != nil && opts.IncludeExecutionPlan {
		if opts.ExecutionPlan == nil {
			opts.ExecutionPlan = &ExecutionPlan{Timings: map[string]int64{}, Notes: []string{}}
		}
		opts.ExecutionPlan.Notes = append(opts.ExecutionPlan.Notes, "ExecuteDuckDBFederatedQuery started")
	}

	// Acquire DuckDB client
	duck := GetDuckDBClient()
	if duck == nil || duck.DB == nil {
		if opts != nil && opts.IncludeExecutionPlan && opts.ExecutionPlan != nil {
			opts.ExecutionPlan.Notes = append(opts.ExecutionPlan.Notes, "duckdb client unavailable")
			opts.ExecutionPlan.Timings["duckdb_fetch"] = 0
			opts.ExecutionPlan.Timings["total"] = time.Since(startTotal).Milliseconds()
		}
		return nil, 0, fmt.Errorf("duckdb client not available")
	}

	// Fetch dirty IDs from Postgres change_log (flushed_at = 0) if change log table configured
	var dirtyIDs []uuid.UUID
	if tables.ChangeLog != "" {
		var err error
		dirtyIDs, err = r.FetchDirtyRowIDs(ctx, tables.ChangeLog, q.SchemaID)
		if err != nil {
			return nil, 0, fmt.Errorf("fetch dirty ids: %w", err)
		}
	}

	// Build template params (matching advanced duckdb template expectations)
	sqlParams := map[string]any{
		"EAVTable":             sanitizeIdentifier(tables.EAVData),
		"MainTable":            sanitizeIdentifier(tables.EntityMain),
		"ChangeLogTable":       sanitizeIdentifier(tables.ChangeLog),
		"MainProjection":       entityMainProjection,
		"SchemaID":             q.SchemaID,
		"UseMainTableAsAnchor": q.UseMainAsAnchor,
		"Anchor": map[string]any{
			"Condition": "1=1", // BuildDuckDBQuery will overwrite with actual where clause
		},
		"SortKeys": attributeOrders,
		"Limit":    limit,
		"Offset":   offset,
		"PageSize": limit,
	}

	startTranslate := time.Now()

	// Build dual clauses (PG pushdown + DuckDB logical) if metadata cache available
	var dualClauses *DualClauses
	var cache forma.SchemaAttributeCache
	if r.metadataCache != nil {
		if c, ok := r.metadataCache.GetSchemaCacheByID(q.SchemaID); ok {
			cache = c
		}
	}
	paramIndex := 0
	dc, err := ToDualClauses(q.Condition, sanitizeIdentifier(tables.EAVData), q.SchemaID, cache, &paramIndex)
	if err != nil {
		return nil, 0, fmt.Errorf("to dual clauses: %w", err)
	}
	dualClauses = &dc

	sqlStr, args, err := BuildDuckDBQuery(optimizedQuerySQLTemplateDuckDB, sqlParams, q, dirtyIDs, dualClauses)
	translateMs := time.Since(startTranslate).Milliseconds()
	if err != nil {
		return nil, 0, fmt.Errorf("build duckdb query: %w", err)
	}
	// Record translation info in execution plan if requested
	if opts != nil && opts.IncludeExecutionPlan && opts.ExecutionPlan != nil {
		dp := DataSourcePlan{
			Tier:              DataTierCold,
			Engine:            "duckdb",
			SQL:               sqlStr,
			RowEstimate:       0,
			PredicatePushdown: q.UseMainAsAnchor,
			ActualRows:        0,
			DurationMs:        0,
			Reason:            "duckdb template rendered",
		}
		opts.ExecutionPlan.Sources = append(opts.ExecutionPlan.Sources, dp)
		opts.ExecutionPlan.Timings["translate"] = translateMs
	}

	startQuery := time.Now()
	rows, err := duck.DB.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		// record failed fetch timing
		if opts != nil && opts.IncludeExecutionPlan && opts.ExecutionPlan != nil {
			opts.ExecutionPlan.Timings["duckdb_fetch"] = time.Since(startQuery).Milliseconds()
			opts.ExecutionPlan.Timings["total"] = time.Since(startTotal).Milliseconds()
			opts.ExecutionPlan.Notes = append(opts.ExecutionPlan.Notes, fmt.Sprintf("duckdb query failed: %v", err))
		}
		return nil, 0, fmt.Errorf("execute duckdb query: %w", err)
	}
	defer rows.Close()

	// Prepare scan buffers based on entityMainColumnDescriptors
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

	textVals := make([]sql.NullString, textCount)
	smallVals := make([]sql.NullInt64, smallCount)
	intVals := make([]sql.NullInt64, intCount)
	bigVals := make([]sql.NullInt64, bigCount)
	doubleVals := make([]sql.NullFloat64, doubleCount)
	uuidVals := make([]sql.NullString, uuidCount)

	var records []*PersistentRecord
	var totalRecords int64
	totalSet := false

	for rows.Next() {
		scanArgs := make([]any, 0, len(entityMainColumnDescriptors)+4)
		textIdx, smallIdx, intIdx, bigIdx, doubleIdx, uuidIdx := 0, 0, 0, 0, 0, 0

		for _, desc := range entityMainColumnDescriptors {
			switch desc.kind {
			case columnKindText:
				scanArgs = append(scanArgs, &textVals[textIdx])
				textIdx++
			case columnKindSmallint, columnKindInteger, columnKindBigint:
				// use NullInt64 for all integer kinds (casting later)
				switch desc.kind {
				case columnKindSmallint:
					scanArgs = append(scanArgs, &smallVals[smallIdx])
					smallIdx++
				case columnKindInteger:
					scanArgs = append(scanArgs, &intVals[intIdx])
					intIdx++
				case columnKindBigint:
					scanArgs = append(scanArgs, &bigVals[bigIdx])
					bigIdx++
				}
			case columnKindDouble:
				scanArgs = append(scanArgs, &doubleVals[doubleIdx])
				doubleIdx++
			case columnKindUUID:
				// DuckDB will typically return UUID as text; use NullString and parse
				scanArgs = append(scanArgs, &uuidVals[uuidIdx])
				uuidIdx++
			default:
				// fallback to NullString
				var ns sql.NullString
				scanArgs = append(scanArgs, &ns)
			}
		}

		// attributes_json, total_records, total_pages, current_page
		var attrsJSON sql.NullString
		var totalPages sql.NullInt64
		var currentPage sql.NullInt64
		var totalRec sql.NullInt64

		scanArgs = append(scanArgs, &attrsJSON, &totalRec, &totalPages, &currentPage)

		if err := rows.Scan(scanArgs...); err != nil {
			return nil, 0, fmt.Errorf("scan duckdb row: %w", err)
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

		textIdx, smallIdx, intIdx, bigIdx, doubleIdx, uuidIdx = 0, 0, 0, 0, 0, 0
		for i, desc := range entityMainColumnDescriptors {
			switch desc.kind {
			case columnKindText:
				val := textVals[textIdx]
				if val.Valid {
					record.TextItems[desc.name] = val.String
				}
				textIdx++
			case columnKindSmallint:
				val := smallVals[smallIdx]
				if val.Valid {
					if desc.name == "ltbase_schema_id" {
						record.SchemaID = int16(val.Int64)
					} else {
						record.Int16Items[desc.name] = int16(val.Int64)
					}
				}
				smallIdx++
			case columnKindInteger:
				val := intVals[intIdx]
				if val.Valid {
					record.Int32Items[desc.name] = int32(val.Int64)
				}
				intIdx++
			case columnKindBigint:
				val := bigVals[bigIdx]
				if val.Valid {
					// Handle known system columns
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
				bigIdx++
			case columnKindDouble:
				val := doubleVals[doubleIdx]
				if val.Valid {
					record.Float64Items[desc.name] = val.Float64
				}
				doubleIdx++
			case columnKindUUID:
				val := uuidVals[uuidIdx]
				if val.Valid {
					if parsed, err := uuid.Parse(val.String); err == nil {
						if desc.name == "ltbase_row_id" {
							record.RowID = parsed
						} else {
							record.UUIDItems[desc.name] = parsed
						}
					}
				}
				uuidIdx++
			}
			_ = i // keep linter happy if needed
		}

		// Parse attributes JSON
		if attrsJSON.Valid && attrsJSON.String != "" && attrsJSON.String != "[]" {
			var attributes []map[string]interface{}
			if err := json.Unmarshal([]byte(attrsJSON.String), &attributes); err != nil {
				return nil, 0, fmt.Errorf("unmarshal attributes json: %w", err)
			}
			record.OtherAttributes = make([]EAVRecord, 0, len(attributes))
			for _, attrObj := range attributes {
				attr := EAVRecord{
					SchemaID: int16(attrObj["schema_id"].(float64)),
					AttrID:   int16(attrObj["attr_id"].(float64)),
				}
				if rowIDStr, ok := attrObj["row_id"].(string); ok {
					if parsedUUID, err := uuid.Parse(rowIDStr); err == nil {
						attr.RowID = parsedUUID
					}
				}
				if indices, ok := attrObj["array_indices"].(string); ok {
					attr.ArrayIndices = indices
				}
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

		if !totalSet && totalRec.Valid {
			totalRecords = totalRec.Int64
			totalSet = true
		}

		records = append(records, record)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate duckdb rows: %w", err)
	}

	// finalize execution plan for duckdb
	if opts != nil && opts.IncludeExecutionPlan && opts.ExecutionPlan != nil {
		qMs := time.Since(startQuery).Milliseconds()
		if len(opts.ExecutionPlan.Sources) > 0 {
			idx := len(opts.ExecutionPlan.Sources) - 1
			dp := opts.ExecutionPlan.Sources[idx]
			dp.ActualRows = int64(len(records))
			dp.DurationMs = qMs
			opts.ExecutionPlan.Sources[idx] = dp
		}
		opts.ExecutionPlan.Timings["duckdb_fetch"] = qMs
		opts.ExecutionPlan.Timings["total"] = time.Since(startTotal).Milliseconds()
	}

	return records, totalRecords, nil
}
