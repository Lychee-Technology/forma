package federated

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// duckDBScanBuffers holds pre-allocated scan buffers for DuckDB row scanning.
type duckDBScanBuffers struct {
	textVals   []sql.NullString
	smallVals  []sql.NullInt64
	intVals    []sql.NullInt64
	bigVals    []sql.NullInt64
	doubleVals []sql.NullFloat64
	uuidVals   []sql.NullString
}

// newDuckDBScanBuffers creates scan buffers sized according to entityMainColumnDescriptors.
func newDuckDBScanBuffers() *duckDBScanBuffers {
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

	return &duckDBScanBuffers{
		textVals:   make([]sql.NullString, textCount),
		smallVals:  make([]sql.NullInt64, smallCount),
		intVals:    make([]sql.NullInt64, intCount),
		bigVals:    make([]sql.NullInt64, bigCount),
		doubleVals: make([]sql.NullFloat64, doubleCount),
		uuidVals:   make([]sql.NullString, uuidCount),
	}
}

// buildScanArgs constructs scan arguments for a single DuckDB row.
// Returns the scan args slice and pointers for metadata columns.
func (b *duckDBScanBuffers) buildScanArgs() ([]any, *sql.NullString, *sql.NullInt64, *sql.NullInt64, *sql.NullInt64) {
	scanArgs := make([]any, 0, len(entityMainColumnDescriptors)+4)
	textIdx, smallIdx, intIdx, bigIdx, doubleIdx, uuidIdx := 0, 0, 0, 0, 0, 0

	for _, desc := range entityMainColumnDescriptors {
		switch desc.kind {
		case columnKindText:
			scanArgs = append(scanArgs, &b.textVals[textIdx])
			textIdx++
		case columnKindSmallint:
			scanArgs = append(scanArgs, &b.smallVals[smallIdx])
			smallIdx++
		case columnKindInteger:
			scanArgs = append(scanArgs, &b.intVals[intIdx])
			intIdx++
		case columnKindBigint:
			scanArgs = append(scanArgs, &b.bigVals[bigIdx])
			bigIdx++
		case columnKindDouble:
			scanArgs = append(scanArgs, &b.doubleVals[doubleIdx])
			doubleIdx++
		case columnKindUUID:
			// DuckDB will typically return UUID as text; use NullString and parse
			scanArgs = append(scanArgs, &b.uuidVals[uuidIdx])
			uuidIdx++
		default:
			// fallback to NullString
			var ns sql.NullString
			scanArgs = append(scanArgs, &ns)
		}
	}

	// attributes_json, total_records, total_pages, current_page
	var attrsJSON sql.NullString
	var totalRec sql.NullInt64
	var totalPages sql.NullInt64
	var currentPage sql.NullInt64

	scanArgs = append(scanArgs, &attrsJSON, &totalRec, &totalPages, &currentPage)

	return scanArgs, &attrsJSON, &totalRec, &totalPages, &currentPage
}

// buildRecordFromBuffers constructs a PersistentRecord from the scanned buffer values.
func (b *duckDBScanBuffers) buildRecordFromBuffers() *PersistentRecord {
	record := &PersistentRecord{
		TextItems:    make(map[string]string),
		Int16Items:   make(map[string]int16),
		Int32Items:   make(map[string]int32),
		Int64Items:   make(map[string]int64),
		Float64Items: make(map[string]float64),
		UUIDItems:    make(map[string]uuid.UUID),
	}

	textIdx, smallIdx, intIdx, bigIdx, doubleIdx, uuidIdx := 0, 0, 0, 0, 0, 0
	for _, desc := range entityMainColumnDescriptors {
		switch desc.kind {
		case columnKindText:
			val := b.textVals[textIdx]
			if val.Valid {
				record.TextItems[desc.name] = val.String
			}
			textIdx++
		case columnKindSmallint:
			val := b.smallVals[smallIdx]
			if val.Valid {
				if desc.name == "ltbase_schema_id" {
					record.SchemaID = int16(val.Int64)
				} else {
					record.Int16Items[desc.name] = int16(val.Int64)
				}
			}
			smallIdx++
		case columnKindInteger:
			val := b.intVals[intIdx]
			if val.Valid {
				record.Int32Items[desc.name] = int32(val.Int64)
			}
			intIdx++
		case columnKindBigint:
			val := b.bigVals[bigIdx]
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
			val := b.doubleVals[doubleIdx]
			if val.Valid {
				record.Float64Items[desc.name] = val.Float64
			}
			doubleIdx++
		case columnKindUUID:
			val := b.uuidVals[uuidIdx]
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

	return record
}

// parseDuckDBAttributesJSON parses the attributes JSON string from DuckDB into EAVRecords.
// This is similar to parseAttributesJSON in postgres_row_scanner.go but takes a string instead of []byte.
func parseDuckDBAttributesJSON(attrsJSON string, record *PersistentRecord) error {
	if attrsJSON == "" || attrsJSON == "[]" {
		return nil
	}
	// Reuse the existing parseAttributesJSON which takes []byte
	return parseAttributesJSON([]byte(attrsJSON), record)
}

// duckDBExecutionPlanContext holds execution plan tracking state.
type duckDBExecutionPlanContext struct {
	opts       *FederatedQueryOptions
	startTotal time.Time
	startQuery time.Time
}

// newDuckDBExecutionPlanContext initializes execution plan tracking if requested.
func newDuckDBExecutionPlanContext(opts *FederatedQueryOptions) *duckDBExecutionPlanContext {
	ctx := &duckDBExecutionPlanContext{
		opts:       opts,
		startTotal: time.Now(),
	}

	if opts != nil && opts.IncludeExecutionPlan {
		if opts.ExecutionPlan == nil {
			opts.ExecutionPlan = &ExecutionPlan{Timings: map[string]int64{}, Notes: []string{}}
		}
		opts.ExecutionPlan.Notes = append(opts.ExecutionPlan.Notes, "StreamDuckDBFederatedQuery started")
	}

	return ctx
}

// recordDirtyIDSource records the dirty ID source in the execution plan.
func (c *duckDBExecutionPlanContext) recordDirtyIDSource(changeLogTable string, dirtyCount int) {
	if c.opts == nil || !c.opts.IncludeExecutionPlan || c.opts.ExecutionPlan == nil {
		return
	}

	dpDirty := DataSourcePlan{
		Tier:        DataTierHot,
		Engine:      "postgres",
		SQL:         fmt.Sprintf("SELECT row_id FROM %s WHERE schema_id = $1 AND flushed_at = 0", sqlutil.SanitizeIdentifier(changeLogTable)),
		RowEstimate: int64(dirtyCount),
		Reason:      "dirty id set fetched",
	}
	c.opts.ExecutionPlan.Sources = append(c.opts.ExecutionPlan.Sources, dpDirty)
}

// recordPushdownFragment records the Postgres pushdown fragment in the execution plan.
func (c *duckDBExecutionPlanContext) recordPushdownFragment(pgMainClause string) {
	if c.opts == nil || !c.opts.IncludeExecutionPlan || c.opts.ExecutionPlan == nil {
		return
	}

	pgDP := DataSourcePlan{
		Tier:              DataTierHot,
		Engine:            "postgres",
		SQL:               pgMainClause,
		RowEstimate:       0,
		PredicatePushdown: pgMainClause != "",
		ActualRows:        0,
		DurationMs:        0,
		Reason:            "pushdown fragment",
	}
	c.opts.ExecutionPlan.Sources = append(c.opts.ExecutionPlan.Sources, pgDP)
}

// recordTranslation records the query translation in the execution plan.
func (c *duckDBExecutionPlanContext) recordTranslation(sqlStr string, translateMs int64, useMainAsAnchor bool) {
	if c.opts == nil || !c.opts.IncludeExecutionPlan || c.opts.ExecutionPlan == nil {
		return
	}

	dp := DataSourcePlan{
		Tier:              DataTierCold,
		Engine:            "duckdb",
		SQL:               sqlStr,
		RowEstimate:       0,
		PredicatePushdown: useMainAsAnchor,
		ActualRows:        0,
		DurationMs:        0,
		Reason:            "duckdb template rendered",
	}
	c.opts.ExecutionPlan.Sources = append(c.opts.ExecutionPlan.Sources, dp)
	c.opts.ExecutionPlan.Timings["translate"] = translateMs
}

// recordQueryStart marks the start of query execution.
func (c *duckDBExecutionPlanContext) recordQueryStart() {
	c.startQuery = time.Now()
}

// recordQueryFailure records a query failure in the execution plan.
func (c *duckDBExecutionPlanContext) recordQueryFailure(err error) {
	if c.opts == nil || !c.opts.IncludeExecutionPlan || c.opts.ExecutionPlan == nil {
		return
	}

	c.opts.ExecutionPlan.Timings["duckdb_fetch"] = time.Since(c.startQuery).Milliseconds()
	c.opts.ExecutionPlan.Timings["total"] = time.Since(c.startTotal).Milliseconds()
	c.opts.ExecutionPlan.Notes = append(c.opts.ExecutionPlan.Notes, fmt.Sprintf("duckdb query failed: %v", err))
}

// recordClientUnavailable records when the DuckDB client is not available.
func (c *duckDBExecutionPlanContext) recordClientUnavailable() {
	if c.opts == nil || !c.opts.IncludeExecutionPlan || c.opts.ExecutionPlan == nil {
		return
	}

	c.opts.ExecutionPlan.Notes = append(c.opts.ExecutionPlan.Notes, "duckdb client unavailable")
	c.opts.ExecutionPlan.Timings["duckdb_fetch"] = 0
	c.opts.ExecutionPlan.Timings["total"] = time.Since(c.startTotal).Milliseconds()
}
