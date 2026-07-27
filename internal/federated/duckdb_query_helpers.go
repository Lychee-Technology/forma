package federated

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/redact"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// nullDuckDBUUID scans a nullable UUID column from DuckDB. The duckdb-go
// driver returns UUID values as 16 raw bytes (not their text form), so a
// plain sql.NullString silently yields an unparseable value and the record
// ends up with a zero row id (#147).
type nullDuckDBUUID struct {
	UUID  uuid.UUID
	Valid bool
}

// Scan implements sql.Scanner for text, raw-byte, and native UUID sources.
func (n *nullDuckDBUUID) Scan(src any) error {
	n.UUID, n.Valid = uuid.Nil, false
	switch v := src.(type) {
	case nil:
		return nil
	case string:
		parsed, err := uuid.Parse(v)
		if err != nil {
			return fmt.Errorf("scan duckdb uuid %q: %w", v, err)
		}
		n.UUID, n.Valid = parsed, true
		return nil
	case []byte:
		if len(v) == 16 {
			parsed, err := uuid.FromBytes(v)
			if err != nil {
				return fmt.Errorf("scan duckdb uuid bytes: %w", err)
			}
			n.UUID, n.Valid = parsed, true
			return nil
		}
		parsed, err := uuid.ParseBytes(v)
		if err != nil {
			return fmt.Errorf("scan duckdb uuid %q: %w", string(v), err)
		}
		n.UUID, n.Valid = parsed, true
		return nil
	case uuid.UUID:
		n.UUID, n.Valid = v, true
		return nil
	default:
		return fmt.Errorf("scan duckdb uuid: unsupported source type %T", src)
	}
}

// duckDBScanBuffers holds pre-allocated scan buffers for DuckDB row scanning.
type duckDBScanBuffers struct {
	textVals   []sql.NullString
	smallVals  []sql.NullInt64
	intVals    []sql.NullInt64
	bigVals    []sql.NullInt64
	doubleVals []sql.NullFloat64
	uuidVals   []nullDuckDBUUID
}

// newDuckDBScanBuffers creates scan buffers sized according to model.EntityMainColumnDescriptors.
func newDuckDBScanBuffers() *duckDBScanBuffers {
	textCount, smallCount, intCount, bigCount, doubleCount, uuidCount := 0, 0, 0, 0, 0, 0
	for _, desc := range model.EntityMainColumnDescriptors {
		switch desc.Kind {
		case model.ColumnKindText:
			textCount++
		case model.ColumnKindSmallint:
			smallCount++
		case model.ColumnKindInteger:
			intCount++
		case model.ColumnKindBigint:
			bigCount++
		case model.ColumnKindDouble:
			doubleCount++
		case model.ColumnKindUUID:
			uuidCount++
		}
	}

	return &duckDBScanBuffers{
		textVals:   make([]sql.NullString, textCount),
		smallVals:  make([]sql.NullInt64, smallCount),
		intVals:    make([]sql.NullInt64, intCount),
		bigVals:    make([]sql.NullInt64, bigCount),
		doubleVals: make([]sql.NullFloat64, doubleCount),
		uuidVals:   make([]nullDuckDBUUID, uuidCount),
	}
}

// buildScanArgs constructs scan arguments for a single DuckDB row.
// Returns the scan args slice and pointers for metadata columns.
func (b *duckDBScanBuffers) buildScanArgs() ([]any, *sql.NullString, *sql.NullInt64, *sql.NullInt64, *sql.NullInt64) {
	scanArgs := make([]any, 0, len(model.EntityMainColumnDescriptors)+4)
	textIdx, smallIdx, intIdx, bigIdx, doubleIdx, uuidIdx := 0, 0, 0, 0, 0, 0

	for _, desc := range model.EntityMainColumnDescriptors {
		switch desc.Kind {
		case model.ColumnKindText:
			scanArgs = append(scanArgs, &b.textVals[textIdx])
			textIdx++
		case model.ColumnKindSmallint:
			scanArgs = append(scanArgs, &b.smallVals[smallIdx])
			smallIdx++
		case model.ColumnKindInteger:
			scanArgs = append(scanArgs, &b.intVals[intIdx])
			intIdx++
		case model.ColumnKindBigint:
			scanArgs = append(scanArgs, &b.bigVals[bigIdx])
			bigIdx++
		case model.ColumnKindDouble:
			scanArgs = append(scanArgs, &b.doubleVals[doubleIdx])
			doubleIdx++
		case model.ColumnKindUUID:
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

// buildRecordFromBuffers constructs a model.PersistentRecord from the scanned buffer values.
func (b *duckDBScanBuffers) buildRecordFromBuffers() *model.PersistentRecord {
	record := &model.PersistentRecord{
		TextItems:    make(map[string]string),
		Int16Items:   make(map[string]int16),
		Int32Items:   make(map[string]int32),
		Int64Items:   make(map[string]int64),
		Float64Items: make(map[string]float64),
		UUIDItems:    make(map[string]uuid.UUID),
	}

	textIdx, smallIdx, intIdx, bigIdx, doubleIdx, uuidIdx := 0, 0, 0, 0, 0, 0
	for _, desc := range model.EntityMainColumnDescriptors {
		switch desc.Kind {
		case model.ColumnKindText:
			val := b.textVals[textIdx]
			if val.Valid {
				record.TextItems[desc.Name] = val.String
			}
			textIdx++
		case model.ColumnKindSmallint:
			val := b.smallVals[smallIdx]
			if val.Valid {
				if desc.Name == "ltbase_schema_id" {
					record.SchemaID = int16(val.Int64)
				} else {
					record.Int16Items[desc.Name] = int16(val.Int64)
				}
			}
			smallIdx++
		case model.ColumnKindInteger:
			val := b.intVals[intIdx]
			if val.Valid {
				record.Int32Items[desc.Name] = int32(val.Int64)
			}
			intIdx++
		case model.ColumnKindBigint:
			val := b.bigVals[bigIdx]
			if val.Valid {
				// Handle known system columns
				switch desc.Name {
				case "ltbase_created_at":
					record.CreatedAt = val.Int64
				case "ltbase_updated_at":
					record.UpdatedAt = val.Int64
				case "ltbase_deleted_at":
					record.DeletedAt = &val.Int64
				default:
					record.Int64Items[desc.Name] = val.Int64
				}
			}
			bigIdx++
		case model.ColumnKindDouble:
			val := b.doubleVals[doubleIdx]
			if val.Valid {
				record.Float64Items[desc.Name] = val.Float64
			}
			doubleIdx++
		case model.ColumnKindUUID:
			val := b.uuidVals[uuidIdx]
			if val.Valid {
				if desc.Name == "ltbase_row_id" {
					record.RowID = val.UUID
				} else {
					record.UUIDItems[desc.Name] = val.UUID
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
// This is similar to model.ParseAttributesJSON but takes a string instead of []byte.
func parseDuckDBAttributesJSON(attrsJSON string, record *model.PersistentRecord) error {
	if attrsJSON == "" || attrsJSON == "[]" {
		return nil
	}
	// Reuse model.ParseAttributesJSON which takes []byte
	return model.ParseAttributesJSON([]byte(attrsJSON), record)
}

// duckDBExecutionPlanContext holds execution plan tracking state.
type duckDBExecutionPlanContext struct {
	opts       *model.FederatedQueryOptions
	startTotal time.Time
	startQuery time.Time
}

// newDuckDBExecutionPlanContext initializes execution plan tracking if requested.
func newDuckDBExecutionPlanContext(opts *model.FederatedQueryOptions) *duckDBExecutionPlanContext {
	ctx := &duckDBExecutionPlanContext{
		opts:       opts,
		startTotal: time.Now(),
	}

	if opts != nil && opts.IncludeExecutionPlan {
		if opts.ExecutionPlan == nil {
			opts.ExecutionPlan = &model.ExecutionPlan{Timings: map[string]int64{}, Notes: []string{}}
		}
		opts.ExecutionPlan.Notes = append(opts.ExecutionPlan.Notes, "StreamDuckDBFederatedQuery started")
	}

	return ctx
}

// recordDirtyIDSource records the dirty ID source in the execution plan. The
// change_log scan runs in every DuckDB tier form; when hot is excluded from
// PreferredTiers its role shifts from hot data source to pure consistency
// barrier (#184), and the reason says so — the plan reflects the actual
// access, not the requested tiers.
func (c *duckDBExecutionPlanContext) recordDirtyIDSource(changeLogTable string, schemaID int16, dirtyCount int, hasHot bool) {
	if c.opts == nil || !c.opts.IncludeExecutionPlan || c.opts.ExecutionPlan == nil {
		return
	}

	reason := "dirty id set fetched"
	if !hasHot {
		reason = "consistency barrier (dirty-id anti-join)"
	}
	dpDirty := model.DataSourcePlan{
		Tier:        model.DataTierHot,
		Engine:      "postgres",
		SQL:         fmt.Sprintf("SELECT row_id FROM %s WHERE schema_id = $1 AND flushed_at = 0", sqlutil.SanitizeIdentifier(changeLogTable)),
		Params:      formatPlanParams([]any{schemaID}),
		RowEstimate: int64(dirtyCount),
		Reason:      reason,
	}
	c.opts.ExecutionPlan.Sources = append(c.opts.ExecutionPlan.Sources, dpDirty)
}

// recordPushdownFragment records the Postgres pushdown fragment in the execution plan.
func (c *duckDBExecutionPlanContext) recordPushdownFragment(pgMainClause string) {
	if c.opts == nil || !c.opts.IncludeExecutionPlan || c.opts.ExecutionPlan == nil {
		return
	}

	pgDP := model.DataSourcePlan{
		Tier:              model.DataTierHot,
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

// recordTranslation records the query translation in the execution plan,
// including the bind parameters of the rendered DuckDB SQL. DataSourcePlan
// carries a single Tier, but the one read_parquet scan physically serves the
// whole warm∪cold equivalence class (one flat glob, #177/#184), so the entry
// uses a representative parquet tier and a note enumerates the members —
// emitting one source per tier would misrepresent the physical scan count.
func (c *duckDBExecutionPlanContext) recordTranslation(sqlStr string, args []any, translateMs int64, q *model.FederatedAttributeQuery) {
	if c.opts == nil || !c.opts.IncludeExecutionPlan || c.opts.ExecutionPlan == nil {
		return
	}

	served := parquetTiersServed(q)
	useMainAsAnchor := q != nil && q.UseMainAsAnchor
	dp := model.DataSourcePlan{
		Tier:   served[0],
		Engine: "duckdb",
		// #306: the rendered SQL embeds postgres_scan('…password=…'); the
		// internal plan reaches Go embedders via attachExecutionPlan, so the
		// credential is scrubbed here while the rest of the query shape stays
		// diagnosable. The engine still executes the unscrubbed sqlStr.
		SQL:    redact.ConnStringPassword(sqlStr),
		Params: formatPlanParams(args),
		// The anchor hint is a no-op in the advanced template, so it must not
		// masquerade as actual pushdown here (#184); real pushdown facts live
		// on the pushdown-fragment source. The hint itself is recorded as a
		// request in the Notes below.
		PredicatePushdown: false,
		Reason:            "duckdb template rendered",
	}
	c.opts.ExecutionPlan.Sources = append(c.opts.ExecutionPlan.Sources, dp)
	// The note states the physical truth first: the single flat glob cannot
	// separate warm from cold (#177), so one scan always covers both — the
	// requested subset is context, not the access boundary.
	c.opts.ExecutionPlan.Notes = append(c.opts.ExecutionPlan.Notes,
		fmt.Sprintf("parquet(read_parquet) physically serves warm+cold (single flat glob); requested parquet tiers: %s", joinTiers(served)))
	if useMainAsAnchor {
		c.opts.ExecutionPlan.Notes = append(c.opts.ExecutionPlan.Notes,
			"UseMainAsAnchor hint requested (advanced template does not apply it)")
	}
	c.opts.ExecutionPlan.Timings["translate"] = translateMs
}

// parquetTiersServed returns the requested parquet tiers in warm, cold order;
// empty PreferredTiers defaults to both. The DuckDB path is only reached when
// at least one parquet tier participates (the engine gate intercepts hot-only
// requests), but a defensive cold fallback keeps the plan well-formed.
func parquetTiersServed(q *model.FederatedAttributeQuery) []model.DataTier {
	if q == nil || len(q.PreferredTiers) == 0 {
		return []model.DataTier{model.DataTierWarm, model.DataTierCold}
	}
	served := make([]model.DataTier, 0, 2)
	for _, tier := range []model.DataTier{model.DataTierWarm, model.DataTierCold} {
		for _, preferred := range q.PreferredTiers {
			if preferred == tier {
				served = append(served, tier)
				break
			}
		}
	}
	if len(served) == 0 {
		served = []model.DataTier{model.DataTierCold}
	}
	return served
}

func joinTiers(tiers []model.DataTier) string {
	parts := make([]string, 0, len(tiers))
	for _, tier := range tiers {
		parts = append(parts, string(tier))
	}
	return strings.Join(parts, ",")
}

// formatPlanParams renders bind parameters into their diagnostic string
// forms. Only called on plan-capture paths, so the cost is opt-in.
func formatPlanParams(args []any) []string {
	if len(args) == 0 {
		return nil
	}
	params := make([]string, len(args))
	for i, arg := range args {
		params[i] = fmt.Sprintf("%v", arg)
	}
	return params
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

// recordPlanCache notes whether the compiled DuckDB plan came from the
// shared plan cache (#142), in both Notes and Timings form.
func (c *duckDBExecutionPlanContext) recordPlanCache(hit bool) {
	if c == nil || c.opts == nil || !c.opts.IncludeExecutionPlan || c.opts.ExecutionPlan == nil {
		return
	}
	if hit {
		c.opts.ExecutionPlan.Notes = append(c.opts.ExecutionPlan.Notes, "plan_cache=hit")
		c.opts.ExecutionPlan.Timings["plan_cache_hit"] = 1
	} else {
		c.opts.ExecutionPlan.Notes = append(c.opts.ExecutionPlan.Notes, "plan_cache=miss")
		c.opts.ExecutionPlan.Timings["plan_cache_miss"] = 1
	}
}

// recordProjectionCache notes whether the schema projection came from the
// per-engine cache (#142).
func (c *duckDBExecutionPlanContext) recordProjectionCache(hit bool) {
	if c == nil || c.opts == nil || !c.opts.IncludeExecutionPlan || c.opts.ExecutionPlan == nil {
		return
	}
	if hit {
		c.opts.ExecutionPlan.Notes = append(c.opts.ExecutionPlan.Notes, "schema_projection_cache_hit")
	} else {
		c.opts.ExecutionPlan.Notes = append(c.opts.ExecutionPlan.Notes, "schema_projection_cache_miss")
	}
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
