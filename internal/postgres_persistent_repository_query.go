package internal

import (
	"context"
	"fmt"
	"strconv"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/queryplan"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"go.uber.org/zap"
)

func (r *DBPersistentRecordRepository) StreamOptimizedQuery(
	ctx context.Context,
	tables model.StorageTables,
	schemaID int16,
	clause string,
	args []any,
	limit, offset int,
	attributeOrders []model.AttributeOrder,
	useMainTableAsAnchor bool,
	rowHandler func(*model.PersistentRecord) error,
) (int64, error) {
	if clause == "" {
		return 0, fmt.Errorf("query condition cannot be empty")
	}
	if schemaID <= 0 {
		return 0, fmt.Errorf("schema id must be positive")
	}
	if limit <= 0 {
		limit = model.DefaultPageSize
	}
	if offset < 0 {
		offset = 0
	}

	sqlParams := map[string]any{
		"EAVTable":             sanitizeIdentifier(tables.EAVData),
		"MainTable":            sanitizeIdentifier(tables.EntityMain),
		"ChangeLogTable":       sanitizeIdentifier(tables.ChangeLog),
		"MainProjection":       model.EntityMainProjection,
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

	// SchemaVersion is defense-in-depth (PR #148 review): today every render
	// input derives from key-covered values (metadata reaches the skeleton
	// only through clause text and attributeOrders), but fingerprinting keeps
	// the key correct if the template ever consumes metadata directly.
	fingerprint := ""
	if r.metadataCache != nil {
		fingerprint, _ = r.metadataCache.SchemaFingerprint(schemaID)
	}
	renderKey := queryplan.Key{
		Kind:          "postgres_optimized_template",
		SchemaVersion: fingerprint,
		SchemaID:      schemaID,
		ShapeHash:     strconv.FormatUint(optimizedQueryShapeKey(tables, useMainTableAsAnchor, clause, len(args), attributeOrders), 16),
	}
	queryAny, cacheHit, err := r.planCache.GetOrBuild(renderKey, func() (any, error) {
		return renderTemplate(optimizedQuerySQLTemplate, sqlParams)
	})
	if err != nil {
		return 0, fmt.Errorf("build optimized query: %w", err)
	}
	query := queryAny.(string)
	if !cacheHit {
		zap.S().Debugw("optimized query render cache miss", "schemaID", schemaID)
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

// optimizedQueryShapeKey fingerprints every input that influences the
// rendered optimized-query SQL text (#142): table names, anchor choice, the
// value-free WHERE clause, the arg count (placeholder numbering), and each
// sort key's template-visible fields. Values are bound per request and never
// enter the key. The leading version tag pins the template identity.
func optimizedQueryShapeKey(tables model.StorageTables, useMainTableAsAnchor bool, clause string, argCount int, orders []model.AttributeOrder) uint64 {
	parts := make([]string, 0, 6+6*len(orders))
	parts = append(parts,
		"pg-optimized-v2", // v2: anchor condition parenthesized (#269)
		tables.EAVData,
		tables.EntityMain,
		tables.ChangeLog,
		strconv.FormatBool(useMainTableAsAnchor),
		clause,
		strconv.Itoa(argCount),
	)
	for _, o := range orders {
		parts = append(parts,
			strconv.Itoa(int(o.AttrID)),
			string(o.ValueType),
			string(o.SortOrder),
			string(o.StorageLocation),
			o.ColumnName,
			o.AttrName,
		)
	}
	return sqlgen.HashShapeParts(parts...)
}

// runOptimizedQuery executes an optimized single-query approach that joins entity_main
// with aggregated EAV data, eliminating the N+1 query problem.
func (r *DBPersistentRecordRepository) runOptimizedQuery(
	ctx context.Context,
	tables model.StorageTables,
	schemaID int16,
	clause string,
	args []any,
	limit, offset int,
	attributeOrders []model.AttributeOrder,
	useMainTableAsAnchor bool,
) ([]*model.PersistentRecord, int64, error) {
	if clause == "" {
		return nil, 0, fmt.Errorf("query condition cannot be empty")
	}
	if schemaID <= 0 {
		return nil, 0, fmt.Errorf("schema id must be positive")
	}
	if limit <= 0 {
		limit = model.DefaultPageSize
	}
	if offset < 0 {
		offset = 0
	}

	var records []*model.PersistentRecord
	totalRecords, appendErr := r.StreamOptimizedQuery(ctx, tables, schemaID, clause, args, limit, offset, attributeOrders, useMainTableAsAnchor, func(rp *model.PersistentRecord) error {
		records = append(records, rp)
		return nil
	})
	if appendErr != nil {
		return nil, 0, fmt.Errorf("stream optimized query (schema %d): %w", schemaID, appendErr)
	}

	// The template carries COUNT(*) OVER() on the data rows, so a page at or
	// beyond the last match returns zero rows and the count is unreadable —
	// totalRecords would misreport 0 while matches exist (#181). Recount with
	// offset 0 (cannot recurse); at offset 0 an empty result is a genuine 0.
	if len(records) == 0 && offset > 0 {
		total, countErr := r.countOptimizedQuery(ctx, tables, schemaID, clause, args, attributeOrders, useMainTableAsAnchor)
		if countErr != nil {
			return nil, 0, fmt.Errorf("compute empty-page total: %w", countErr)
		}
		totalRecords = total
	}

	return records, totalRecords, nil
}

// countOptimizedQuery reads the matching total through the same optimized
// query at limit 1, offset 0: any match then carries the window count back on
// the returned row. Identical clause/args/orders reuse the cached plan —
// limit and offset are bind parameters, not part of the rendered SQL.
func (r *DBPersistentRecordRepository) countOptimizedQuery(
	ctx context.Context,
	tables model.StorageTables,
	schemaID int16,
	clause string,
	args []any,
	attributeOrders []model.AttributeOrder,
	useMainTableAsAnchor bool,
) (int64, error) {
	total, err := r.StreamOptimizedQuery(ctx, tables, schemaID, clause, args, 1, 0, attributeOrders, useMainTableAsAnchor, nil)
	if err != nil {
		return 0, fmt.Errorf("optimized count query (schema %d): %w", schemaID, err)
	}
	return total, nil
}

// RunOptimizedQuery exposes the optimized single-query path (prebuilt WHERE
// clause and args) for the federated engine's federated.PostgresFederatedSource seam.
func (r *DBPersistentRecordRepository) RunOptimizedQuery(
	ctx context.Context,
	tables model.StorageTables,
	schemaID int16,
	clause string,
	args []any,
	limit, offset int,
	attributeOrders []model.AttributeOrder,
	useMainTableAsAnchor bool,
) ([]*model.PersistentRecord, int64, error) {
	return r.runOptimizedQuery(ctx, tables, schemaID, clause, args, limit, offset, attributeOrders, useMainTableAsAnchor)
}

// scanOptimizedRow is implemented in postgres_row_scanner.go

// hasMainTableCondition is implemented in postgres_condition_helpers.go

// hybridConditionBuilder encapsulates the state needed for building hybrid SQL conditions.
type hybridConditionBuilder struct {
	r                    *DBPersistentRecordRepository
	eavTable             string
	mainTable            string
	schemaID             int16
	cache                forma.SchemaAttributeCache
	argCounter           int
	useMainTableAsAnchor bool
}

func (r *DBPersistentRecordRepository) buildHybridConditions(
	eavTable, mainTable string,
	query model.AttributeQuery,
	initArgIndex int,
	useMainTableAsAnchor bool,
) (string, []any, error) {
	if query.Condition == nil {
		return "1=1", nil, nil
	}

	builder := &hybridConditionBuilder{
		r:                    r,
		eavTable:             eavTable,
		mainTable:            mainTable,
		schemaID:             query.SchemaID,
		argCounter:           initArgIndex,
		useMainTableAsAnchor: useMainTableAsAnchor,
	}
	builder.initCache()

	return builder.build(query.Condition)
}

// BuildHybridConditions builds the main-table/EAV hybrid WHERE clause for a
// federated query, for the federated engine's federated.PostgresFederatedSource seam.
func (r *DBPersistentRecordRepository) BuildHybridConditions(tables model.StorageTables, fq *model.FederatedAttributeQuery) (string, []any, error) {
	if fq == nil {
		return "", nil, fmt.Errorf("federated query cannot be nil")
	}
	return r.buildHybridConditions(tables.EAVData, tables.EntityMain, fq.AttributeQuery, 0, fq.UseMainAsAnchor)
}

func (b *hybridConditionBuilder) initCache() {
	if b.schemaID > 0 && b.r.metadataCache != nil {
		if schemaName, ok := b.r.metadataCache.GetSchemaName(b.schemaID); ok {
			if cache, ok := b.r.metadataCache.GetSchemaCache(schemaName); ok {
				b.cache = cache
			}
		}
	}
}

func (b *hybridConditionBuilder) build(c forma.Condition) (string, []any, error) {
	return sqlgen.WalkHybridCondition(c, b.cache, b)
}

// EmitTypedLeaf renders a parse-once hybrid leaf. Routing (main vs EAV) and
// all parsing/value conversion were resolved by the normalizer; this method
// only assigns placeholders and formats the shell with request/builder state
// (table names, the anchor alias, the arg counter).
func (b *hybridConditionBuilder) EmitTypedLeaf(leaf *sqlgen.PredicateLeaf) (string, []any, error) {
	p := leaf.Hybrid
	if p.IsMain {
		return b.emitMainLeaf(p)
	}
	return b.emitEAVLeaf(p)
}

func (b *hybridConditionBuilder) emitMainLeaf(p sqlgen.HybridLeafPayload) (string, []any, error) {
	if p.Err != nil {
		return "", nil, p.Err
	}

	b.argCounter++
	placeholder := fmt.Sprintf("$%d", b.argCounter)

	if b.useMainTableAsAnchor {
		return fmt.Sprintf("m.%s %s %s", sanitizeIdentifier(p.MainColumn), p.MainSQLOp, placeholder), []any{p.MainValue}, nil
	}
	return fmt.Sprintf("EXISTS (SELECT 1 FROM %s m WHERE m.ltbase_row_id = t.row_id AND m.%s %s %s)",
		sanitizeIdentifier(b.mainTable), sanitizeIdentifier(p.MainColumn), p.MainSQLOp, placeholder), []any{p.MainValue}, nil
}

// emitEAVLeaf formats the EAV EXISTS subquery directly against the anchor
// alias (t.* or m.ltbase_*), replacing the retired e.*-then-string-replace
// path. Placeholder numbering (attr_id then value) matches the pre-#154 EAV
// emitter exactly.
func (b *hybridConditionBuilder) emitEAVLeaf(p sqlgen.HybridLeafPayload) (string, []any, error) {
	if b.cache == nil {
		return "", nil, fmt.Errorf("schema metadata cache not available for schema_id %d", b.schemaID)
	}
	eav := p.Eav
	if eav.Err != nil {
		return "", nil, eav.Err
	}

	schemaAlias, rowAlias := "t.schema_id", "t.row_id"
	if b.useMainTableAsAnchor {
		schemaAlias, rowAlias = "m.ltbase_schema_id", "m.ltbase_row_id"
	}

	b.argCounter++
	attrPlaceholder := fmt.Sprintf("$%d", b.argCounter)
	b.argCounter++
	valuePlaceholder := fmt.Sprintf("$%d", b.argCounter)

	clause := fmt.Sprintf(
		"EXISTS (SELECT 1 FROM %s x WHERE x.schema_id = %s AND x.row_id = %s AND x.attr_id = %s AND x.%s %s %s)",
		b.eavTable, schemaAlias, rowAlias, attrPlaceholder, eav.ValueColumn, eav.SQLOp, valuePlaceholder,
	)
	return clause, []any{eav.AttrID, eav.Value}, nil
}
