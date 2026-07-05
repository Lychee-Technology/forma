package internal

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/lychee-technology/forma/internal/conditionexpr"
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
		"pg-optimized-v1",
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
		return nil, 0, appendErr
	}

	return records, totalRecords, nil
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
	return sqlgen.WalkCondition(c, sqlgen.HybridStyle, nil, b)
}

func (b *hybridConditionBuilder) EmitLeaf(cond *forma.KvCondition) (string, []any, error) {
	colName := b.resolveColumnName(cond.Attr)
	if colName != "" {
		return b.buildMainTableCondition(cond, colName)
	}
	return b.buildEAVCondition(cond)
}

func (b *hybridConditionBuilder) resolveColumnName(attr string) string {
	if model.IsMainTableColumn(attr) {
		return attr
	}
	if b.cache != nil {
		if meta, ok := b.cache[attr]; ok && meta.ColumnBinding != nil {
			return string(meta.ColumnBinding.ColumnName)
		}
	}
	return ""
}

func (b *hybridConditionBuilder) buildMainTableCondition(cond *forma.KvCondition, colName string) (string, []any, error) {
	parsed := conditionexpr.ParseOperatorValueLenient(cond.Value)
	sqlOpResult, err := conditionexpr.ToSQLOperator(parsed.Operator, parsed.Value)
	if err != nil {
		return "", nil, err
	}

	meta, err := b.resolveLeafMeta(cond, colName)
	if err != nil {
		return "", nil, err
	}
	parsedValue, err := sqlgen.ConvertPgMainValue(sqlOpResult.Value, cond.Attr, meta)
	if err != nil {
		return "", nil, err
	}

	b.argCounter++
	placeholder := fmt.Sprintf("$%d", b.argCounter)

	if b.useMainTableAsAnchor {
		return fmt.Sprintf("m.%s %s %s", sanitizeIdentifier(colName), sqlOpResult.SQLOperator, placeholder), []any{parsedValue}, nil
	}
	return fmt.Sprintf("EXISTS (SELECT 1 FROM %s m WHERE m.ltbase_row_id = t.row_id AND m.%s %s %s)",
		sanitizeIdentifier(b.mainTable), sanitizeIdentifier(colName), sqlOpResult.SQLOperator, placeholder), []any{parsedValue}, nil
}

// resolveLeafMeta returns the attribute metadata driving value conversion for
// a main-table leaf. The physical column is always validated against the
// entity_main descriptors (preserving the pre-#140 "unknown main table
// column" contract even for cache-bound attributes); raw column references
// without schema metadata derive a minimal metadata from the descriptor kind.
func (b *hybridConditionBuilder) resolveLeafMeta(cond *forma.KvCondition, colName string) (forma.AttributeMetadata, error) {
	desc := model.GetMainColumnDescriptor(colName)
	if desc == nil {
		return forma.AttributeMetadata{}, fmt.Errorf("unknown main table column: %s", colName)
	}
	if b.cache != nil {
		if meta, ok := b.cache[cond.Attr]; ok {
			// Bound attributes may omit value_type (e.g. audit columns); the
			// pre-#140 implementation converted by physical column kind, so
			// derive the missing type from the descriptor to keep that contract.
			if meta.ValueType == "" {
				meta.ValueType = model.ColumnKindToValueType(desc.Kind)
			}
			return meta, nil
		}
	}
	return forma.AttributeMetadata{ValueType: model.ColumnKindToValueType(desc.Kind)}, nil
}

func (b *hybridConditionBuilder) buildEAVCondition(cond *forma.KvCondition) (string, []any, error) {
	if b.cache == nil {
		return "", nil, fmt.Errorf("schema metadata cache not available for schema_id %d", b.schemaID)
	}
	gen := sqlgen.NewSQLGenerator()
	pIdx := b.argCounter
	clause, args, err := gen.ToSQLClauses(cond, b.eavTable, b.schemaID, b.cache, &pIdx)
	if err != nil {
		return "", nil, err
	}
	b.argCounter = pIdx

	if b.useMainTableAsAnchor {
		clause = strings.ReplaceAll(clause, "e.row_id", "m.ltbase_row_id")
		clause = strings.ReplaceAll(clause, "e.schema_id", "m.ltbase_schema_id")
	} else {
		clause = strings.ReplaceAll(clause, "e.row_id", "t.row_id")
		clause = strings.ReplaceAll(clause, "e.schema_id", "t.schema_id")
	}
	return clause, args, nil
}
