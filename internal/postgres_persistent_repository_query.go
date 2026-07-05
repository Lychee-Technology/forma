package internal

import (
	"context"
	"fmt"
	"strings"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"go.uber.org/zap"
)

func computeTotalPages(total int64, limit int) int {
	if total == 0 || limit <= 0 {
		return 0
	}
	return int((total + int64(limit) - 1) / int64(limit))
}

func (r *DBPersistentRecordRepository) StreamOptimizedQuery(
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
		limit = defaultPageSize
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
func (r *DBPersistentRecordRepository) runOptimizedQuery(
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
		limit = defaultPageSize
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

// RunOptimizedQuery exposes the optimized single-query path (prebuilt WHERE
// clause and args) for the federated engine's PostgresFederatedSource seam.
func (r *DBPersistentRecordRepository) RunOptimizedQuery(
	ctx context.Context,
	tables StorageTables,
	schemaID int16,
	clause string,
	args []any,
	limit, offset int,
	attributeOrders []AttributeOrder,
	useMainTableAsAnchor bool,
) ([]*PersistentRecord, int64, error) {
	return r.runOptimizedQuery(ctx, tables, schemaID, clause, args, limit, offset, attributeOrders, useMainTableAsAnchor)
}

// scanOptimizedRow is implemented in postgres_row_scanner.go

// hasMainTableCondition, parseKvConditionForColumnWithMeta, and convertDateValueForQuery
// are implemented in postgres_condition_helpers.go

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
	query AttributeQuery,
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
// federated query, for the federated engine's PostgresFederatedSource seam.
func (r *DBPersistentRecordRepository) BuildHybridConditions(tables StorageTables, fq *FederatedAttributeQuery) (string, []any, error) {
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
	if isMainTableColumn(attr) {
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
	var attrMeta *forma.AttributeMetadata
	if b.cache != nil {
		if meta, ok := b.cache[cond.Attr]; ok {
			attrMeta = &meta
		}
	}
	op, val, err := parseKvConditionForColumnWithMeta(cond, colName, attrMeta)
	if err != nil {
		return "", nil, err
	}

	b.argCounter++
	placeholder := fmt.Sprintf("$%d", b.argCounter)

	if b.useMainTableAsAnchor {
		return fmt.Sprintf("m.%s %s %s", sanitizeIdentifier(colName), op, placeholder), []any{val}, nil
	}
	return fmt.Sprintf("EXISTS (SELECT 1 FROM %s m WHERE m.ltbase_row_id = t.row_id AND m.%s %s %s)",
		sanitizeIdentifier(b.mainTable), sanitizeIdentifier(colName), op, placeholder), []any{val}, nil
}

func (b *hybridConditionBuilder) buildEAVCondition(cond *forma.KvCondition) (string, []any, error) {
	if b.cache == nil {
		return "", nil, fmt.Errorf("schema metadata cache not available for schema_id %d", b.schemaID)
	}
	gen := NewSQLGenerator()
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
