package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"lychee.technology/ltbase/forma"
)

type PostgresAttributeRepository struct {
	pool            *pgxpool.Pool
	eavTableName    string
	schemaTableName string
	attributeCaches map[string]forma.SchemaAttributeCache // schema_name -> attribute cache
	cacheMutex      sync.RWMutex                          // protects concurrent access to cache
	metadataCache   *MetadataCache                        // global metadata cache with schema/attr mappings
}

func NewPostgresAttributeRepository(
	pool *pgxpool.Pool,
	eavTableName string,
	schemaTableName string,
	metadataCache *MetadataCache,
) *PostgresAttributeRepository {
	return &PostgresAttributeRepository{
		pool:            pool,
		eavTableName:    eavTableName,
		schemaTableName: schemaTableName,
		attributeCaches: make(map[string]forma.SchemaAttributeCache),
		metadataCache:   metadataCache,
	}
}

// RegisterSchemaCache registers an attribute cache for a schema
func (r *PostgresAttributeRepository) RegisterSchemaCache(
	schemaName string,
	cache forma.SchemaAttributeCache,
) {
	r.cacheMutex.Lock()
	defer r.cacheMutex.Unlock()
	r.attributeCaches[schemaName] = cache
}

// LoadAndRegisterSchemaCache loads attribute definitions from JSON file and registers the cache
func (r *PostgresAttributeRepository) LoadAndRegisterSchemaCache(
	schemaName string,
	schemaDir string,
) error {
	filePath := filepath.Join(schemaDir, schemaName+"_attributes.json")
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("read attribute definitions file %s: %w", filePath, err)
	}

	type jsonAttrMeta struct {
		AttributeID int16  `json:"attributeID"`
		ValueType   string `json:"valueType"`
		InsideArray bool   `json:"insideArray"`
	}

	var jsonDefs map[string]jsonAttrMeta
	if err := json.Unmarshal(data, &jsonDefs); err != nil {
		return fmt.Errorf("parse attribute definitions: %w", err)
	}

	// Convert to SchemaAttributeCache
	cache := make(forma.SchemaAttributeCache)
	for attrName, jsonMeta := range jsonDefs {
		cache[attrName] = forma.AttributeMeta{
			AttributeID: jsonMeta.AttributeID,
			ValueType:   forma.ValueType(jsonMeta.ValueType),
			InsideArray: jsonMeta.InsideArray,
		}
	}

	r.RegisterSchemaCache(schemaName, cache)
	return nil
}

// getSchemaCache retrieves the attribute cache for a schema (thread-safe)
func (r *PostgresAttributeRepository) getSchemaCache(schemaName string) forma.SchemaAttributeCache {
	r.cacheMutex.RLock()
	defer r.cacheMutex.RUnlock()
	return r.attributeCaches[schemaName]
}

// InsertAttributes inserts new attributes into the EAV table
// Attributes are inserted in batches of 500 using multi-value INSERT statements with pgx.Batch
func (r *PostgresAttributeRepository) InsertAttributes(ctx context.Context, attributes []Attribute) error {
	if len(attributes) == 0 {
		return nil
	}

	const batchSize = 500
	batch := &pgx.Batch{}

	// Build all multi-value INSERT statements and queue them in the batch
	for i := 0; i < len(attributes); i += batchSize {
		end := i + batchSize
		if end > len(attributes) {
			end = len(attributes)
		}

		chunk := attributes[i:end]
		valuesClause, args := r.buildValuesClause(chunk)

		query := fmt.Sprintf(
			"INSERT INTO %s (schema_id, row_id, attr_id, array_indices, value_text, value_numeric, value_date, value_bool) VALUES %s",
			r.eavTableName,
			valuesClause,
		)

		batch.Queue(query, args...)
	}

	// Execute all statements in the batch
	results := r.pool.SendBatch(ctx, batch)
	defer results.Close()

	batchCount := (len(attributes) + batchSize - 1) / batchSize
	for i := 0; i < batchCount; i++ {
		_, err := results.Exec()
		if err != nil {
			return fmt.Errorf("failed to execute insert batch %d: %w", i, err)
		}
	}

	return nil
}

// buildValuesClause constructs the VALUES clause and parameter array for multi-value INSERT statements
func (r *PostgresAttributeRepository) buildValuesClause(attributes []Attribute) (string, []interface{}) {
	var valuesClauses []string
	var args []interface{}
	paramIndex := 1

	for _, attr := range attributes {
		valuePlaceholders := fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			paramIndex, paramIndex+1, paramIndex+2, paramIndex+3,
			paramIndex+4, paramIndex+5, paramIndex+6, paramIndex+7)
		valuesClauses = append(valuesClauses, valuePlaceholders)

		args = append(args,
			attr.SchemaID,
			attr.RowID,
			attr.AttrID,
			attr.ArrayIndices,
			attr.ValueText,
			attr.ValueNumeric,
			attr.ValueDate,
			attr.ValueBool,
		)
		paramIndex += 8
	}

	return strings.Join(valuesClauses, ", "), args
}

// UpdateAttributes updates existing attributes in the EAV table
func (r *PostgresAttributeRepository) UpdateAttributes(ctx context.Context, attributes []Attribute) error {
	if len(attributes) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	query := fmt.Sprintf(
		`UPDATE %s SET 
			value_text = $1, 
			value_numeric = $2, 
			value_date = $3, 
			value_bool = $4 
		WHERE schema_id = $5 AND row_id = $6 AND attr_id = $7 AND array_indices = $8`,
		r.eavTableName,
	)

	for _, attr := range attributes {
		batch.Queue(query,
			attr.ValueText,
			attr.ValueNumeric,
			attr.ValueDate,
			attr.ValueBool,
			attr.SchemaID,
			attr.RowID,
			attr.AttrID,
			attr.ArrayIndices,
		)
	}

	results := r.pool.SendBatch(ctx, batch)
	defer results.Close()

	for i := 0; i < len(attributes); i++ {
		_, err := results.Exec()
		if err != nil {
			return fmt.Errorf("failed to update attribute at index %d: %w", i, err)
		}
	}

	return nil
}

// BatchUpsertAttributes performs upsert operations on attributes
// Attributes are upserted in batches of 500 using multi-value INSERT statements with pgx.Batch
func (r *PostgresAttributeRepository) BatchUpsertAttributes(ctx context.Context, attributes []Attribute) error {
	if len(attributes) == 0 {
		return nil
	}

	const batchSize = 500
	batch := &pgx.Batch{}

	// Build all multi-value UPSERT statements and queue them in the batch
	for i := 0; i < len(attributes); i += batchSize {
		end := i + batchSize
		if end > len(attributes) {
			end = len(attributes)
		}

		chunk := attributes[i:end]
		valuesClause, args := r.buildValuesClause(chunk)

		query := fmt.Sprintf(
			`INSERT INTO %s (schema_id, row_id, attr_id, array_indices, value_text, value_numeric, value_date, value_bool) 
			VALUES %s
			ON CONFLICT (schema_id, row_id, attr_id, array_indices)
			DO UPDATE SET 
				value_text = EXCLUDED.value_text,
				value_numeric = EXCLUDED.value_numeric,
				value_date = EXCLUDED.value_date,
				value_bool = EXCLUDED.value_bool`,
			r.eavTableName,
			valuesClause,
		)

		batch.Queue(query, args...)
	}

	// Execute all statements in the batch
	results := r.pool.SendBatch(ctx, batch)
	defer results.Close()

	batchCount := (len(attributes) + batchSize - 1) / batchSize
	for i := 0; i < batchCount; i++ {
		_, err := results.Exec()
		if err != nil {
			return fmt.Errorf("failed to execute upsert batch %d: %w", i, err)
		}
	}

	return nil
}

// DeleteAttributes deletes all attributes for given row IDs in a schema
func (r *PostgresAttributeRepository) DeleteAttributes(ctx context.Context, schemaName string, rowIDs []uuid.UUID) error {
	if len(rowIDs) == 0 {
		return nil
	}

	// Get schema_id from metadata cache
	schemaID, ok := r.metadataCache.GetSchemaID(schemaName)
	if !ok {
		return fmt.Errorf("schema not found in metadata cache: %s", schemaName)
	}

	query := fmt.Sprintf(
		`DELETE FROM %s
 WHERE schema_id = $1
 AND row_id = ANY($2)`,
		r.eavTableName,
	)

	_, err := r.pool.Exec(ctx, query, schemaID, rowIDs)
	if err != nil {
		return fmt.Errorf("failed to delete attributes for schema %s: %w", schemaName, err)
	}

	return nil
}

// GetAttributes retrieves all attributes for a specific row ID
func (r *PostgresAttributeRepository) GetAttributes(ctx context.Context, schemaName string, rowID uuid.UUID) ([]Attribute, error) {
	// Get schema_id from metadata cache
	schemaID, ok := r.metadataCache.GetSchemaID(schemaName)
	if !ok {
		return nil, fmt.Errorf("schema not found in metadata cache: %s", schemaName)
	}

	query := fmt.Sprintf(
		`SELECT schema_id, row_id, attr_id, array_indices, value_text, value_numeric, value_date, value_bool
 FROM %s
 WHERE schema_id = $1
 AND row_id = $2`,
		r.eavTableName,
	)

	rows, err := r.pool.Query(ctx, query, schemaID, rowID)
	if err != nil {
		return nil, fmt.Errorf("failed to query attributes: %w", err)
	}
	defer rows.Close()

	attributes := make([]Attribute, 0)
	for rows.Next() {
		var attr Attribute
		err := rows.Scan(
			&attr.SchemaID,
			&attr.RowID,
			&attr.AttrID,
			&attr.ArrayIndices,
			&attr.ValueText,
			&attr.ValueNumeric,
			&attr.ValueDate,
			&attr.ValueBool,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan attribute: %w", err)
		}
		attributes = append(attributes, attr)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating attributes: %w", err)
	}

	return attributes, nil
}

// buildPaginatedQueryTemplate constructs the common paginated query structure with distinct rows and pagination metadata
func (r *PostgresAttributeRepository) buildPaginatedQueryTemplate(
	schemaCondition string,
	conditions string,
	orderByClause string,
	limitParamIdx int,
	offsetParamIdx int,
) string {
	return fmt.Sprintf(`WITH distinct_rows AS (
		SELECT DISTINCT t.row_id
		FROM %s t
		WHERE %s AND %s
		%s
		LIMIT $%d OFFSET $%d
	),
	total_count AS (
		SELECT COUNT(DISTINCT t.row_id) as total
		FROM %s t
		WHERE %s AND %s
	)
	SELECT t.schema_id, t.row_id, t.attr_id, t.array_indices, 
		   t.value_text, t.value_numeric, t.value_date, t.value_bool,
		   tc.total as total_records,
		   CEIL(tc.total::numeric / $%d) as total_pages,
		   ($%d / $%d) + 1 as current_page
	FROM distinct_rows dr
	CROSS JOIN total_count tc
	CROSS JOIN LATERAL (
		SELECT schema_id, row_id, attr_id, array_indices, 
			   value_text, value_numeric, value_date, value_bool
		FROM %s t
		WHERE %s
		AND t.row_id = dr.row_id
	) t`,
		r.eavTableName,
		schemaCondition,
		conditions,
		orderByClause,
		limitParamIdx, offsetParamIdx,
		r.eavTableName,
		schemaCondition,
		conditions,
		limitParamIdx,
		offsetParamIdx, limitParamIdx,
		r.eavTableName,
		schemaCondition,
	)
}

// QueryAttributes performs complex filtered queries on attributes with pagination
func (r *PostgresAttributeRepository) QueryAttributes(ctx context.Context, query *AttributeQuery) ([]Attribute, error) {
	if query == nil {
		return nil, fmt.Errorf("query cannot be nil")
	}

	// Build the conditions based on filters
	var args []any = []any{}
	var conditions string = "1=1"

	// Determine if this is a single-schema, multi-schema, or cross-schema query
	var baseQuery string
	var queryArgs []interface{}
	schemaCondition := "1=1"
	if query.SchemaID > 0 {
		// Single schema query - use subquery to get schema_id
		schemaCondition = fmt.Sprintf("t.schema_id = %d", query.SchemaID)
	}
	// Global query without schema filter
	conditions, args = r.buildFilterConditions(query.SchemaID, query.Filters, 3)
	baseQuery = r.buildPaginatedQueryTemplate(schemaCondition, conditions, r.buildOrderByClause(query.OrderBy), 1, 2)

	queryArgs = append(queryArgs, query.Limit, query.Offset)
	queryArgs = append(queryArgs, args...)

	fmt.Printf("Executing Query: %s\nWith Args: %v\n", baseQuery, queryArgs)
	rows, err := r.pool.Query(ctx, baseQuery, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("failed to query attributes: %w", err)
	}
	defer rows.Close()

	attributes := make([]Attribute, 0)
	for rows.Next() {
		var attr Attribute
		var totalRecords, totalPages, currentPage int64
		err := rows.Scan(
			&attr.SchemaID,
			&attr.RowID,
			&attr.AttrID,
			&attr.ArrayIndices,
			&attr.ValueText,
			&attr.ValueNumeric,
			&attr.ValueDate,
			&attr.ValueBool,
			&totalRecords,
			&totalPages,
			&currentPage,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan attribute: %w", err)
		}
		attributes = append(attributes, attr)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating attributes: %w", err)
	}

	return attributes, nil
}

// ExistsEntity checks if an entity exists
func (r *PostgresAttributeRepository) ExistsEntity(ctx context.Context, schemaName string, rowID uuid.UUID) (bool, error) {
	// Get schema_id from metadata cache
	schemaID, ok := r.metadataCache.GetSchemaID(schemaName)
	if !ok {
		return false, fmt.Errorf("schema not found in metadata cache: %s", schemaName)
	}

	query := fmt.Sprintf(
		`SELECT EXISTS(
SELECT 1 FROM %s
WHERE schema_id = $1
AND row_id = $2
)`,
		r.eavTableName,
	)

	var exists bool
	err := r.pool.QueryRow(ctx, query, schemaID, rowID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check entity existence: %w", err)
	}

	return exists, nil
}

// DeleteEntity deletes all attributes for a specific entity
func (r *PostgresAttributeRepository) DeleteEntity(ctx context.Context, schemaName string, rowID uuid.UUID) error {
	// Get schema_id from metadata cache
	schemaID, ok := r.metadataCache.GetSchemaID(schemaName)
	if !ok {
		return fmt.Errorf("schema not found in metadata cache: %s", schemaName)
	}

	query := fmt.Sprintf(
		`DELETE FROM %s WHERE schema_id = $1
 AND row_id = $2`,
		r.eavTableName,
	)

	_, err := r.pool.Exec(ctx, query, schemaID, rowID)
	if err != nil {
		return fmt.Errorf("failed to delete entity: %w", err)
	}

	return nil
}

// CountEntities counts entities matching the given filters in a schema
func (r *PostgresAttributeRepository) CountEntities(ctx context.Context, schemaName string, filters []forma.Filter) (int64, error) {
	// Get schema_id from metadata cache
	schemaID, ok := r.metadataCache.GetSchemaID(schemaName)
	if !ok {
		return 0, fmt.Errorf("schema not found in metadata cache: %s", schemaName)
	}

	conditions, args := r.buildFilterConditions(schemaID, filters, 2)

	query := fmt.Sprintf(
		`SELECT COUNT(DISTINCT row_id)
FROM %s
WHERE schema_id = $1
AND %s`,
		r.eavTableName,
		conditions,
	)

	queryArgs := append([]interface{}{schemaID}, args...)

	var count int64
	err := r.pool.QueryRow(ctx, query, queryArgs...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count entities: %w", err)
	}

	return count, nil
}

// AdvancedQueryRowIDs executes an advanced condition clause and returns matching row IDs with pagination.
func (r *PostgresAttributeRepository) AdvancedQueryRowIDs(ctx context.Context, clause string, args []any, limit, offset int) ([]uuid.UUID, int64, error) {
	if clause == "" {
		return nil, 0, fmt.Errorf("advanced query clause cannot be empty")
	}

	if limit <= 0 {
		return nil, 0, fmt.Errorf("limit must be greater than zero")
	}

	// Replace hard-coded table reference with configured table name
	clause = strings.ReplaceAll(clause, "public.eav_data", r.eavTableName)

	limitIdx := len(args) + 1
	offsetIdx := len(args) + 2

	queryArgs := append([]any{}, args...)
	queryArgs = append(queryArgs, limit, offset)

	rowQuery := fmt.Sprintf(`
WITH matched_entities AS (%s)
SELECT DISTINCT row_id
FROM matched_entities
ORDER BY row_id
LIMIT $%d OFFSET $%d;
`, clause, limitIdx, offsetIdx)

	rows, err := r.pool.Query(ctx, rowQuery, queryArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to execute advanced query: %w", err)
	}
	defer rows.Close()

	rowIDs := make([]uuid.UUID, 0)
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, 0, fmt.Errorf("failed to scan row_id: %w", err)
		}
		rowIDs = append(rowIDs, id)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("error iterating advanced query rows: %w", err)
	}

	countQuery := fmt.Sprintf(`
WITH matched_entities AS (%s)
SELECT COUNT(DISTINCT row_id)
FROM matched_entities;
`, clause)

	var total int64
	if err := r.pool.QueryRow(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("failed to count advanced query results: %w", err)
	}

	return rowIDs, total, nil
}

// buildFilterConditions converts filters to SQL WHERE clause conditions
// Uses attribute cache to map attribute names to attr_id and determine correct value column
func (r *PostgresAttributeRepository) buildFilterConditions(schemaID int16, filters []forma.Filter, initArgIndex int) (string, []any) {
	if len(filters) == 0 {
		return "1=1", []any{}
	}

	var conditions []string
	var args []interface{}
	argCounter := initArgIndex

	// Get schema cache if available
	var cache forma.SchemaAttributeCache

	for _, filter := range filters {
		var condition string
		var filterValue interface{} = filter.Value

		// Determine which column to use based on filter field
		columnName := mapFilterField(filter.Field)

		// If filtering by attribute name/value and we have cache, use typed columns
		if filter.Field == forma.FilterFieldAttributeName && cache != nil {
			// This is a filter on a specific attribute by name
			// We need to find the attr_id and value_type from cache
			// However, the current Filter structure doesn't include the attribute name
			// So we'll use the general approach for now
			condition, filterValue = r.buildTypedCondition(filter, argCounter, cache)
		} else {
			// Standard field filter
			condition, filterValue = r.buildCondition(filter, columnName, argCounter)
		}

		if condition != "" {
			conditions = append(conditions, condition)
			args = append(args, filterValue)
			argCounter++
		}
	}

	if len(conditions) == 0 {
		return "1=1", []any{}
	}

	return strings.Join(conditions, " AND "), args
}

// buildCondition builds a single filter condition
func (r *PostgresAttributeRepository) buildCondition(filter forma.Filter, columnName string, argIndex int) (string, interface{}) {
	switch filter.Type {
	case forma.FilterEquals:
		return fmt.Sprintf("(%s = $%d)", columnName, argIndex), filter.Value

	case forma.FilterNotEquals:
		return fmt.Sprintf("(%s != $%d)", columnName, argIndex), filter.Value

	case forma.FilterStartsWith:
		return fmt.Sprintf("(%s ILIKE $%d || '%%')", columnName, argIndex), filter.Value

	case forma.FilterContains:
		return fmt.Sprintf("(%s ILIKE '%%' || $%d || '%%')", columnName, argIndex), filter.Value

	case forma.FilterGreaterThan:
		return fmt.Sprintf("(%s > $%d)", columnName, argIndex), filter.Value

	case forma.FilterLessThan:
		return fmt.Sprintf("(%s < $%d)", columnName, argIndex), filter.Value

	case forma.FilterGreaterEq:
		return fmt.Sprintf("(%s >= $%d)", columnName, argIndex), filter.Value

	case forma.FilterLessEq:
		return fmt.Sprintf("(%s <= $%d)", columnName, argIndex), filter.Value

	case forma.FilterIn:
		return fmt.Sprintf("(%s = ANY($%d))", columnName, argIndex), filter.Value

	case forma.FilterNotIn:
		return fmt.Sprintf("(%s != ALL($%d))", columnName, argIndex), filter.Value

	default:
		return "", nil
	}
}

// buildTypedCondition builds a condition using typed value columns based on attribute metadata
func (r *PostgresAttributeRepository) buildTypedCondition(filter forma.Filter, argIndex int, cache forma.SchemaAttributeCache) (string, interface{}) {
	// This is a simplified version - in a real implementation, you would need to:
	// 1. Extract the attribute name from the filter (requires Filter API extension)
	// 2. Look up the attr_id and value_type from cache
	// 3. Build condition using the appropriate value_* column

	// For now, fall back to generic handling
	return r.buildCondition(filter, "value_text", argIndex)
}

// buildOrderByClause constructs the ORDER BY clause from OrderBy specifications
func (r *PostgresAttributeRepository) buildOrderByClause(orderBy []forma.OrderBy) string {
	if len(orderBy) == 0 {
		return "ORDER BY t.row_id ASC"
	}

	var clauses []string
	for _, ob := range orderBy {
		field := mapFilterField(ob.Field)
		order := "ASC"
		if ob.SortOrder == forma.SortOrderDesc {
			order = "DESC"
		}
		clauses = append(clauses, fmt.Sprintf("t.%s %s", field, order))
	}

	return "ORDER BY " + strings.Join(clauses, ", ")
}

// mapFilterField maps FilterField constants to database column names
func mapFilterField(field forma.FilterField) string {
	switch field {
	case forma.FilterFieldAttributeName:
		return "attr_id" // Changed from attr_name
	case forma.FilterFieldAttributeValue:
		return "value_text" // Default to text, should be determined by type
	case forma.FilterFieldRowID:
		return "row_id"
	case forma.FilterFieldSchemaName:
		return "schema_name"
	default:
		return "value_text" // Default fallback
	}
}

// getValueColumnName returns the appropriate value column name based on ValueType
func getValueColumnName(vt forma.ValueType) string {
	switch vt {
	case forma.ValueTypeText:
		return "value_text"
	case forma.ValueTypeNumeric:
		return "value_numeric"
	case forma.ValueTypeDate:
		return "value_date"
	case forma.ValueTypeBool:
		return "value_bool"
	default:
		return "value_text"
	}
}
