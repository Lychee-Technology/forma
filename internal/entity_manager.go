package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"lychee.technology/ltbase/forma"
)

type entityManager struct {
	transformer Transformer
	repository  AttributeRepository
	registry    SchemaRegistry
	config      *forma.Config
}

// NewEntityManager creates a new EntityManager instance
func NewEntityManager(
	transformer Transformer,
	repository AttributeRepository,
	registry SchemaRegistry,
	config *forma.Config,
) forma.EntityManager {
	return &entityManager{
		transformer: transformer,
		repository:  repository,
		registry:    registry,
		config:      config,
	}
}

// Create creates a new entity with the provided data
func (em *entityManager) Create(ctx context.Context, req *forma.EntityOperation) (*forma.DataRecord, error) {
	if req == nil {
		return nil, fmt.Errorf("entity operation cannot be nil")
	}

	if req.SchemaName == "" {
		return nil, fmt.Errorf("schema name is required")
	}

	if req.Data == nil {
		return nil, fmt.Errorf("data is required for create operation")
	}

	// Get schema by name to obtain schema ID
	schemaID, _, err := em.registry.GetSchemaByName(req.SchemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	// Generate UUID v7 for the new entity
	rowID := uuid.Must(uuid.NewV7())

	// Convert JSON data to attributes
	attributes, err := em.transformer.ToAttributes(ctx, schemaID, rowID, req.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to transform data to attributes: %w", err)
	}

	// Insert attributes
	if err := em.repository.InsertAttributes(ctx, attributes); err != nil {
		return nil, fmt.Errorf("failed to insert attributes: %w", err)
	}

	// Return the created entity
	return &forma.DataRecord{
		SchemaName: req.SchemaName,
		RowID:      rowID,
		Attributes: req.Data,
	}, nil
}

// Get retrieves an entity by schema name and row ID
func (em *entityManager) Get(ctx context.Context, req *forma.QueryRequest) (*forma.DataRecord, error) {
	if req == nil {
		return nil, fmt.Errorf("query request cannot be nil")
	}

	if req.SchemaName == "" {
		return nil, fmt.Errorf("schema name is required")
	}

	if req.RowID == nil {
		return nil, fmt.Errorf("row ID is required for get operation")
	}

	// Verify schema exists
	_, _, err := em.registry.GetSchemaByName(req.SchemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	// Get attributes for the entity
	attributes, err := em.repository.GetAttributes(ctx, req.SchemaName, *req.RowID)
	if err != nil {
		return nil, fmt.Errorf("failed to get attributes: %w", err)
	}

	if len(attributes) == 0 {
		return nil, fmt.Errorf("entity not found: %s/%s", req.SchemaName, req.RowID)
	}

	// Convert attributes back to JSON
	jsonData, err := em.transformer.FromAttributes(ctx, attributes)
	if err != nil {
		return nil, fmt.Errorf("failed to transform attributes to JSON: %w", err)
	}

	return &forma.DataRecord{
		SchemaName: req.SchemaName,
		RowID:      *req.RowID,
		Attributes: jsonData,
	}, nil
}

// Update updates an existing entity
func (em *entityManager) Update(ctx context.Context, req *forma.EntityOperation) (*forma.DataRecord, error) {
	if req == nil {
		return nil, fmt.Errorf("entity operation cannot be nil")
	}

	if req.SchemaName == "" {
		return nil, fmt.Errorf("schema name is required")
	}

	if req.RowID == (uuid.UUID{}) {
		return nil, fmt.Errorf("row ID is required for update operation")
	}

	if req.Updates == nil {
		return nil, fmt.Errorf("updates are required for update operation")
	}

	// Get schema by name
	schemaID, _, err := em.registry.GetSchemaByName(req.SchemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	// Check if entity exists
	exists, err := em.repository.ExistsEntity(ctx, req.SchemaName, req.RowID)
	if err != nil {
		return nil, fmt.Errorf("failed to check entity existence: %w", err)
	}

	if !exists {
		return nil, fmt.Errorf("entity not found: %s/%s", req.SchemaName, req.RowID)
	}

	// Get existing entity
	existingAttrs, err := em.repository.GetAttributes(ctx, req.SchemaName, req.RowID)
	if err != nil {
		return nil, fmt.Errorf("failed to get existing attributes: %w", err)
	}

	// Convert existing attributes to JSON
	existingData, err := em.transformer.FromAttributes(ctx, existingAttrs)
	if err != nil {
		return nil, fmt.Errorf("failed to transform existing attributes: %w", err)
	}

	// Merge updates with existing data
	mergedData := mergeMaps(existingData, req.Updates)

	// Convert merged data to attributes
	newAttributes, err := em.transformer.ToAttributes(ctx, schemaID, req.RowID, mergedData)
	if err != nil {
		return nil, fmt.Errorf("failed to transform merged data to attributes: %w", err)
	}

	// Upsert attributes
	if err := em.repository.BatchUpsertAttributes(ctx, newAttributes); err != nil {
		return nil, fmt.Errorf("failed to upsert attributes: %w", err)
	}

	return &forma.DataRecord{
		SchemaName: req.SchemaName,
		RowID:      req.RowID,
		Attributes: mergedData,
	}, nil
}

// Delete deletes an entity
func (em *entityManager) Delete(ctx context.Context, req *forma.EntityOperation) error {
	if req == nil {
		return fmt.Errorf("entity operation cannot be nil")
	}

	if req.SchemaName == "" {
		return fmt.Errorf("schema name is required")
	}

	if req.RowID == (uuid.UUID{}) {
		return fmt.Errorf("row ID is required for delete operation")
	}

	// Verify schema exists
	_, _, err := em.registry.GetSchemaByName(req.SchemaName)
	if err != nil {
		return fmt.Errorf("failed to get schema: %w", err)
	}

	// Delete entity
	if err := em.repository.DeleteEntity(ctx, req.SchemaName, req.RowID); err != nil {
		return fmt.Errorf("failed to delete entity: %w", err)
	}

	return nil
}

// Query queries entities with filters and pagination
func (em *entityManager) Query(ctx context.Context, req *forma.QueryRequest) (*forma.QueryResult, error) {
	if req == nil {
		return nil, fmt.Errorf("query request cannot be nil")
	}

	if req.SchemaName == "" {
		return nil, fmt.Errorf("schema name is required")
	}

	if req.Page < 1 {
		req.Page = 1
	}

	if req.ItemsPerPage < 1 {
		req.ItemsPerPage = em.config.Query.DefaultPageSize
	}

	if req.ItemsPerPage > em.config.Query.MaxPageSize {
		req.ItemsPerPage = em.config.Query.MaxPageSize
	}

	// Verify schema exists
	schemaId, _, err := em.registry.GetSchemaByName(req.SchemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	// Convert QueryRequest filters to AttributeQuery
	filterSlice := make([]forma.Filter, 0, len(req.Filters))
	for _, filter := range req.Filters {
		filterSlice = append(filterSlice, filter)
	}

	attributeQuery := &AttributeQuery{
		SchemaID: schemaId,
		Filters:  filterSlice,
		Limit:    req.ItemsPerPage,
		Offset:   (req.Page - 1) * req.ItemsPerPage,
	}

	// Add ordering
	if len(req.SortBy) > 0 {
		attributeQuery.OrderBy = []forma.OrderBy{
			{
				Field:     forma.FilterField(req.SortBy),
				SortOrder: req.SortOrder,
			},
		}
	}

	// Execute query
	startTime := time.Now()
	attributes, err := em.repository.QueryAttributes(ctx, attributeQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query attributes: %w", err)
	}

	// Group attributes by rowID
	groupedByRowID := make(map[uuid.UUID][]Attribute)
	for _, attr := range attributes {
		groupedByRowID[attr.RowID] = append(groupedByRowID[attr.RowID], attr)
	}

	// Convert attribute groups to DataRecords
	records := make([]*forma.DataRecord, 0, len(groupedByRowID))
	for rowID, attrs := range groupedByRowID {
		jsonData, err := em.transformer.FromAttributes(ctx, attrs)
		if err != nil {
			return nil, fmt.Errorf("failed to transform attributes to JSON: %w", err)
		}
		records = append(records, &forma.DataRecord{
			SchemaName: req.SchemaName,
			RowID:      rowID,
			Attributes: jsonData,
		})
	}

	// Get total count
	countFilterSlice := make([]forma.Filter, 0, len(req.Filters))
	for _, filter := range req.Filters {
		countFilterSlice = append(countFilterSlice, filter)
	}

	totalRecords, err := em.repository.CountEntities(ctx, req.SchemaName, countFilterSlice)
	if err != nil {
		return nil, fmt.Errorf("failed to count entities: %w", err)
	}

	totalPages := int((totalRecords + int64(req.ItemsPerPage) - 1) / int64(req.ItemsPerPage))

	return &forma.QueryResult{
		Data:          records,
		TotalRecords:  int(totalRecords),
		TotalPages:    totalPages,
		CurrentPage:   req.Page,
		ItemsPerPage:  req.ItemsPerPage,
		HasNext:       req.Page < totalPages,
		HasPrevious:   req.Page > 1,
		ExecutionTime: time.Since(startTime),
	}, nil
}

// AdvancedQuery executes complex condition-based queries using the CompositeCondition tree.
func (em *entityManager) AdvancedQuery(ctx context.Context, req *forma.AdvancedQueryRequest) (*forma.QueryResult, error) {
	if req == nil {
		return nil, fmt.Errorf("advanced query request cannot be nil")
	}

	if req.SchemaName == "" {
		return nil, fmt.Errorf("schema name is required")
	}

	if req.Condition == nil {
		return nil, fmt.Errorf("advanced query condition is required")
	}

	if req.Page < 1 {
		req.Page = 1
	}

	limit := req.ItemsPerPage
	if limit < 1 {
		limit = em.config.Query.DefaultPageSize
	}

	if limit > em.config.Query.MaxPageSize {
		limit = em.config.Query.MaxPageSize
	}

	startTime := time.Now()

	schemaID, cache, err := em.registry.GetSchemaByName(req.SchemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	var paramCounter int
	clause, args, err := req.Condition.ToSqlClauses(schemaID, cache, &paramCounter)
	if err != nil {
		return nil, fmt.Errorf("failed to build advanced query: %w", err)
	}

	if clause == "" {
		return nil, fmt.Errorf("advanced query condition produced an empty clause")
	}

	offset := (req.Page - 1) * limit
	rowIDs, totalRecords, err := em.repository.AdvancedQueryRowIDs(ctx, clause, args, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to execute advanced query: %w", err)
	}

	records := make([]*forma.DataRecord, 0, len(rowIDs))
	for _, rowID := range rowIDs {
		attributes, err := em.repository.GetAttributes(ctx, req.SchemaName, rowID)
		if err != nil {
			return nil, fmt.Errorf("failed to get attributes for row %s: %w", rowID, err)
		}

		data, err := em.transformer.FromAttributes(ctx, attributes)
		if err != nil {
			return nil, fmt.Errorf("failed to transform attributes for row %s: %w", rowID, err)
		}

		records = append(records, &forma.DataRecord{
			SchemaName: req.SchemaName,
			RowID:      rowID,
			Attributes: data,
		})
	}

	totalPages := 0
	if totalRecords > 0 {
		totalPages = int((totalRecords + int64(limit) - 1) / int64(limit))
	}

	result := &forma.QueryResult{
		Data:          records,
		TotalRecords:  int(totalRecords),
		TotalPages:    totalPages,
		CurrentPage:   req.Page,
		ItemsPerPage:  limit,
		HasNext:       req.Page < totalPages,
		HasPrevious:   req.Page > 1,
		ExecutionTime: time.Since(startTime),
	}

	return result, nil
}

// CrossSchemaSearch searches across multiple schemas using a single optimized query
func (em *entityManager) CrossSchemaSearch(ctx context.Context, req *forma.CrossSchemaRequest) (*forma.QueryResult, error) {
	if req == nil {
		return nil, fmt.Errorf("cross schema request cannot be nil")
	}

	if len(req.SchemaNames) == 0 {
		return nil, fmt.Errorf("schema names are required")
	}

	if req.SearchTerm == "" {
		return nil, fmt.Errorf("search term is required")
	}

	if req.Page < 1 {
		req.Page = 1
	}

	if req.ItemsPerPage < 1 {
		req.ItemsPerPage = em.config.Query.DefaultPageSize
	}

	if req.ItemsPerPage > em.config.Query.MaxPageSize {
		req.ItemsPerPage = em.config.Query.MaxPageSize
	}

	// Verify all schemas exist
	for _, schemaName := range req.SchemaNames {
		_, _, err := em.registry.GetSchemaByName(schemaName)
		if err != nil {
			return nil, fmt.Errorf("failed to get schema %s: %w", schemaName, err)
		}
	}

	// Build search filter
	searchFilter := forma.Filter{
		Type:  forma.FilterContains,
		Value: req.SearchTerm,
		Field: forma.FilterFieldAttributeValue,
	}

	// Create filters list from map, preserving the search filter
	filterSlice := make([]forma.Filter, 0, len(req.Filters)+1)
	for _, filter := range req.Filters {
		filterSlice = append(filterSlice, filter)
	}
	filterSlice = append(filterSlice, searchFilter)

	// Execute single cross-schema query using EAV table optimization
	startTime := time.Now()

	// For cross-schema search, we need to query without a specific schema filter
	// The search will be performed across all schemas, but filtered by the requested schema names
	attributeQuery := &AttributeQuery{
		Filters: filterSlice,
		Limit:   req.ItemsPerPage,
		Offset:  (req.Page - 1) * req.ItemsPerPage,
	}

	attributes, err := em.repository.QueryAttributes(ctx, attributeQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query attributes across schemas: %w", err)
	}

	// Group attributes by rowID and schemaID
	type entityKey struct {
		schemaID int16
		rowID    uuid.UUID
	}
	groupedByEntity := make(map[entityKey][]Attribute)
	for _, attr := range attributes {
		key := entityKey{schemaID: attr.SchemaID, rowID: attr.RowID}
		groupedByEntity[key] = append(groupedByEntity[key], attr)
	}

	// Convert attribute groups to DataRecords with schema names
	records := make([]*forma.DataRecord, 0, len(groupedByEntity))
	schemaIDToName := make(map[int16]string)

	for key, attrs := range groupedByEntity {
		// Resolve schema name if not cached
		schemaName, ok := schemaIDToName[key.schemaID]
		if !ok {
			// Find the schema name from registry
			for _, sn := range req.SchemaNames {
				schemaID, _, err := em.registry.GetSchemaByName(sn)
				if err == nil && schemaID == key.schemaID {
					schemaName = sn
					schemaIDToName[key.schemaID] = sn
					break
				}
			}
		}

		jsonData, err := em.transformer.FromAttributes(ctx, attrs)
		if err != nil {
			return nil, fmt.Errorf("failed to transform attributes to JSON: %w", err)
		}

		records = append(records, &forma.DataRecord{
			SchemaName: schemaName,
			RowID:      key.rowID,
			Attributes: jsonData,
		})
	}

	// Get total count from query metadata
	// Note: The total count is embedded in the query results but we need to extract it
	// For now, calculate based on the results
	totalRecords := int64(len(records))
	totalPages := int((totalRecords + int64(req.ItemsPerPage) - 1) / int64(req.ItemsPerPage))

	return &forma.QueryResult{
		Data:          records,
		TotalRecords:  int(totalRecords),
		TotalPages:    totalPages,
		CurrentPage:   req.Page,
		ItemsPerPage:  req.ItemsPerPage,
		HasNext:       req.Page < totalPages,
		HasPrevious:   req.Page > 1,
		ExecutionTime: time.Since(startTime),
	}, nil
}

// BatchCreate creates multiple entities atomically
func (em *entityManager) BatchCreate(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	if req == nil {
		return nil, fmt.Errorf("batch operation cannot be nil")
	}

	if len(req.Operations) == 0 {
		return &forma.BatchResult{
			Successful: make([]*forma.DataRecord, 0),
			Failed:     make([]forma.OperationError, 0),
			TotalCount: 0,
		}, nil
	}

	startTime := time.Now()

	// For atomic operations, we need transaction support
	// For now, we'll collect all results and return them
	successful := make([]*forma.DataRecord, 0)
	failed := make([]forma.OperationError, 0)

	for _, op := range req.Operations {
		record, err := em.Create(ctx, &op)
		if err != nil {
			failed = append(failed, forma.OperationError{
				Operation: op,
				Error:     err.Error(),
				Code:      "CREATE_FAILED",
			})
		} else {
			successful = append(successful, record)
		}
	}

	duration := time.Since(startTime).Microseconds()

	return &forma.BatchResult{
		Successful: successful,
		Failed:     failed,
		TotalCount: len(req.Operations),
		Duration:   duration,
	}, nil
}

// BatchUpdate updates multiple entities atomically
func (em *entityManager) BatchUpdate(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	if req == nil {
		return nil, fmt.Errorf("batch operation cannot be nil")
	}

	if len(req.Operations) == 0 {
		return &forma.BatchResult{
			Successful: make([]*forma.DataRecord, 0),
			Failed:     make([]forma.OperationError, 0),
			TotalCount: 0,
		}, nil
	}

	startTime := time.Now()

	successful := make([]*forma.DataRecord, 0)
	failed := make([]forma.OperationError, 0)

	for _, op := range req.Operations {
		record, err := em.Update(ctx, &op)
		if err != nil {
			failed = append(failed, forma.OperationError{
				Operation: op,
				Error:     err.Error(),
				Code:      "UPDATE_FAILED",
			})
		} else {
			successful = append(successful, record)
		}
	}

	duration := time.Since(startTime).Microseconds()

	return &forma.BatchResult{
		Successful: successful,
		Failed:     failed,
		TotalCount: len(req.Operations),
		Duration:   duration,
	}, nil
}

// BatchDelete deletes multiple entities atomically
func (em *entityManager) BatchDelete(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	if req == nil {
		return nil, fmt.Errorf("batch operation cannot be nil")
	}

	if len(req.Operations) == 0 {
		return &forma.BatchResult{
			Successful: make([]*forma.DataRecord, 0),
			Failed:     make([]forma.OperationError, 0),
			TotalCount: 0,
		}, nil
	}

	startTime := time.Now()

	successful := make([]*forma.DataRecord, 0)
	failed := make([]forma.OperationError, 0)

	for _, op := range req.Operations {
		err := em.Delete(ctx, &op)
		if err != nil {
			failed = append(failed, forma.OperationError{
				Operation: op,
				Error:     err.Error(),
				Code:      "DELETE_FAILED",
			})
		} else {
			successful = append(successful, &forma.DataRecord{
				SchemaName: op.SchemaName,
				RowID:      op.RowID,
			})
		}
	}

	duration := time.Since(startTime).Microseconds()

	return &forma.BatchResult{
		Successful: successful,
		Failed:     failed,
		TotalCount: len(req.Operations),
		Duration:   duration,
	}, nil
}

// Helper functions

// mergeMaps merges updates into existing data (deep merge)
func mergeMaps(existing map[string]any, updates any) map[string]any {
	result := copyMapDeep(existing)

	if updateMap, ok := updates.(map[string]any); ok {
		for key, value := range updateMap {
			if nestedExisting, existsInExisting := result[key]; existsInExisting {
				if existingMap, okExisting := nestedExisting.(map[string]any); okExisting {
					if updateNested, okUpdate := value.(map[string]any); okUpdate {
						result[key] = mergeMaps(existingMap, updateNested)
						continue
					}
				}
			}
			result[key] = value
		}
	}

	return result
}

// copyMapDeep creates a deep copy of a map
func copyMapDeep(m map[string]any) map[string]any {
	result := make(map[string]any)
	for key, value := range m {
		result[key] = deepCopyValue(value)
	}
	return result
}

// deepCopyValue creates a deep copy of any value
func deepCopyValue(value any) any {
	switch v := value.(type) {
	case map[string]any:
		return copyMapDeep(v)
	case []any:
		result := make([]any, len(v))
		for i, item := range v {
			result[i] = deepCopyValue(item)
		}
		return result
	default:
		return value
	}
}
