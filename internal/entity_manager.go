package internal

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

type entityManager struct {
	transformer PersistentRecordTransformer
	repository  PersistentRecordRepository
	registry    forma.SchemaRegistry
	config      *forma.Config
	relations   *RelationIndex
}

// NewEntityManager creates a new EntityManager instance
func NewEntityManager(
	transformer PersistentRecordTransformer,
	repository PersistentRecordRepository,
	registry forma.SchemaRegistry,
	config *forma.Config,
) forma.EntityManager {
	var relationIdx *RelationIndex
	if config != nil {
		idx, err := LoadRelationIndex(config.Entity.SchemaDirectory)
		if err != nil {
			zap.S().Warnw("failed to load schema relations", "error", err)
		} else {
			relationIdx = idx
		}
	}
	return &entityManager{
		transformer: transformer,
		repository:  repository,
		registry:    registry,
		config:      config,
		relations:   relationIdx,
	}
}

func (em *entityManager) storageTables() StorageTables {
	if em == nil || em.config == nil {
		return StorageTables{}
	}
	tables := StorageTables{}
	if em.config.Database.TableNames.EntityMain != "" {
		tables.EntityMain = em.config.Database.TableNames.EntityMain
	}
	if em.config.Database.TableNames.EAVData != "" {
		tables.EAVData = em.config.Database.TableNames.EAVData
	}
	return tables
}

func (em *entityManager) toDataRecord(ctx context.Context, schemaName string, record *PersistentRecord) (*forma.DataRecord, error) {
	if record == nil {
		return nil, fmt.Errorf("persistent record cannot be nil")
	}
	resolvedName := schemaName
	if resolvedName == "" {
		name, _, err := em.registry.GetSchemaAttributeCacheByID(record.SchemaID)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve schema name for id %d: %w", record.SchemaID, err)
		}
		resolvedName = name
	}

	attributes, err := em.transformer.FromPersistentRecord(ctx, record)
	if err != nil {
		return nil, fmt.Errorf("failed to transform persistent record to JSON: %w", err)
	}

	return &forma.DataRecord{
		SchemaName: resolvedName,
		RowID:      record.RowID,
		Attributes: attributes,
	}, nil
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
	schemaID, _, err := em.registry.GetSchemaAttributeCacheByName(req.SchemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	rowID := uuid.Must(uuid.NewV7())
	inputData := req.Data
	if em.relations != nil {
		inputData = em.relations.StripComputedFields(req.SchemaName, req.Data)
	}
	zap.S().Debugw("Creating entity", "schemaName", req.SchemaName, "schemaID", schemaID, "rowID", rowID)
	record, err := em.transformer.ToPersistentRecord(ctx, schemaID, rowID, inputData)
	if err != nil {
		return nil, fmt.Errorf("failed to transform data to persistent record: %w", err)
	}

	tables := em.storageTables()
	if err := em.repository.InsertPersistentRecord(ctx, tables, record); err != nil {
		return nil, fmt.Errorf("failed to insert persistent record: %w", err)
	}

	attributes, err := em.transformer.FromPersistentRecord(ctx, record)
	if err != nil {
		return nil, fmt.Errorf("failed to transform persistent record to JSON: %w", err)
	}

	return &forma.DataRecord{
		SchemaName: req.SchemaName,
		RowID:      rowID,
		Attributes: attributes,
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

	// Verify schema exists and fetch schema ID
	schemaID, _, err := em.registry.GetSchemaAttributeCacheByName(req.SchemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	record, err := em.repository.GetPersistentRecord(ctx, em.storageTables(), schemaID, *req.RowID)
	if err != nil {
		return nil, fmt.Errorf("failed to load persistent record: %w", err)
	}
	if record == nil {
		return nil, fmt.Errorf("entity not found: %s/%s", req.SchemaName, req.RowID)
	}

	dataRecord, err := em.toDataRecord(ctx, req.SchemaName, record)
	if err != nil {
		return nil, err
	}

	if err := em.enrichDataRecords(ctx, req.SchemaName, req.Attrs, dataRecord); err != nil {
		return nil, err
	}

	applyProjection([]*forma.DataRecord{dataRecord}, req.Attrs)

	return dataRecord, nil
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
	schemaID, _, err := em.registry.GetSchemaAttributeCacheByName(req.SchemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	tables := em.storageTables()
	existingRecord, err := em.repository.GetPersistentRecord(ctx, tables, schemaID, req.RowID)
	if err != nil {
		return nil, fmt.Errorf("failed to load existing record: %w", err)
	}
	if existingRecord == nil {
		return nil, fmt.Errorf("entity not found: %s/%s", req.SchemaName, req.RowID)
	}

	existingData, err := em.transformer.FromPersistentRecord(ctx, existingRecord)
	if err != nil {
		return nil, fmt.Errorf("failed to transform existing record: %w", err)
	}

	mergedData := mergeMaps(existingData, req.Updates)
	if em.relations != nil {
		mergedData = em.relations.StripComputedFields(req.SchemaName, mergedData)
	}

	updatedRecord, err := em.transformer.ToPersistentRecord(ctx, schemaID, req.RowID, mergedData)
	if err != nil {
		return nil, fmt.Errorf("failed to transform merged data: %w", err)
	}

	updatedRecord.CreatedAt = existingRecord.CreatedAt
	updatedRecord.DeletedAt = existingRecord.DeletedAt

	if err := em.repository.UpdatePersistentRecord(ctx, tables, updatedRecord); err != nil {
		return nil, fmt.Errorf("failed to update persistent record: %w", err)
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

	schemaID, _, err := em.registry.GetSchemaAttributeCacheByName(req.SchemaName)
	if err != nil {
		return fmt.Errorf("failed to get schema: %w", err)
	}

	if err := em.repository.DeletePersistentRecord(ctx, em.storageTables(), schemaID, req.RowID); err != nil {
		return fmt.Errorf("failed to delete persistent record: %w", err)
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

	// Verify schema exists and get attribute metadata
	schemaId, schemaCache, err := em.registry.GetSchemaAttributeCacheByName(req.SchemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	sortOrder := req.SortOrder
	if sortOrder == "" {
		sortOrder = forma.SortOrderAsc
	}

	attributeOrders := make([]AttributeOrder, 0, len(req.SortBy))
	for _, sortAttr := range req.SortBy {
		meta, ok := schemaCache[sortAttr]
		if !ok {
			return nil, fmt.Errorf("cannot sort by unknown attribute '%s' in schema '%s'", sortAttr, req.SchemaName)
		}
		order := AttributeOrder{
			AttrID:    meta.AttributeID,
			ValueType: meta.ValueType,
			SortOrder: sortOrder,
		}
		// Check if attribute has column_binding to main table
		if meta.ColumnBinding != nil {
			order.StorageLocation = forma.AttributeStorageLocationMain
			order.ColumnName = string(meta.ColumnBinding.ColumnName)
		} else {
			order.StorageLocation = forma.AttributeStorageLocationEAV
		}
		attributeOrders = append(attributeOrders, order)
	}

	tables := em.storageTables()
	query := &PersistentRecordQuery{
		Tables:          tables,
		SchemaID:        schemaId,
		Condition:       req.Condition,
		AttributeOrders: attributeOrders,
		Limit:           req.ItemsPerPage,
		Offset:          (req.Page - 1) * req.ItemsPerPage,
	}

	startTime := time.Now()
	page, err := em.repository.QueryPersistentRecords(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query persistent records: %w", err)
	}

	records := make([]*forma.DataRecord, 0, len(page.Records))
	for _, record := range page.Records {
		dataRecord, err := em.toDataRecord(ctx, req.SchemaName, record)
		if err != nil {
			return nil, err
		}
		records = append(records, dataRecord)
	}

	if err := em.enrichDataRecords(ctx, req.SchemaName, req.Attrs, records...); err != nil {
		return nil, err
	}

	applyProjection(records, req.Attrs)

	totalPages := page.TotalPages
	if totalPages == 0 && page.TotalRecords > 0 && req.ItemsPerPage > 0 {
		totalPages = int((page.TotalRecords + int64(req.ItemsPerPage) - 1) / int64(req.ItemsPerPage))
	}

	zap.S().Infow("query results", "records", len(records), "totalPages", totalPages)

	return &forma.QueryResult{
		Data:          records,
		TotalRecords:  int(page.TotalRecords),
		TotalPages:    totalPages,
		CurrentPage:   req.Page,
		ItemsPerPage:  req.ItemsPerPage,
		HasNext:       req.Page < totalPages,
		HasPrevious:   req.Page > 1,
		ExecutionTime: time.Since(startTime),
	}, nil
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

	startTime := time.Now()
	tables := em.storageTables()

	// Build search condition - search for the term in text values
	// This is a simplified approach; you may want to extend this to search across multiple attributes
	var searchCondition forma.Condition = req.Condition
	if searchCondition == nil {
		// If no condition provided, create a default search condition
		// Note: This is a placeholder - you may want to implement more sophisticated search logic
		searchCondition = &forma.CompositeCondition{
			Logic:      forma.LogicAnd,
			Conditions: []forma.Condition{},
		}
	}

	type schemaContext struct {
		name      string
		id        int16
		condition forma.Condition
	}

	schemaContexts := make([]schemaContext, 0, len(req.SchemaNames))
	for _, schemaName := range req.SchemaNames {
		schemaID, _, err := em.registry.GetSchemaAttributeCacheByName(schemaName)
		if err != nil {
			return nil, fmt.Errorf("failed to get schema %s: %w", schemaName, err)
		}
		schemaContexts = append(schemaContexts, schemaContext{
			name:      schemaName,
			id:        schemaID,
			condition: searchCondition,
		})
	}

	schemaTotals := make([]int64, len(schemaContexts))
	for idx, schemaCtx := range schemaContexts {
		page, err := em.repository.QueryPersistentRecords(ctx, &PersistentRecordQuery{
			Tables:    tables,
			SchemaID:  schemaCtx.id,
			Condition: schemaCtx.condition,
			Limit:     1,
			Offset:    0,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to count records for schema %s: %w", schemaCtx.name, err)
		}
		schemaTotals[idx] = page.TotalRecords
	}

	var totalRecords int64
	for _, count := range schemaTotals {
		totalRecords += count
	}

	if totalRecords == 0 {
		return &forma.QueryResult{
			Data:          []*forma.DataRecord{},
			TotalRecords:  0,
			TotalPages:    0,
			CurrentPage:   req.Page,
			ItemsPerPage:  req.ItemsPerPage,
			HasNext:       false,
			HasPrevious:   req.Page > 1,
			ExecutionTime: time.Since(startTime),
		}, nil
	}

	offset := (req.Page - 1) * req.ItemsPerPage
	remaining := req.ItemsPerPage
	results := make([]*forma.DataRecord, 0, req.ItemsPerPage)
	skip := offset

	for idx, schemaCtx := range schemaContexts {
		count := int(schemaTotals[idx])
		if skip >= count {
			skip -= count
			continue
		}

		schemaOffset := skip
		skip = 0
		avail := count - schemaOffset
		schemaLimit := remaining
		if avail < schemaLimit {
			schemaLimit = avail
		}
		if schemaLimit <= 0 {
			continue
		}

		page, err := em.repository.QueryPersistentRecords(ctx, &PersistentRecordQuery{
			Tables:    tables,
			SchemaID:  schemaCtx.id,
			Condition: schemaCtx.condition,
			Limit:     schemaLimit,
			Offset:    schemaOffset,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to fetch records for schema %s: %w", schemaCtx.name, err)
		}

		batchRecords := make([]*forma.DataRecord, 0, len(page.Records))
		for _, record := range page.Records {
			dataRecord, err := em.toDataRecord(ctx, schemaCtx.name, record)
			if err != nil {
				return nil, err
			}
			batchRecords = append(batchRecords, dataRecord)
		}

		if err := em.enrichDataRecords(ctx, schemaCtx.name, req.Attrs, batchRecords...); err != nil {
			return nil, err
		}

		applyProjection(batchRecords, req.Attrs)
		results = append(results, batchRecords...)

		remaining -= len(page.Records)
		if remaining <= 0 {
			break
		}
	}

	totalPages := int((totalRecords + int64(req.ItemsPerPage) - 1) / int64(req.ItemsPerPage))
	return &forma.QueryResult{
		Data:          results,
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
	zap.S().Debugw("BatchCreate called", "operationCount", len(req.Operations))
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
			zap.S().Warnw("BatchCreate operation failed", "operation", op, "error", err)
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
	zap.S().Debugw("BatchCreate completed", "successfulCount", len(successful), "failedCount", len(failed), "durationMicroseconds", duration)
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

	zap.S().Debugw("BatchUpdate called", "operationCount", len(req.Operations))
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

func (em *entityManager) enrichDataRecords(ctx context.Context, schemaName string, requested []string, records ...*forma.DataRecord) error {
	if em == nil || em.relations == nil || len(records) == 0 {
		return nil
	}

	rels := em.relations.Relations(schemaName)
	if len(rels) == 0 {
		return nil
	}

	requestedSet := make(map[string]struct{}, len(requested))
	for _, attr := range requested {
		requestedSet[attr] = struct{}{}
	}

	for _, rel := range rels {
		if len(requestedSet) > 0 {
			want := false
			for _, attr := range requested {
				if attr == rel.ChildPath || strings.HasPrefix(attr, rel.ChildPath+".") {
					want = true
					break
				}
			}
			if !want {
				continue
			}
		}

		fkBuckets := make(map[string][]*forma.DataRecord)
		for _, rec := range records {
			fkVal, ok := readStringAtPath(rec.Attributes, rel.ForeignKeyAttr)
			if !ok || fkVal == "" {
				if rel.ForeignKeyRequired {
					zap.S().Warnw("missing required parent foreign key", "schema", schemaName, "attr", rel.ForeignKeyAttr)
				}
				continue
			}
			fkBuckets[fkVal] = append(fkBuckets[fkVal], rec)
		}

		if len(fkBuckets) == 0 {
			continue
		}

		parents, err := em.fetchParents(ctx, rel, fkBuckets)
		if err != nil {
			return err
		}

		for fk, recs := range fkBuckets {
			parentAttrs, ok := parents[fk]
			if !ok {
				continue
			}

			fragment := getValueAtPath(parentAttrs, rel.ParentPath)
			if fragment == nil {
				continue
			}

			for _, rec := range recs {
				setNestedValue(rec.Attributes, rel.ChildPath, deepCopyValue(fragment))
			}
		}
	}

	return nil
}

func (em *entityManager) fetchParents(ctx context.Context, rel RelationDescriptor, fkBuckets map[string][]*forma.DataRecord) (map[string]map[string]any, error) {
	ids := make([]string, 0, len(fkBuckets))
	for id := range fkBuckets {
		ids = append(ids, id)
	}

	if len(ids) == 0 {
		return map[string]map[string]any{}, nil
	}

	parentSchemaID, _, err := em.registry.GetSchemaByName(rel.ParentSchema)
	if err != nil {
		return nil, fmt.Errorf("get parent schema %s: %w", rel.ParentSchema, err)
	}

	var cond forma.Condition
	if len(ids) == 1 {
		cond = &forma.KvCondition{Attr: rel.ParentIDAttr, Value: ids[0]}
	} else {
		conditions := make([]forma.Condition, 0, len(ids))
		for _, id := range ids {
			conditions = append(conditions, &forma.KvCondition{Attr: rel.ParentIDAttr, Value: id})
		}
		cond = &forma.CompositeCondition{Logic: forma.LogicOr, Conditions: conditions}
	}

	page, err := em.repository.QueryPersistentRecords(ctx, &PersistentRecordQuery{
		Tables:    em.storageTables(),
		SchemaID:  parentSchemaID,
		Condition: cond,
		Limit:     len(ids),
		Offset:    0,
	})
	if err != nil {
		return nil, fmt.Errorf("query parent records for schema %s: %w", rel.ParentSchema, err)
	}

	parents := make(map[string]map[string]any, len(page.Records))
	for _, rec := range page.Records {
		attrs, err := em.transformer.FromPersistentRecord(ctx, rec)
		if err != nil {
			return nil, fmt.Errorf("transform parent record for schema %s: %w", rel.ParentSchema, err)
		}
		parentID, _ := readStringAtPath(attrs, rel.ParentIDAttr)
		if parentID == "" {
			continue
		}
		parents[parentID] = attrs
	}

	return parents, nil
}

func applyProjection(records []*forma.DataRecord, attrs []string) {
	if len(attrs) == 0 {
		return
	}
	for _, rec := range records {
		rec.Attributes = FilterAttributes(rec.Attributes, attrs)
	}
}

func readStringAtPath(m map[string]any, path string) (string, bool) {
	val := getValueAtPath(m, path)
	if val == nil {
		return "", false
	}
	switch v := val.(type) {
	case string:
		return v, true
	default:
		return fmt.Sprintf("%v", v), true
	}
}

func getValueAtPath(m map[string]any, path string) any {
	if m == nil || path == "" {
		return m
	}
	segments := strings.Split(path, ".")
	current := any(m)
	for _, segment := range segments {
		asMap, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		next, exists := asMap[segment]
		if !exists {
			return nil
		}
		current = next
	}
	return current
}

func setNestedValue(m map[string]any, path string, value any) {
	if m == nil || path == "" {
		return
	}
	segments := strings.Split(path, ".")
	current := m
	for idx, segment := range segments {
		if idx == len(segments)-1 {
			current[segment] = value
			return
		}
		next, ok := current[segment].(map[string]any)
		if !ok {
			next = make(map[string]any)
			current[segment] = next
		}
		current = next
	}
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
