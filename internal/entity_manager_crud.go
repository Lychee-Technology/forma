package internal

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

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
