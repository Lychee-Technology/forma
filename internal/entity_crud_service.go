package internal

import (
	"context"
	"fmt"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemavalidate"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

type entityCRUDService struct {
	transformer       model.PersistentRecordTransformer
	repository        model.PersistentRecordRepository
	registry          forma.SchemaRegistry
	relations         *RelationIndex
	toDataRecord      dataRecordConverter
	enrichDataRecords dataRecordEnricher
	storageTables     storageTablesResolver

	// validator is nil when schema validation is unconfigured. Callers must skip
	// validation entirely in that case: Validate on a nil validator returns an
	// error, not a no-op.
	validator             *schemavalidate.Validator
	validateUpdatesStrict bool
}

func newEntityCRUDService(em *entityManager) *entityCRUDService {
	if em == nil {
		return &entityCRUDService{}
	}
	return &entityCRUDService{
		transformer:       em.transformer,
		repository:        em.repository,
		registry:          em.registry,
		relations:         em.relations,
		toDataRecord:      em.toDataRecord,
		enrichDataRecords: em.enrichDataRecords,
		storageTables:     em.storageTables,

		validator:             em.validator,
		validateUpdatesStrict: em.validateUpdatesStrict,
	}
}

func (s *entityCRUDService) Create(ctx context.Context, req *forma.EntityOperation) (*forma.DataRecord, error) {
	if err := s.validateDependencies(); err != nil {
		return nil, err
	}

	if req == nil {
		return nil, fmt.Errorf("entity operation cannot be nil: %w", forma.ErrInvalidInput)
	}

	if req.SchemaName == "" {
		return nil, fmt.Errorf("schema name is required: %w", forma.ErrInvalidInput)
	}

	if req.Data == nil {
		return nil, fmt.Errorf("data is required for create operation: %w", forma.ErrInvalidInput)
	}

	// Get schema by name to obtain schema ID and the attribute cache the
	// validator needs to recognise literal dotted keys.
	schemaID, schemaCache, err := s.registry.GetSchemaAttributeCacheByName(req.SchemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	rowID := uuid.Must(uuid.NewV7())
	inputData := req.Data
	if s.relations != nil {
		inputData = s.relations.StripComputedFields(req.SchemaName, req.Data)
	}

	// Creates always enforce. Validated after stripping so that what is checked
	// is what is stored: computed relation fields are derived on read and never
	// persisted, so validating them would judge a document no row will hold.
	//
	// Hazard: a schema that lists a relation root in "required" becomes
	// unwritable, and unfixably so — the field is stripped before the validator
	// sees it, so sending it does not help. No shipped schema does this
	// (x-relation occurs once, visit.json's contactSnapshot, and is not
	// required), but a new one that did would reject every create and update to
	// it. Validate before stripping if that ever happens.
	if err := validateWritePayload(s.validator, schemaID, schemaCache, inputData, true); err != nil {
		return nil, fmt.Errorf("failed to validate create payload: %w", err)
	}

	zap.S().Debugw("Creating entity", "schemaName", req.SchemaName, "schemaID", schemaID, "rowID", rowID)
	record, err := s.transformer.ToPersistentRecord(ctx, schemaID, rowID, inputData)
	if err != nil {
		return nil, fmt.Errorf("failed to transform data to persistent record: %w", err)
	}

	if err := s.repository.InsertPersistentRecord(ctx, s.resolveTables(), record); err != nil {
		return nil, fmt.Errorf("failed to insert persistent record: %w", err)
	}

	attributes, err := s.transformer.FromPersistentRecord(ctx, record)
	if err != nil {
		return nil, fmt.Errorf("failed to transform persistent record to JSON: %w", err)
	}

	return &forma.DataRecord{
		SchemaName: req.SchemaName,
		RowID:      rowID,
		Attributes: attributes,
	}, nil
}

func (s *entityCRUDService) Get(ctx context.Context, req *forma.QueryRequest) (*forma.DataRecord, error) {
	if err := s.validateDependencies(); err != nil {
		return nil, err
	}

	if req == nil {
		return nil, fmt.Errorf("query request cannot be nil: %w", forma.ErrInvalidInput)
	}

	if req.SchemaName == "" {
		return nil, fmt.Errorf("schema name is required: %w", forma.ErrInvalidInput)
	}

	if req.RowID == nil {
		return nil, fmt.Errorf("row ID is required for get operation: %w", forma.ErrInvalidInput)
	}

	// Verify schema exists and fetch schema ID.
	schemaID, _, err := s.registry.GetSchemaAttributeCacheByName(req.SchemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	record, err := s.repository.GetPersistentRecord(ctx, s.resolveTables(), schemaID, *req.RowID)
	if err != nil {
		return nil, fmt.Errorf("failed to load persistent record: %w", err)
	}
	if record == nil {
		return nil, fmt.Errorf("entity not found: %s/%s: %w", req.SchemaName, req.RowID, forma.ErrNotFound)
	}

	dataRecord, err := s.toDataRecord(ctx, req.SchemaName, record)
	if err != nil {
		return nil, err
	}

	if err := s.enrichDataRecords(ctx, req.SchemaName, req.Attrs, dataRecord); err != nil {
		return nil, err
	}

	applyProjection([]*forma.DataRecord{dataRecord}, req.Attrs)

	return dataRecord, nil
}

func (s *entityCRUDService) Update(ctx context.Context, req *forma.EntityOperation) (*forma.DataRecord, error) {
	if err := s.validateDependencies(); err != nil {
		return nil, err
	}

	if req == nil {
		return nil, fmt.Errorf("entity operation cannot be nil: %w", forma.ErrInvalidInput)
	}

	if req.SchemaName == "" {
		return nil, fmt.Errorf("schema name is required: %w", forma.ErrInvalidInput)
	}

	if req.RowID == (uuid.UUID{}) {
		return nil, fmt.Errorf("row ID is required for update operation: %w", forma.ErrInvalidInput)
	}

	if req.Updates == nil {
		return nil, fmt.Errorf("updates are required for update operation: %w", forma.ErrInvalidInput)
	}

	// Get schema by name.
	schemaID, schemaCache, err := s.registry.GetSchemaAttributeCacheByName(req.SchemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	tables := s.resolveTables()
	existingRecord, err := s.repository.GetPersistentRecord(ctx, tables, schemaID, req.RowID)
	if err != nil {
		return nil, fmt.Errorf("failed to load existing record: %w", err)
	}
	if existingRecord == nil {
		return nil, fmt.Errorf("entity not found: %s/%s: %w", req.SchemaName, req.RowID, forma.ErrNotFound)
	}

	existingData, err := s.transformer.FromPersistentRecord(ctx, existingRecord)
	if err != nil {
		return nil, fmt.Errorf("failed to transform existing record: %w", err)
	}

	mergedData := mergeMaps(existingData, req.Updates)
	if s.relations != nil {
		mergedData = s.relations.StripComputedFields(req.SchemaName, mergedData)
	}

	// The *merged* document is what gets validated: a partial update that does
	// not mention a required attribute must still succeed. The relation-root
	// hazard noted in Create applies here too.
	if err := validateWritePayload(s.validator, schemaID, schemaCache, mergedData, s.validateUpdatesStrict); err != nil {
		return nil, fmt.Errorf("failed to validate update payload: %w", err)
	}

	updatedRecord, err := s.transformer.ToPersistentRecord(ctx, schemaID, req.RowID, mergedData)
	if err != nil {
		return nil, fmt.Errorf("failed to transform merged data: %w", err)
	}

	updatedRecord.CreatedAt = existingRecord.CreatedAt
	updatedRecord.DeletedAt = existingRecord.DeletedAt

	if err := s.repository.UpdatePersistentRecord(ctx, tables, updatedRecord); err != nil {
		return nil, fmt.Errorf("failed to update persistent record: %w", err)
	}

	return &forma.DataRecord{
		SchemaName: req.SchemaName,
		RowID:      req.RowID,
		Attributes: mergedData,
	}, nil
}

func (s *entityCRUDService) Delete(ctx context.Context, req *forma.EntityOperation) error {
	if err := s.validateDependencies(); err != nil {
		return err
	}

	if req == nil {
		return fmt.Errorf("entity operation cannot be nil: %w", forma.ErrInvalidInput)
	}

	if req.SchemaName == "" {
		return fmt.Errorf("schema name is required: %w", forma.ErrInvalidInput)
	}

	if req.RowID == (uuid.UUID{}) {
		return fmt.Errorf("row ID is required for delete operation: %w", forma.ErrInvalidInput)
	}

	schemaID, _, err := s.registry.GetSchemaAttributeCacheByName(req.SchemaName)
	if err != nil {
		return fmt.Errorf("failed to get schema: %w", err)
	}

	if err := s.repository.DeletePersistentRecord(ctx, s.resolveTables(), schemaID, req.RowID); err != nil {
		return fmt.Errorf("failed to delete persistent record: %w", err)
	}

	return nil
}

func (s *entityCRUDService) validateDependencies() error {
	if s.registry == nil || s.repository == nil || s.transformer == nil || s.toDataRecord == nil || s.enrichDataRecords == nil {
		return fmt.Errorf("entity crud service is not initialized")
	}
	return nil
}

func (s *entityCRUDService) resolveTables() model.StorageTables {
	if s.storageTables == nil {
		return model.StorageTables{}
	}
	return s.storageTables()
}
