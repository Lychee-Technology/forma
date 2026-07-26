package internal

import (
	"context"
	"fmt"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemavalidate"

	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

type entityManager struct {
	transformer          model.PersistentRecordTransformer
	repository           model.PersistentRecordRepository
	federatedQueryEngine model.FederatedQueryEngine
	registry             forma.SchemaRegistry
	config               *forma.Config
	relations            *RelationIndex

	// validator is nil when schema validation is unconfigured. Callers must skip
	// validation entirely in that case: Validate on a nil validator returns an
	// error, not a no-op, and a plain one that would surface as an operator-facing
	// 500 on every write rather than being ignored.
	validator             *schemavalidate.Validator
	validateUpdatesStrict bool

	crud     *entityCRUDService
	query    *entityQueryService
	batch    *entityBatchService
	relation *entityRelationService
}

var _ forma.EntityManager = (*entityManager)(nil)

// NewEntityManager creates a new EntityManager instance
func NewEntityManager(
	transformer model.PersistentRecordTransformer,
	repository model.PersistentRecordRepository,
	federatedQueryEngine model.FederatedQueryEngine,
	registry forma.SchemaRegistry,
	config *forma.Config,
	validator *schemavalidate.Validator,
) forma.EntityManager {
	if config == nil {
		config = forma.DefaultConfig(registry)
		zap.S().Warn("entity manager config is nil; falling back to default config")
	}

	var relationIdx *RelationIndex
	if config.Entity.SchemaDirectory != "" {
		idx, err := LoadRelationIndex(config.Entity.SchemaDirectory)
		if err != nil {
			zap.S().Warnw("failed to load schema relations", "error", err)
		} else {
			relationIdx = idx
		}
	}
	em := &entityManager{
		transformer:          transformer,
		repository:           repository,
		federatedQueryEngine: federatedQueryEngine,
		registry:             registry,
		config:               config,
		relations:            relationIdx,

		validator:             validator,
		validateUpdatesStrict: config.Entity.ValidateUpdatesStrict,
	}
	em.relation = newEntityRelationService(em)
	em.crud = newEntityCRUDService(em)
	em.query = newEntityQueryService(em)
	em.batch = newEntityBatchService(em, em.crud)
	return em
}

func (em *entityManager) storageTables() model.StorageTables {
	if em == nil || em.config == nil {
		return model.StorageTables{}
	}
	tables := model.StorageTables{}
	if em.config.Database.TableNames.EntityMain != "" {
		tables.EntityMain = em.config.Database.TableNames.EntityMain
	}
	if em.config.Database.TableNames.EAVData != "" {
		tables.EAVData = em.config.Database.TableNames.EAVData
	}
	if em.config.Database.TableNames.SchemaRegistry != "" {
		tables.SchemaRegistry = em.config.Database.TableNames.SchemaRegistry
	}
	if em.config.Database.TableNames.ChangeLog != "" {
		tables.ChangeLog = em.config.Database.TableNames.ChangeLog
	}

	return tables
}

func (em *entityManager) toDataRecord(ctx context.Context, schemaName string, record *model.PersistentRecord) (*forma.DataRecord, error) {
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

// Create creates a new entity with the provided data.
func (em *entityManager) Create(ctx context.Context, req *forma.EntityOperation) (*forma.DataRecord, error) {
	return em.crud.Create(ctx, req)
}

// Get retrieves an entity by schema name and row ID.
func (em *entityManager) Get(ctx context.Context, req *forma.QueryRequest) (*forma.DataRecord, error) {
	return em.crud.Get(ctx, req)
}

// Update updates an existing entity.
func (em *entityManager) Update(ctx context.Context, req *forma.EntityOperation) (*forma.DataRecord, error) {
	return em.crud.Update(ctx, req)
}

// Delete deletes an entity.
func (em *entityManager) Delete(ctx context.Context, req *forma.EntityOperation) error {
	return em.crud.Delete(ctx, req)
}

// Query queries entities with filters and pagination.
func (em *entityManager) Query(ctx context.Context, req *forma.QueryRequest) (*forma.QueryResult, error) {
	return em.query.Query(ctx, req)
}

// CrossSchemaSearch searches across multiple schemas using a single optimized query.
func (em *entityManager) CrossSchemaSearch(ctx context.Context, req *forma.CrossSchemaRequest) (*forma.QueryResult, error) {
	return em.query.CrossSchemaSearch(ctx, req)
}

// BatchCreate creates multiple entities.
func (em *entityManager) BatchCreate(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	return em.batch.BatchCreate(ctx, req)
}

// BatchUpdate updates multiple entities.
func (em *entityManager) BatchUpdate(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	return em.batch.BatchUpdate(ctx, req)
}

// BatchDelete deletes multiple entities.
func (em *entityManager) BatchDelete(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	return em.batch.BatchDelete(ctx, req)
}

func (em *entityManager) enrichDataRecords(ctx context.Context, schemaName string, requested []string, records ...*forma.DataRecord) error {
	return em.relation.enrichDataRecords(ctx, schemaName, requested, records...)
}
