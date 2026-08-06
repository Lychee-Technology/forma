package internal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/lychee-technology/forma/internal/transform"

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

	// closers holds resources the manager owns and must release on Close,
	// registered at construction via WithCloser (#302). Directly-constructed
	// managers (the e2e harness) register nothing: their resources are owned
	// by the Env.
	closers []io.Closer
	// closeOnce guards teardown: Close is public API surface, so concurrent
	// callers must not double-close owned resources (#327 review). closeErr
	// caches the joined result so every caller — first, repeated, or racing —
	// observes the same outcome; sync.Once's happens-before edge makes the
	// cached read safe.
	closeOnce sync.Once
	closeErr  error
}

var _ forma.EntityManager = (*entityManager)(nil)

// EntityManagerOption customizes NewEntityManager construction.
type EntityManagerOption func(*entityManager)

// WithCloser registers a resource the manager owns and must release on Close.
// Callers pass only non-nil resources; a typed-nil pointer boxed in io.Closer
// would not compare equal to nil here, so the guard lives at the call site.
func WithCloser(c io.Closer) EntityManagerOption {
	return func(em *entityManager) {
		if c != nil {
			em.closers = append(em.closers, c)
		}
	}
}

// Close releases every registered resource exactly once. All closers run even
// when one fails; each failure is wrapped with the resource's type before
// joining (errors.Is still reaches the underlying error). Safe for concurrent
// use: teardown is guarded by closeOnce and the joined result is cached, so
// every call returns the same outcome.
func (em *entityManager) Close() error {
	em.closeOnce.Do(func() {
		var errs []error
		for _, c := range em.closers {
			if err := c.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close entity manager resource %T: %w", c, err))
			}
		}
		em.closers = nil
		em.closeErr = errors.Join(errs...)
	})
	return em.closeErr
}

// NewEntityManager creates a new EntityManager instance
func NewEntityManager(
	transformer model.PersistentRecordTransformer,
	repository model.PersistentRecordRepository,
	federatedQueryEngine model.FederatedQueryEngine,
	registry forma.SchemaRegistry,
	config *forma.Config,
	validator *schemavalidate.Validator,
	opts ...EntityManagerOption,
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
	// The relation index is loaded here, from config, so the transformer this
	// manager was handed predates it. Install the roots now: without them the
	// EAV required-policy check enforces policies on relation-root children,
	// which #314 ruled must never gate a write (#315).
	if aware, ok := transformer.(transform.RelationRootsAware); ok {
		aware.SetRelationRoots(relationIdx.RelationRoots)
	} else if relationIdx != nil {
		// Loud, not silent: a transformer decorator that does not forward the
		// install leaves relation-root children under required-policy
		// enforcement, which rejects writes #314 ruled acceptable.
		zap.S().Warn("transformer does not accept relation roots; required-policy enforcement will not honour the #314 relation-root carve-out")
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
	for _, opt := range opts {
		opt(em)
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
