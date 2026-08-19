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
	// reportOnlyStats feeds the #317 milestone log line. Owned here so the CRUD
	// and batch services aggregate into one set of per-schema counts.
	reportOnlyStats *reportOnlyStats

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

// WithRelationIndex installs a relation index the caller has already built and
// validated, instead of letting the manager load its own.
//
// It exists because the composition root has to check relation declarations
// before it builds anything, and a second, independent load afterwards is not
// guaranteed to see what the first one saw: forma.SchemaRegistry may serve
// documents from a database or over a network, so it can change or fail between
// the two reads. When it does, the manager's own load is the one that fails, and
// the caller is left holding a construction error over declarations its
// preflight had already approved. Handing the validated instance forward removes
// the second read entirely (#318 review).
//
// A nil index is ignored rather than installed, so the option cannot become a
// way to switch stripping off by accident; the manager then self-loads as any
// direct constructor does. That self-load fails closed — NewEntityManager
// returns the error rather than warning and continuing with no index (#388) — so
// the option changes which read decides, never whether stripping happens.
func WithRelationIndex(idx *RelationIndex) EntityManagerOption {
	return func(em *entityManager) {
		if idx != nil {
			em.relations = idx
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

// NewEntityManager creates a new EntityManager instance, or fails.
//
// The one failure it has is the relation index, and it is why this returns an
// error at all (#388). A manager holding no index does not merely lose a
// feature, and loses it for every schema rather than for the offending one:
//
//   - StripComputedFields is an identity function on a nil receiver, so the
//     relation subtree becomes caller-writable and persistable again — the exact
//     state #318 exists to remove — and nothing says so at any point afterwards;
//   - RelationRoots answers nil on the same receiver, and the required-policy
//     check reads an empty set as "this attribute is not beneath a relation
//     root" (transform.RelationRoots.Covers, attribute_converter.go), so #315's
//     carve-out stops applying and payloads #314 ruled acceptable begin failing
//     with missing-required errors.
//
// Neither is visible to the caller on the returned manager, and both last for
// the process lifetime. So a registry fault that used to become a warning now
// becomes a refusal to build, and the caller decides what to do about it.
//
// A successfully built manager therefore always holds a non-nil index, from one
// of two places: WithRelationIndex, when the caller has already built and
// validated one (the composition root has), or a load of the registry performed
// here. LoadRelationIndex answers an empty index rather than nil for a registry
// that lists nothing and for a nil registry, so neither of those is a failure
// and neither leaves the field nil.
//
// The load is gated on the registry because the registry is what the index is
// built from. It used to be gated on config.Entity.SchemaDirectory, which
// stopped being the source: an embedder who registers a schema carrying
// x-relation but leaves SchemaDirectory empty gets stripping, enrichment and the
// #315 carve-out, all of which that schema asks for and none of which an
// unrelated path setting should have been deciding.
func NewEntityManager(
	transformer model.PersistentRecordTransformer,
	repository model.PersistentRecordRepository,
	federatedQueryEngine model.FederatedQueryEngine,
	registry forma.SchemaRegistry,
	config *forma.Config,
	validator *schemavalidate.Validator,
	opts ...EntityManagerOption,
) (forma.EntityManager, error) {
	if config == nil {
		config = forma.DefaultConfig(registry)
		zap.S().Warn("entity manager config is nil; falling back to default config")
	}

	em := &entityManager{
		transformer:          transformer,
		repository:           repository,
		federatedQueryEngine: federatedQueryEngine,
		registry:             registry,
		config:               config,

		validator:             validator,
		validateUpdatesStrict: config.Entity.ValidateUpdatesStrict,
		reportOnlyStats:       newReportOnlyStats(),
	}
	// Options run before the relation index is resolved, because one of them
	// supplies it: WithRelationIndex is how the composition root hands over the
	// instance it already validated, and a self-load afterwards would defeat the
	// point.
	for _, opt := range opts {
		opt(em)
	}
	if em.relations == nil {
		idx, err := LoadRelationIndex(registry)
		if err != nil {
			return nil, fmt.Errorf("failed to load schema relations: %w", err)
		}
		em.relations = idx
	}
	installRelationRoots(transformer, em.relations)

	em.relation = newEntityRelationService(em)
	em.crud = newEntityCRUDService(em)
	em.query = newEntityQueryService(em)
	em.batch = newEntityBatchService(em, em.crud)
	return em, nil
}

// installRelationRoots hands the transformer the relation roots it needs for the
// #315 required-policy carve-out.
//
// The transformer is built before the manager, so it cannot know them at
// construction; without the install the EAV required-policy check enforces
// policies on relation-root children, which #314 ruled must never gate a write.
func installRelationRoots(transformer model.PersistentRecordTransformer, idx *RelationIndex) {
	aware, ok := transformer.(transform.RelationRootsAware)
	if !ok {
		if idx != nil {
			// Loud, not silent: a transformer decorator that does not forward the
			// install leaves relation-root children under required-policy
			// enforcement, which rejects writes #314 ruled acceptable.
			zap.S().Warn("transformer does not accept relation roots; required-policy enforcement will not honour the #314 relation-root carve-out")
		}
		return
	}
	// Safe when idx is nil: RelationRoots is nil-receiver-safe
	// (relation_index.go) and installs an empty lookup.
	aware.SetRelationRoots(idx.RelationRoots)
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
