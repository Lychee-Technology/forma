package production

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma/internal/schemaledger/ledgertest"
	"github.com/lychee-technology/forma/internal/schemameta"
)

// EvolveSchema swaps the Env's live schema metadata to the generation
// authored in newSchemaDir, modelling a production restart under updated
// metadata (#189). It rebuilds the registry (the write/flush path — parquet
// column shapes derive from it at export time) and the metadata cache (the
// read/oracle path) from newSchemaDir, then drops the memoized engine and
// EntityManager so their next use re-binds the new caches; the plan cache is
// discarded with the engine, matching a cold restart. The indexes the new
// generation declares are built first, as init-db would before that restart
// (#647). The circuit breaker is deliberately kept (mirroring Engine's
// ReopenDuckDB contract). Already written parquet and manifest state is
// untouched — a later flush under the new schema therefore produces a
// differently-shaped parquet generation, which is exactly the
// cross-generation state schema-evolution tests need.
//
// The new declaration is recorded as the next generation first, as
// apply-schema would record it before the restart (#733); a change the D2
// rules refuse fails here.
func (e *Env) EvolveSchema(ctx context.Context, newSchemaDir string) error {
	return e.evolveSchema(ctx, newSchemaDir, ledgertest.Apply)
}

// ForceEvolveSchema is EvolveSchema without the D2 rules. It models a
// deployment whose declaration changed before the generation gate existed,
// which is how the Phase 1 guard's tests (#732) read an object written under
// numeric through a bigint declaration.
func (e *Env) ForceEvolveSchema(ctx context.Context, newSchemaDir string) error {
	return e.evolveSchema(ctx, newSchemaDir, ledgertest.Force)
}

type recordFn func(ctx context.Context, pool *pgxpool.Pool, registry, schemaDir string) error

func (e *Env) evolveSchema(ctx context.Context, newSchemaDir string, record recordFn) error {
	if err := record(ctx, e.Pool, e.Tables.SchemaRegistry, newSchemaDir); err != nil {
		return fmt.Errorf("record the declaration in %s: %w", newSchemaDir, err)
	}
	registry, err := schemameta.NewFileSchemaRegistryContext(ctx, e.Pool, e.Tables.SchemaRegistry, newSchemaDir)
	if err != nil {
		return fmt.Errorf("evolve schema registry from %s: %w", newSchemaDir, err)
	}
	metadata, err := schemameta.NewMetadataLoader(e.Pool, e.Tables.SchemaRegistry, newSchemaDir).LoadMetadata(ctx)
	if err != nil {
		return fmt.Errorf("evolve schema metadata from %s: %w", newSchemaDir, err)
	}
	// A restart under new metadata follows an init-db run over the new
	// directory, which builds whatever indexes the new generation declares.
	if err := applyDeclaredIndexes(ctx, e.Pool, e.Tables.EntityMain, registry); err != nil {
		return fmt.Errorf("evolve schema from %s: %w", newSchemaDir, err)
	}

	e.Registry = registry
	e.Metadata = metadata
	e.opts.schemaDir = newSchemaDir
	e.engine = nil
	e.manager = nil
	return nil
}
