package production

import (
	"context"
	"fmt"

	"github.com/lychee-technology/forma/internal/schemameta"
)

// EvolveSchema swaps the Env's live schema metadata to the generation
// authored in newSchemaDir, modelling a production restart under updated
// metadata (#189). It rebuilds the registry (the write/flush path — parquet
// column shapes derive from it at export time) and the metadata cache (the
// read/oracle path) from newSchemaDir, then drops the memoized engine and
// EntityManager so their next use re-binds the new caches; the plan cache is
// discarded with the engine, matching a cold restart. The circuit breaker is
// deliberately kept (mirroring Engine's ReopenDuckDB contract). Already
// written parquet and manifest state is untouched — a later flush under the
// new schema therefore produces a differently-shaped parquet generation,
// which is exactly the cross-generation state schema-evolution tests need.
func (e *Env) EvolveSchema(ctx context.Context, newSchemaDir string) error {
	registry, err := schemameta.NewFileSchemaRegistryContext(ctx, e.Pool, e.Tables.SchemaRegistry, newSchemaDir)
	if err != nil {
		return fmt.Errorf("evolve schema registry from %s: %w", newSchemaDir, err)
	}
	metadata, err := schemameta.NewMetadataLoader(e.Pool, e.Tables.SchemaRegistry, newSchemaDir).LoadMetadata(ctx)
	if err != nil {
		return fmt.Errorf("evolve schema metadata from %s: %w", newSchemaDir, err)
	}

	e.Registry = registry
	e.Metadata = metadata
	e.opts.schemaDir = newSchemaDir
	e.engine = nil
	e.manager = nil
	return nil
}
