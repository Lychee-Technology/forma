package schemameta

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemaledger"
)

// trackedRegistry is a registry the generation gate admitted (#733). It
// knows the generation each schema started on, so cdc-flush and cdc-init
// can tell when apply-schema records a new one mid-run
// (schemaledger.RequireUnchanged).
type trackedRegistry struct {
	*fileSchemaRegistry
	generations map[int16]int64
}

// gateRegistry admits a loaded registry only when every schema it declares
// is the schema's active generation: a file edit or a restart cannot
// activate a declaration apply-schema did not record.
func gateRegistry(ctx context.Context, r *fileSchemaRegistry, decls []schemaledger.Declaration) (forma.SchemaRegistry, error) {
	loaded, err := schemaledger.Gate(ctx, r.pool, r.schemaTable, decls)
	if err != nil {
		return nil, fmt.Errorf("schema directory %s: %w", r.schemaDir, err)
	}
	return &trackedRegistry{fileSchemaRegistry: r, generations: loaded}, nil
}

// LoadedGeneration implements schemaledger.Tracker.
func (r *trackedRegistry) LoadedGeneration(schemaID int16) (int64, bool) {
	n, ok := r.generations[schemaID]
	return n, ok
}

// ActiveGeneration implements schemaledger.Tracker.
func (r *trackedRegistry) ActiveGeneration(ctx context.Context, schemaID int16) (int64, bool, error) {
	n, ok, err := schemaledger.ReadPointer(ctx, r.pool, r.schemaTable, schemaID)
	if err != nil {
		return 0, false, fmt.Errorf("read the active generation of schema id %d: %w", schemaID, err)
	}
	return n, ok, nil
}

// NewControlPlaneSchemaRegistryContext is NewFileSchemaRegistryContext
// without the generation gate, for the control-plane tools that load a
// declaration the ledger does not hold yet, or report on one: init-db's
// declared-index and unique-key steps, and validate-schema-consistency. A
// guard test confines its callers; the data plane never uses it.
func NewControlPlaneSchemaRegistryContext(ctx context.Context, pool *pgxpool.Pool, schemaTable, schemaDir string) (forma.SchemaRegistry, error) {
	registry := newFileSchemaRegistry(pool, schemaTable, schemaDir)
	if _, err := registry.loadSchemasFromDB(ctx); err != nil {
		return nil, err
	}
	return registry, nil
}
