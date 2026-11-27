package schemameta

import (
	"fmt"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// GuardReservedAttrNames answers a registry whose attribute caches never hold
// an active attribute named after a reserved column: an entity_main column
// (#470), or a system or dedup column (row_id, rn, ltbase_created_at, ...;
// #260, #552). That is the combined reserved set admission keeps out of the
// attribute namespace (sqlgen.ValidateReservedAttrNames), the one the
// EntityManager is built over.
//
// Admission refuses such names for the registries this package builds
// (fileSchemaRegistry and the generation gate's trackedRegistry): each cache
// went through validateSchemaAttributeCacheOpts when it loaded, and neither
// type reloads, so they are answered unchanged. Their type is unexported, so
// a caller's own registry cannot pass for one, and returning them as they are
// keeps the schemaMetadataProvider fast path the transformer reads through.
//
// Any other registry is the caller's forma.SchemaRegistry, whose caches no
// admission saw. Every schema it lists is checked once here, so a colliding
// one fails startup, and the registry answered checks every cache it serves
// again on every read. That second check is what covers a registry that
// reloads: forma.SchemaRegistry promises nothing about repeated reads, and a
// cache served after startup is refused before any condition, sort or
// transform sees it. The wrapper delegates every other method, and hides any
// optional interface the caller's registry implements; the EntityManager
// reads none.
//
// The guard enforces the reserved-name rule only. The other admission rules
// (attributeID, binding and fold collisions, binding validity) are not new in
// #470 and are not applied to a caller's registry here.
func GuardReservedAttrNames(registry forma.SchemaRegistry) (forma.SchemaRegistry, error) {
	switch registry.(type) {
	case nil:
		return nil, fmt.Errorf("schema registry is not configured")
	case *fileSchemaRegistry, *trackedRegistry:
		return registry, nil
	}
	guarded := &reservedNameGuardedRegistry{SchemaRegistry: registry}
	for _, name := range registry.ListSchemas() {
		if _, _, err := guarded.GetSchemaAttributeCacheByName(name); err != nil {
			return nil, fmt.Errorf("failed to check the attributes of schema %s: %w", name, err)
		}
	}
	return guarded, nil
}

// reservedNameGuardedRegistry refuses, on every read, an attribute cache that
// holds an active attribute named after a reserved column. The refusal is
// a plain operator error, never forma.ErrInvalidInput: the registry is
// misconfigured, and no request can fix it.
type reservedNameGuardedRegistry struct {
	forma.SchemaRegistry
}

// GetSchemaAttributeCacheByName answers the wrapped registry's cache once it
// holds no colliding name. The registry's own error is returned unwrapped, so
// a not-found carrier keeps its status and its published message.
func (r *reservedNameGuardedRegistry) GetSchemaAttributeCacheByName(name string) (int16, forma.SchemaAttributeCache, error) {
	id, cache, err := r.SchemaRegistry.GetSchemaAttributeCacheByName(name)
	if err != nil {
		return 0, nil, err
	}
	if err := sqlgen.ValidateReservedAttrNames(cache); err != nil {
		return 0, nil, fmt.Errorf("schema %s served by the configured SchemaRegistry: %w", name, err)
	}
	return id, cache, nil
}

// GetSchemaAttributeCacheByID is the id-keyed half of the same check.
func (r *reservedNameGuardedRegistry) GetSchemaAttributeCacheByID(id int16) (string, forma.SchemaAttributeCache, error) {
	name, cache, err := r.SchemaRegistry.GetSchemaAttributeCacheByID(id)
	if err != nil {
		return "", nil, err
	}
	if err := sqlgen.ValidateReservedAttrNames(cache); err != nil {
		return "", nil, fmt.Errorf("schema %s (id %d) served by the configured SchemaRegistry: %w", name, id, err)
	}
	return name, cache, nil
}
