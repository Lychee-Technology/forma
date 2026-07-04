package internal

import (
	"fmt"

	"github.com/lychee-technology/forma"
)

// schemaMetadataProvider is implemented by registries that build both schema
// metadata representations at load time. Returned maps are registry-owned and
// callers must treat them as read-only.
type schemaMetadataProvider interface {
	schemaMetadataForID(schemaID int16) (forma.SchemaAttributeCache, map[int16]string, error)
}

// getSchemaMetadata returns schema metadata and an attribute-id lookup table.
// The fast path returns registry-owned maps; callers must not mutate them.
func getSchemaMetadata(registry forma.SchemaRegistry, schemaID int16) (forma.SchemaAttributeCache, map[int16]string, error) {
	if registry == nil {
		return nil, nil, fmt.Errorf("schema registry is not configured")
	}
	if provider, ok := registry.(schemaMetadataProvider); ok {
		return provider.schemaMetadataForID(schemaID)
	}

	_, schemaCache, err := registry.GetSchemaAttributeCacheByID(schemaID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load schema metadata for id %d: %w", schemaID, err)
	}

	idToName := make(map[int16]string, len(schemaCache))
	for name, meta := range schemaCache {
		if existingName, exists := idToName[meta.AttributeID]; exists {
			return nil, nil, fmt.Errorf("duplicate attribute id %d for %s and %s", meta.AttributeID, existingName, name)
		}
		idToName[meta.AttributeID] = name
	}

	return schemaCache, idToName, nil
}
