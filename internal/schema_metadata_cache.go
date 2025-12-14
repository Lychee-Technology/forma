package internal

import (
	"fmt"
	"sync"

	"github.com/lychee-technology/forma"
)

// schemaMetadataCache centralizes schema metadata lookup and caching so
// multiple components can share the same implementation.
type schemaMetadataCache struct {
	registry    forma.SchemaRegistry
	cacheMu     sync.RWMutex
	attrCache   map[int16]forma.SchemaAttributeCache
	idNameCache map[int16]map[int16]string
}

func newSchemaMetadataCache(registry forma.SchemaRegistry) *schemaMetadataCache {
	return &schemaMetadataCache{
		registry:    registry,
		attrCache:   make(map[int16]forma.SchemaAttributeCache),
		idNameCache: make(map[int16]map[int16]string),
	}
}

func (c *schemaMetadataCache) getSchemaMetadata(schemaID int16) (forma.SchemaAttributeCache, map[int16]string, error) {
	c.cacheMu.RLock()
	cache, okCache := c.attrCache[schemaID]
	idMap, okMap := c.idNameCache[schemaID]
	c.cacheMu.RUnlock()

	if okCache && okMap {
		return cache, idMap, nil
	}

	if c.registry == nil {
		return nil, nil, fmt.Errorf("schema registry is not configured")
	}

	_, schemaCache, err := c.registry.GetSchemaAttributeCacheByID(schemaID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load schema metadata for id %d: %w", schemaID, err)
	}

	idToName := make(map[int16]string, len(schemaCache))
	for name, meta := range schemaCache {
		idToName[meta.AttributeID] = name
	}

	c.cacheMu.Lock()
	c.attrCache[schemaID] = schemaCache
	c.idNameCache[schemaID] = idToName
	c.cacheMu.Unlock()

	return schemaCache, idToName, nil
}
