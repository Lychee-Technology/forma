package internal

import (
	"fmt"
	"math"

	"github.com/lychee-technology/forma"
)

func parseAttributeID(raw any, attrName, source string) (int16, error) {
	id, ok := raw.(float64)
	if !ok {
		return 0, fmt.Errorf("invalid or missing attributeID for attribute %s in %s", attrName, source)
	}
	if math.Trunc(id) != id {
		return 0, fmt.Errorf("attributeID for attribute %s in %s must be an integer", attrName, source)
	}
	if id < 0 || id > math.MaxInt16 {
		return 0, fmt.Errorf("attributeID for attribute %s in %s is out of range for int16", attrName, source)
	}
	return int16(id), nil
}

func validateSchemaAttributeCache(schemaName string, cache forma.SchemaAttributeCache) error {
	seenAttrIDs := make(map[int16]string, len(cache))
	seenBindings := make(map[forma.MainColumn]string)
	for attrName, meta := range cache {
		if existingAttr, exists := seenAttrIDs[meta.AttributeID]; exists {
			return fmt.Errorf("schema %s has duplicate attribute id %d for %s and %s", schemaName, meta.AttributeID, existingAttr, attrName)
		}
		seenAttrIDs[meta.AttributeID] = attrName

		if meta.ColumnBinding == nil {
			continue
		}
		if existingAttr, exists := seenBindings[meta.ColumnBinding.ColumnName]; exists {
			return fmt.Errorf("schema %s has duplicate column binding %s for %s and %s", schemaName, meta.ColumnBinding.ColumnName, existingAttr, attrName)
		}
		seenBindings[meta.ColumnBinding.ColumnName] = attrName
	}
	return nil
}
