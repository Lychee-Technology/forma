package schemameta

import (
	"fmt"
	"math"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
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

// validateSchemaAttributeCache rejects attributeID, main-column, and folded
// parquet-column collisions across the FULL cache, retired entries included.
// Every error it returns begins with "schema <name>", so callers wrap it with
// what it cannot know — the attributes file the metadata came from, or the
// numeric schema id — never with the name again.
func validateSchemaAttributeCache(schemaName string, cache forma.SchemaAttributeCache) error {
	seenAttrIDs := make(map[int16]string, len(cache))
	seenBindings := make(map[forma.MainColumn]string)
	for attrName, meta := range cache {
		if existingAttr, exists := seenAttrIDs[meta.AttributeID]; exists {
			return attrIDCollisionError(schemaName, cache, meta.AttributeID, existingAttr, attrName)
		}
		seenAttrIDs[meta.AttributeID] = attrName

		if meta.ColumnBinding == nil {
			continue
		}
		if existingAttr, exists := seenBindings[meta.ColumnBinding.ColumnName]; exists {
			return bindingCollisionError(schemaName, cache, meta.ColumnBinding.ColumnName, existingAttr, attrName)
		}
		seenBindings[meta.ColumnBinding.ColumnName] = attrName
	}

	// Reject attribute names whose folded parquet columns land on a CDC/read
	// system column or collide with each other — such a schema would accept
	// hot-tier writes it can never flush or read back federated (#260,
	// PR #273 review). Retired entries stay in scope: their folded columns
	// still exist in flushed parquet files (#342).
	if err := sqlgen.ValidateParquetAttrColumns(cache); err != nil {
		return fmt.Errorf("schema %s: %w", schemaName, err)
	}
	return nil
}

// activeAttributeCache returns cache without retired entries. Retired entries
// exist only as the attributeID ledger (#342): they take part in validation
// above, but no consumer may see them — a retired attribute reads, writes,
// flushes, and projects exactly as if its entry were absent (#294 skip).
// Callers must invoke this strictly after validateSchemaAttributeCache, so the
// guard always sees the full ledger.
func activeAttributeCache(cache forma.SchemaAttributeCache) forma.SchemaAttributeCache {
	active := make(forma.SchemaAttributeCache, len(cache))
	for name, meta := range cache {
		if meta.Retired {
			continue
		}
		active[name] = meta
	}
	return active
}

// retiredAttributeIDs is activeAttributeCache's counterpart: it keeps the books
// the strip discards. The result is the attributeID ledger of removed
// attributes (#342) — id → name — which validation tooling needs to tell an EAV
// row #294 preserved from a genuinely orphaned one (#341). Like its
// counterpart, callers must invoke it strictly after
// validateSchemaAttributeCache, which is what makes the id key unambiguous:
// that guard has already rejected any duplicate attributeID in the file.
func retiredAttributeIDs(cache forma.SchemaAttributeCache) map[int16]string {
	var retired map[int16]string
	for name, meta := range cache {
		if !meta.Retired {
			continue
		}
		if retired == nil {
			retired = make(map[int16]string)
		}
		retired[meta.AttributeID] = name
	}
	return retired
}

// attrIDCollisionError names the retired side of an attributeID collision.
// AttributeIDs are physical EAV keys: an id freed by attribute removal is
// still bound to preserved rows, so rebinding it to a different attribute
// would surface old values under the new name or make rows unreadable (#342).
func attrIDCollisionError(schemaName string, cache forma.SchemaAttributeCache, id int16, a, b string) error {
	if retiredName, activeName, ok := splitRetiredPair(cache, a, b); ok {
		return fmt.Errorf(
			"schema %s reuses attribute id %d: retired attribute %s (valueType %s) still owns preserved EAV rows and cannot be rebound to %s; re-add the original name and valueType to restore it, or assign a new id",
			schemaName, id, retiredName, cache[retiredName].ValueType, activeName)
	}
	return fmt.Errorf("schema %s has duplicate attribute id %d for %s and %s", schemaName, id, a, b)
}

// bindingCollisionError is the main-table analogue: a retired attribute's hot
// column still holds its historical values.
func bindingCollisionError(schemaName string, cache forma.SchemaAttributeCache, col forma.MainColumn, a, b string) error {
	if retiredName, activeName, ok := splitRetiredPair(cache, a, b); ok {
		return fmt.Errorf(
			"schema %s reuses main column %s: retired attribute %s (valueType %s) still owns its stored values and cannot share the column with %s; keep the binding retired or assign a new column",
			schemaName, col, retiredName, cache[retiredName].ValueType, activeName)
	}
	return fmt.Errorf("schema %s has duplicate column binding %s for %s and %s", schemaName, col, a, b)
}

// splitRetiredPair returns (retired, active, true) when exactly one of the two
// colliding attributes is retired — the reuse case #342 guards against.
func splitRetiredPair(cache forma.SchemaAttributeCache, a, b string) (string, string, bool) {
	aRetired, bRetired := cache[a].Retired, cache[b].Retired
	if aRetired == bRetired {
		return "", "", false
	}
	if aRetired {
		return a, b, true
	}
	return b, a, true
}
