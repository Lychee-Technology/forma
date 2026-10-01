package internal

import (
	"fmt"
	"strings"

	"github.com/lychee-technology/forma"
)

func applyProjection(records []*forma.DataRecord, attrs []string) {
	if len(attrs) == 0 {
		return
	}
	for _, rec := range records {
		rec.Attributes = FilterAttributes(rec.Attributes, attrs)
	}
}

func readStringAtPath(m map[string]any, path string) (string, bool) {
	val := getValueAtPath(m, path)
	if val == nil {
		return "", false
	}
	switch v := val.(type) {
	case string:
		return v, true
	default:
		return fmt.Sprintf("%v", v), true
	}
}

func getValueAtPath(m map[string]any, path string) any {
	if m == nil || path == "" {
		return m
	}
	segments := strings.Split(path, ".")
	current := any(m)
	for _, segment := range segments {
		asMap, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		next, exists := asMap[segment]
		if !exists {
			return nil
		}
		current = next
	}
	return current
}

func setNestedValue(m map[string]any, path string, value any) {
	if m == nil || path == "" {
		return
	}
	segments := strings.Split(path, ".")
	current := m
	for idx, segment := range segments {
		if idx == len(segments)-1 {
			current[segment] = value
			return
		}
		next, ok := current[segment].(map[string]any)
		if !ok {
			next = make(map[string]any)
			current[segment] = next
		}
		current = next
	}
}

// mergeMaps merges updates into existing data (deep merge)
func mergeMaps(existing map[string]any, updates any) map[string]any {
	result := copyMapDeep(existing)

	if updateMap, ok := updates.(map[string]any); ok {
		for key, value := range updateMap {
			if nestedExisting, existsInExisting := result[key]; existsInExisting {
				if existingMap, okExisting := nestedExisting.(map[string]any); okExisting {
					if updateNested, okUpdate := value.(map[string]any); okUpdate {
						result[key] = mergeMaps(existingMap, updateNested)
						continue
					}
				}
			}
			result[key] = value
		}
	}

	return result
}

// replacedByUpdate reports the attributes mergeMaps(existing, updates) takes
// from updates whatever existing holds, so an update's merge base need not
// convert their stored values (PersistentRecordTransformer.MergeBase): the
// update that repairs a value the read refuses must not be blocked by it
// (#590). See replacesPath for the walk.
func replacedByUpdate(updates any) func(attrName string) bool {
	updateMap, ok := updates.(map[string]any)
	if !ok {
		return func(string) bool { return false }
	}
	return func(attrName string) bool {
		return replacesPath(updateMap, strings.Split(attrName, "."))
	}
}

// replacesPath walks an attribute's dotted path through one level of an
// update the way mergeMaps recurses. An object value merges key by key, so
// the walk goes into it; any other value (a scalar, null, an array) replaces
// the whole subtree, list elements included, and so does an object at the
// path's end. A key may also spell several segments at once ("contact.email"
// for contact, email): mergeMaps keeps it literal and dedupeEAVRecords lets
// that spelling win (#312), so every joined prefix is tried. The walk never
// sees stored values, so where mergeMaps also replaces a stored non-object
// under an object update, it keeps the attribute: that costs a conversion,
// never a value the merge would keep.
func replacesPath(level map[string]any, segments []string) bool {
	for n := 1; n <= len(segments); n++ {
		value, present := level[strings.Join(segments[:n], ".")]
		if !present {
			continue
		}
		nested, isObject := value.(map[string]any)
		if !isObject || n == len(segments) || replacesPath(nested, segments[n:]) {
			return true
		}
	}
	return false
}

// copyMapDeep creates a deep copy of a map
func copyMapDeep(m map[string]any) map[string]any {
	result := make(map[string]any)
	for key, value := range m {
		result[key] = deepCopyValue(value)
	}
	return result
}

// deepCopyValue creates a deep copy of any value
func deepCopyValue(value any) any {
	switch v := value.(type) {
	case map[string]any:
		return copyMapDeep(v)
	case []any:
		result := make([]any, len(v))
		for i, item := range v {
			result[i] = deepCopyValue(item)
		}
		return result
	default:
		return value
	}
}
