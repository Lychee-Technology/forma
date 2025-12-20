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
