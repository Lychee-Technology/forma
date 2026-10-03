package transform

import (
	"fmt"
	"strconv"
	"strings"
)

// joinIndices converts a slice of indices to a comma-separated string
func joinIndices(indices []int) string {
	if len(indices) == 0 {
		return ""
	}
	parts := make([]string, len(indices))
	for i, idx := range indices {
		parts[i] = strconv.Itoa(idx)
	}
	return strings.Join(parts, ",")
}

// parseIndices converts a comma-separated string to a slice of indices
func parseIndices(indices string) ([]int, error) {
	if indices == "" {
		return nil, nil
	}
	parts := strings.Split(indices, ",")
	result := make([]int, len(parts))
	for i, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("empty index")
		}
		value, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("invalid index '%s'", part)
		}
		result[i] = value
	}
	return result, nil
}

// ensureArray retrieves or creates an array at the given key
func ensureArray(target map[string]any, key string) []any {
	existing := target[key]
	if existing == nil {
		return []any{}
	}
	if arr, ok := existing.([]any); ok {
		return arr
	}
	return []any{}
}

// setObjectArrayValue sets a field value within an object at a specific array index
func setObjectArrayValue(arr []any, indices []int, fieldName string, value any) []any {
	if len(indices) == 0 {
		return arr
	}

	idx := indices[0]
	if idx < 0 {
		return arr
	}

	// Expand array if needed
	arr = expandArray(arr, idx)

	if len(indices) == 1 {
		// Last index - set the field in the object at this index
		obj := getOrCreateObject(arr, idx)
		obj[fieldName] = value
		arr[idx] = obj
	} else {
		// More indices - need nested array within the object
		obj := getOrCreateObject(arr, idx)

		// Get or create nested array
		var nestedArr []any
		if existingNested := obj[fieldName]; existingNested != nil {
			if nested, ok := existingNested.([]any); ok {
				nestedArr = nested
			}
		}
		if nestedArr == nil {
			nestedArr = []any{}
		}

		// Recursively set in nested array
		nestedArr = setObjectArrayValue(nestedArr, indices[1:], fieldName, value)
		obj[fieldName] = nestedArr
		arr[idx] = obj
	}

	return arr
}

// setArrayValueRecursive recursively sets a value in a nested array structure
func setArrayValueRecursive(arr []any, indices []int, value any) []any {
	if len(indices) == 0 {
		return arr
	}

	idx := indices[0]
	if idx < 0 {
		return arr
	}

	// Expand array if needed
	arr = expandArray(arr, idx)

	if len(indices) == 1 {
		// Last index - set the value directly
		arr[idx] = value
	} else {
		// More indices - need nested array
		var nestedArr []any
		if arr[idx] != nil {
			if existing, ok := arr[idx].([]any); ok {
				nestedArr = existing
			}
		}
		if nestedArr == nil {
			nestedArr = []any{}
		}
		arr[idx] = setArrayValueRecursive(nestedArr, indices[1:], value)
	}

	return arr
}

// expandArray ensures the array has at least idx+1 elements
func expandArray(arr []any, idx int) []any {
	for len(arr) <= idx {
		arr = append(arr, nil)
	}
	return arr
}

// getOrCreateObject gets or creates an object at the given array index
func getOrCreateObject(arr []any, idx int) map[string]any {
	if arr[idx] != nil {
		if existing, ok := arr[idx].(map[string]any); ok {
			return existing
		}
	}
	return make(map[string]any)
}
