package internal

import (
	"strings"

	"github.com/lychee-technology/forma"
)

// FilterAttributes filters a map of attributes based on the requested attribute paths.
// If attrs is nil or empty, returns the original attributes unchanged.
// Supports nested paths like "contact.name" or "contact.phone".
func FilterAttributes(attributes map[string]any, attrs []string) map[string]any {
	if len(attrs) == 0 {
		return attributes
	}

	result := make(map[string]any)

	for _, attrPath := range attrs {
		attrPath = strings.TrimSpace(attrPath)
		if attrPath == "" {
			continue
		}

		// Handle nested paths (e.g., "contact.name")
		segments := strings.Split(attrPath, ".")

		if len(segments) == 1 {
			// Simple attribute
			if val, ok := attributes[attrPath]; ok {
				result[attrPath] = val
			}
		} else {
			// Nested attribute - need to extract from nested structure
			extractNestedValue(attributes, segments, result)
		}
	}

	return result
}

// extractNestedValue extracts a nested value from source and sets it in the result
// maintaining the nested structure.
func extractNestedValue(source map[string]any, segments []string, result map[string]any) {
	if len(segments) == 0 {
		return
	}

	firstSegment := segments[0]
	sourceVal, exists := source[firstSegment]
	if !exists {
		return
	}

	if len(segments) == 1 {
		// Last segment - copy the value
		result[firstSegment] = sourceVal
		return
	}

	// More segments - need to go deeper
	sourceMap, ok := sourceVal.(map[string]any)
	if !ok {
		return
	}

	// Ensure the nested structure exists in result
	resultVal, exists := result[firstSegment]
	var resultMap map[string]any
	if exists {
		resultMap, ok = resultVal.(map[string]any)
		if !ok {
			resultMap = make(map[string]any)
		}
	} else {
		resultMap = make(map[string]any)
	}

	// Recursively extract the nested value
	extractNestedValue(sourceMap, segments[1:], resultMap)

	// Only set if we found something
	if len(resultMap) > 0 {
		result[firstSegment] = resultMap
	}
}

// FilterDataRecord applies attribute filtering to a forma.DataRecord.
// Returns a new DataRecord with filtered attributes.
func FilterDataRecord(record *forma.DataRecord, attrs []string) *forma.DataRecord {
	if record == nil {
		return nil
	}

	if len(attrs) == 0 {
		return record
	}

	return &forma.DataRecord{
		SchemaName: record.SchemaName,
		RowID:      record.RowID,
		Attributes: FilterAttributes(record.Attributes, attrs),
	}
}
