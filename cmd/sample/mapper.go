package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// FieldMapper defines the interface for transforming a CSV field value to a Schema field value.
type FieldMapper interface {
	// Map transforms a CSV string value to the target type.
	Map(csvValue string) (any, error)
}

// FieldMapping describes a single field mapping from CSV column to Schema path.
type FieldMapping struct {
	CSVColumn  string      // CSV column name
	SchemaPath string      // Schema field path (e.g., "contact.name", "propertyInterests[0].propertyId")
	Mapper     FieldMapper // Transformer for the field
	Required   bool        // Whether this field is required
}

// CSVToSchemaMapper defines the interface for mapping CSV records to Schema attributes.
type CSVToSchemaMapper interface {
	// SchemaName returns the target schema name.
	SchemaName() string

	// Mappings returns all field mappings.
	Mappings() []FieldMapping

	// MapRecord transforms a CSV record (column->value) to Schema attributes.
	// Returns a nested map structure based on the schema paths.
	MapRecord(csvRecord map[string]string) (map[string]any, error)
}

// MapperBuilder provides a fluent API for building CSV to Schema mappers.
type MapperBuilder struct {
	schemaName string
	mappings   []FieldMapping
}

// NewMapperBuilder creates a new MapperBuilder for the specified schema.
func NewMapperBuilder(schemaName string) *MapperBuilder {
	return &MapperBuilder{
		schemaName: schemaName,
		mappings:   make([]FieldMapping, 0),
	}
}

// Map adds a simple string mapping (identity transform).
func (b *MapperBuilder) Map(csvColumn, schemaPath string) *MapperBuilder {
	b.mappings = append(b.mappings, FieldMapping{
		CSVColumn:  csvColumn,
		SchemaPath: schemaPath,
		Mapper:     &identityMapper{},
		Required:   false,
	})
	return b
}

// MapWith adds a mapping with a custom transformer.
func (b *MapperBuilder) MapWith(csvColumn, schemaPath string, mapper FieldMapper) *MapperBuilder {
	b.mappings = append(b.mappings, FieldMapping{
		CSVColumn:  csvColumn,
		SchemaPath: schemaPath,
		Mapper:     mapper,
		Required:   false,
	})
	return b
}

// Required adds a required field mapping (identity transform).
func (b *MapperBuilder) Required(csvColumn, schemaPath string) *MapperBuilder {
	b.mappings = append(b.mappings, FieldMapping{
		CSVColumn:  csvColumn,
		SchemaPath: schemaPath,
		Mapper:     &identityMapper{},
		Required:   true,
	})
	return b
}

// RequiredWith adds a required field mapping with a custom transformer.
func (b *MapperBuilder) RequiredWith(csvColumn, schemaPath string, mapper FieldMapper) *MapperBuilder {
	b.mappings = append(b.mappings, FieldMapping{
		CSVColumn:  csvColumn,
		SchemaPath: schemaPath,
		Mapper:     mapper,
		Required:   true,
	})
	return b
}

// Build creates the CSVToSchemaMapper from the builder configuration.
func (b *MapperBuilder) Build() CSVToSchemaMapper {
	return &schemaMapper{
		schemaName: b.schemaName,
		mappings:   b.mappings,
	}
}

// schemaMapper implements CSVToSchemaMapper.
type schemaMapper struct {
	schemaName string
	mappings   []FieldMapping
}

func (m *schemaMapper) SchemaName() string {
	return m.schemaName
}

func (m *schemaMapper) Mappings() []FieldMapping {
	return m.mappings
}

func (m *schemaMapper) MapRecord(csvRecord map[string]string) (map[string]any, error) {
	result := make(map[string]any)
	result["createdAt"] = time.Now()
	for _, mapping := range m.mappings {
		csvValue, exists := csvRecord[mapping.CSVColumn]

		// Check required fields
		if mapping.Required {
			if !exists || strings.TrimSpace(csvValue) == "" {
				return nil, &MappingError{
					CSVColumn:  mapping.CSVColumn,
					SchemaPath: mapping.SchemaPath,
					RawValue:   csvValue,
					Reason:     "required field is empty",
				}
			}
		}

		// Skip empty optional fields
		if !exists || strings.TrimSpace(csvValue) == "" {
			continue
		}

		// Transform the value
		transformedValue, err := mapping.Mapper.Map(csvValue)
		if err != nil {
			return nil, &MappingError{
				CSVColumn:  mapping.CSVColumn,
				SchemaPath: mapping.SchemaPath,
				RawValue:   csvValue,
				Reason:     err.Error(),
			}
		}

		// Set the value at the schema path
		if err := setNestedValue(result, mapping.SchemaPath, transformedValue); err != nil {
			return nil, &MappingError{
				CSVColumn:  mapping.CSVColumn,
				SchemaPath: mapping.SchemaPath,
				RawValue:   csvValue,
				Reason:     fmt.Sprintf("failed to set nested value: %v", err),
			}
		}
	}

	return result, nil
}

// MappingError represents an error that occurred during field mapping.
type MappingError struct {
	CSVColumn  string
	SchemaPath string
	RawValue   string
	Reason     string
}

func (e *MappingError) Error() string {
	return fmt.Sprintf("column %q -> path %q: value %q - %s",
		e.CSVColumn, e.SchemaPath, e.RawValue, e.Reason)
}

// pathSegment represents a segment of a schema path.
type pathSegment struct {
	Key   string
	Index int  // -1 if not an array index
	IsArr bool // true if this segment is an array access
}

// arrayIndexPattern matches array notation like [0], [1], etc.
var arrayIndexPattern = regexp.MustCompile(`^([^\[]+)\[(\d+)\]$`)

// parseSchemaPath parses a schema path like "contact.name" or "propertyInterests[0].propertyId"
// into path segments.
func parseSchemaPath(path string) []pathSegment {
	parts := strings.Split(path, ".")
	segments := make([]pathSegment, 0, len(parts))

	for _, part := range parts {
		if matches := arrayIndexPattern.FindStringSubmatch(part); matches != nil {
			// This is an array access like "propertyInterests[0]"
			key := matches[1]
			index, _ := strconv.Atoi(matches[2])
			segments = append(segments, pathSegment{
				Key:   key,
				Index: index,
				IsArr: true,
			})
		} else {
			// Regular key
			segments = append(segments, pathSegment{
				Key:   part,
				Index: -1,
				IsArr: false,
			})
		}
	}

	return segments
}

// setNestedValue sets a value at a nested path in the map.
// Supports paths like "contact.name", "propertyInterests[0].propertyId".
func setNestedValue(data map[string]any, path string, value any) error {
	segments := parseSchemaPath(path)
	if len(segments) == 0 {
		return fmt.Errorf("empty path")
	}

	current := data

	for i, seg := range segments {
		isLast := i == len(segments)-1

		if seg.IsArr {
			// Handle array access
			arr, exists := current[seg.Key]
			if !exists {
				// Create array if it doesn't exist
				arr = make([]any, seg.Index+1)
				current[seg.Key] = arr
			}

			arrSlice, ok := arr.([]any)
			if !ok {
				return fmt.Errorf("path segment %q is not an array", seg.Key)
			}

			// Expand array if needed
			for len(arrSlice) <= seg.Index {
				arrSlice = append(arrSlice, nil)
			}
			current[seg.Key] = arrSlice

			if isLast {
				arrSlice[seg.Index] = value
			} else {
				// Get or create nested map at array index
				if arrSlice[seg.Index] == nil {
					arrSlice[seg.Index] = make(map[string]any)
				}
				nestedMap, ok := arrSlice[seg.Index].(map[string]any)
				if !ok {
					return fmt.Errorf("array element at %q[%d] is not a map", seg.Key, seg.Index)
				}
				current = nestedMap
			}
		} else {
			// Handle regular key
			if isLast {
				current[seg.Key] = value
			} else {
				nested, exists := current[seg.Key]
				if !exists {
					nested = make(map[string]any)
					current[seg.Key] = nested
				}
				nestedMap, ok := nested.(map[string]any)
				if !ok {
					return fmt.Errorf("path segment %q is not a map", seg.Key)
				}
				current = nestedMap
			}
		}
	}

	return nil
}
