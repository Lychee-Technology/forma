package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func runInlineSchema(args []string) error {
	flags := flag.NewFlagSet("inline-schema", flag.ContinueOnError)
	flags.SetOutput(os.Stdout)
	flags.Usage = func() {
		fmt.Println("Usage: forma-tools inline-schema [options]")
		fmt.Println("")
		fmt.Println("Options:")
		flags.PrintDefaults()
	}

	schemaFile := flags.String("schema-file", "", "Path to the JSON schema file (required)")
	outputFile := flags.String("out", "", "Path to write the inlined schema (defaults to stdout)")

	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	if *schemaFile == "" {
		return fmt.Errorf("-schema-file is required")
	}

	inliner := NewSchemaInliner(filepath.Dir(*schemaFile))
	result, err := inliner.InlineFile(*schemaFile)
	if err != nil {
		return fmt.Errorf("inline schema: %w", err)
	}

	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal result: %w", err)
	}

	if *outputFile == "" {
		fmt.Println(string(encoded))
	} else {
		if err := os.MkdirAll(filepath.Dir(*outputFile), 0o755); err != nil {
			return fmt.Errorf("create output directory: %w", err)
		}
		if err := os.WriteFile(*outputFile, encoded, 0o644); err != nil {
			return fmt.Errorf("write output file: %w", err)
		}
		fmt.Printf("Inlined schema written, output: %s\n", *outputFile)
	}

	return nil
}

// SchemaInliner handles inlining of $ref references in JSON schemas
type SchemaInliner struct {
	baseDir   string
	cache     map[string]map[string]any // cache for loaded schema files
	resolving map[string]bool           // tracks refs currently being resolved (cycle detection)
}

// NewSchemaInliner creates a new SchemaInliner with the given base directory
func NewSchemaInliner(baseDir string) *SchemaInliner {
	return &SchemaInliner{
		baseDir:   baseDir,
		cache:     make(map[string]map[string]any),
		resolving: make(map[string]bool),
	}
}

// InlineFile loads a schema file and returns the fully inlined version
func (s *SchemaInliner) InlineFile(filePath string) (map[string]any, error) {
	schema, err := s.loadSchemaFile(filePath)
	if err != nil {
		return nil, err
	}

	result, err := s.inlineNode(schema, filePath)
	if err != nil {
		return nil, err
	}

	resultMap, ok := result.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected object at root level")
	}

	// Remove $defs from the result (they are now inlined)
	delete(resultMap, "$defs")
	delete(resultMap, "definitions") // also remove legacy "definitions"

	return resultMap, nil
}

// loadSchemaFile loads and caches a schema file
func (s *SchemaInliner) loadSchemaFile(filePath string) (map[string]any, error) {
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		return nil, fmt.Errorf("resolve path %s: %w", filePath, err)
	}

	if cached, ok := s.cache[absPath]; ok {
		return cached, nil
	}

	data, err := os.ReadFile(absPath)
	if err != nil {
		return nil, fmt.Errorf("read file %s: %w", absPath, err)
	}

	var schema map[string]any
	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, fmt.Errorf("parse JSON %s: %w", absPath, err)
	}

	s.cache[absPath] = schema
	return schema, nil
}

// inlineNode recursively processes a node, inlining $ref and filtering x-* properties
func (s *SchemaInliner) inlineNode(node any, currentFile string) (any, error) {
	switch v := node.(type) {
	case map[string]any:
		return s.inlineObject(v, currentFile)
	case []any:
		return s.inlineArray(v, currentFile)
	default:
		return node, nil
	}
}

// inlineObject processes an object node
func (s *SchemaInliner) inlineObject(obj map[string]any, currentFile string) (map[string]any, error) {
	// Check if this object has a $ref
	if ref, ok := obj["$ref"].(string); ok {
		resolved, err := s.resolveRef(ref, currentFile)
		if err != nil {
			return nil, err
		}

		// Merge any additional properties from the original object (except $ref)
		// This handles cases like { "$ref": "...", "description": "override" }
		result := make(map[string]any)
		for k, v := range resolved {
			result[k] = v
		}
		for k, v := range obj {
			if k != "$ref" && !strings.HasPrefix(k, "x-") {
				result[k] = v
			}
		}

		// Recursively inline the merged result
		return s.inlineObjectProperties(result, currentFile)
	}

	return s.inlineObjectProperties(obj, currentFile)
}

// inlineObjectProperties processes all properties of an object
func (s *SchemaInliner) inlineObjectProperties(obj map[string]any, currentFile string) (map[string]any, error) {
	result := make(map[string]any)

	for key, value := range obj {
		// Filter out x-* extension properties
		if strings.HasPrefix(key, "x-") {
			continue
		}

		// Skip $defs and definitions at any level (they will be inlined where referenced)
		if key == "$defs" || key == "definitions" {
			continue
		}

		inlined, err := s.inlineNode(value, currentFile)
		if err != nil {
			return nil, fmt.Errorf("inline property %q: %w", key, err)
		}
		result[key] = inlined
	}

	return result, nil
}

// inlineArray processes an array node
func (s *SchemaInliner) inlineArray(arr []any, currentFile string) ([]any, error) {
	result := make([]any, len(arr))
	for i, item := range arr {
		inlined, err := s.inlineNode(item, currentFile)
		if err != nil {
			return nil, fmt.Errorf("inline array item %d: %w", i, err)
		}
		result[i] = inlined
	}
	return result, nil
}

// resolveRef resolves a $ref reference and returns the inlined content
func (s *SchemaInliner) resolveRef(ref string, currentFile string) (map[string]any, error) {
	// Create a unique key for cycle detection
	absCurrentFile, _ := filepath.Abs(currentFile)
	cycleKey := absCurrentFile + "|" + ref

	if s.resolving[cycleKey] {
		return nil, fmt.Errorf("circular reference detected: %s in %s", ref, currentFile)
	}
	s.resolving[cycleKey] = true
	defer func() { delete(s.resolving, cycleKey) }()

	// Parse the reference
	filePath, jsonPointer := parseRef(ref)

	// Determine the target file
	var targetFile string
	if filePath == "" {
		targetFile = currentFile
	} else {
		if filepath.IsAbs(filePath) {
			targetFile = filePath
		} else {
			targetFile = filepath.Join(filepath.Dir(currentFile), filePath)
		}
	}

	// Load the target schema
	schema, err := s.loadSchemaFile(targetFile)
	if err != nil {
		return nil, fmt.Errorf("load ref target %s: %w", ref, err)
	}

	// If no JSON pointer, return the whole schema (after inlining)
	if jsonPointer == "" {
		return s.inlineObjectProperties(schema, targetFile)
	}

	// Resolve the JSON pointer
	target, err := resolveJSONPointer(schema, jsonPointer)
	if err != nil {
		return nil, fmt.Errorf("resolve pointer %s in %s: %w", jsonPointer, targetFile, err)
	}

	targetObj, ok := target.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("ref target is not an object: %s", ref)
	}

	// Recursively inline the target
	return s.inlineObjectProperties(targetObj, targetFile)
}

// parseRef parses a $ref value into file path and JSON pointer components
// Examples:
//   - "#/$defs/id" -> "", "/$defs/id"
//   - "./common.json" -> "./common.json", ""
//   - "./common.json#/$defs/address" -> "./common.json", "/$defs/address"
func parseRef(ref string) (filePath string, jsonPointer string) {
	if idx := strings.Index(ref, "#"); idx != -1 {
		filePath = ref[:idx]
		jsonPointer = ref[idx+1:]
	} else {
		filePath = ref
	}
	return
}

// resolveJSONPointer resolves a JSON pointer (RFC 6901) against a document
func resolveJSONPointer(doc any, pointer string) (any, error) {
	if pointer == "" || pointer == "/" {
		return doc, nil
	}

	// Remove leading slash
	pointer = strings.TrimPrefix(pointer, "/")

	parts := strings.Split(pointer, "/")
	current := doc

	for _, part := range parts {
		// Unescape JSON pointer encoding
		part = strings.ReplaceAll(part, "~1", "/")
		part = strings.ReplaceAll(part, "~0", "~")

		switch v := current.(type) {
		case map[string]any:
			var ok bool
			current, ok = v[part]
			if !ok {
				return nil, fmt.Errorf("key not found: %s", part)
			}
		case []any:
			idx, err := strconv.Atoi(part)
			if err != nil {
				return nil, fmt.Errorf("invalid array index: %s", part)
			}
			if idx < 0 || idx >= len(v) {
				return nil, fmt.Errorf("array index out of bounds: %d", idx)
			}
			current = v[idx]
		default:
			return nil, fmt.Errorf("cannot traverse into %T", current)
		}
	}

	return current, nil
}
