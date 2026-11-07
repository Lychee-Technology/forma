package schemameta

import (
	"fmt"
	"os"
	"sort"
	"strings"
)

// ListInitDBSchemaNames returns the schema names init-db registers from
// schemaDir: every .json file except the *_attributes.json metadata files.
// The DB-backed registry (NewFileSchemaRegistryContext, built by cmd/server
// and cmd/lambda) serves exactly the names init-db wrote to the schema_registry
// table, so this is the name set production analyses.
//
// It is deliberately wider than NewFileSchemaRegistryFromDirectory, which also
// leaves out *_full.json. Tests that need the production name set without a
// database derive it here, so they cannot drift from what init-db registers.
//
// The order is that of the file names, which on an empty registry fixes the
// schema ids.
func ListInitDBSchemaNames(schemaDir string) ([]string, error) {
	entries, err := os.ReadDir(schemaDir)
	if err != nil {
		return nil, fmt.Errorf("read schema directory(%s): %w", schemaDir, err)
	}

	var schemaFiles []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".json") || strings.HasSuffix(name, "_attributes.json") {
			continue
		}
		schemaFiles = append(schemaFiles, name)
	}
	sort.Strings(schemaFiles)

	names := make([]string, len(schemaFiles))
	for i, file := range schemaFiles {
		names[i] = strings.TrimSuffix(file, ".json")
	}
	return names, nil
}
