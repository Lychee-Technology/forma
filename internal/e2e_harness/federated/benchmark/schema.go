package benchmark

import (
	"fmt"
	"path/filepath"
	"runtime"

	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
)

const (
	SchemaIDCustomer int16 = 100
	SchemaIDSecurity int16 = 101
	SchemaIDTrade    int16 = 102
)

// SchemaFixture describes a benchmark schema fixture.
type SchemaFixture struct {
	ID          int16  `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

// SchemaRegistrar is the narrow harness contract needed by the benchmark scaffold.
type SchemaRegistrar interface {
	SetupSchema(schemaID int16, schemaName string) error
}

// DefaultSchemaFixtures returns the TPC-E-inspired schema set for the benchmark.
func DefaultSchemaFixtures() []SchemaFixture {
	return []SchemaFixture{
		{ID: SchemaIDCustomer, Name: "customer", Description: "customer dimension with hot filter and EAV profile fields"},
		{ID: SchemaIDSecurity, Name: "security", Description: "security reference data with hot symbol lookup and cold metrics"},
		{ID: SchemaIDTrade, Name: "trade", Description: "trade fact entity with hybrid pagination and filter attributes"},
	}
}

// FixturesDir returns the on-disk schema fixture directory.
func FixturesDir() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return ""
	}
	return filepath.Join(filepath.Dir(file), "schemas")
}

// LoadFixtureRegistry loads the benchmark schema fixtures through the standard file registry.
func LoadFixtureRegistry() (forma.SchemaRegistry, error) {
	registry, err := internal.NewFileSchemaRegistryFromDirectory(FixturesDir())
	if err != nil {
		return nil, fmt.Errorf("load fixture registry: %w", err)
	}
	for _, fixture := range DefaultSchemaFixtures() {
		id, _, err := registry.GetSchemaAttributeCacheByName(fixture.Name)
		if err != nil {
			return nil, fmt.Errorf("load fixture %s: %w", fixture.Name, err)
		}
		if id != fixture.ID {
			return nil, fmt.Errorf("fixture %s expected schema ID %d, got %d", fixture.Name, fixture.ID, id)
		}
	}
	return registry, nil
}

// RegisterFixtureSchemas wires the benchmark fixtures into a harness-backed schema registry table.
func RegisterFixtureSchemas(registrar SchemaRegistrar) error {
	for _, fixture := range DefaultSchemaFixtures() {
		if err := registrar.SetupSchema(fixture.ID, fixture.Name); err != nil {
			return fmt.Errorf("register fixture %s: %w", fixture.Name, err)
		}
	}
	return nil
}

func workloadSchemaID(schemaName string) (int16, error) {
	for _, fixture := range DefaultSchemaFixtures() {
		if fixture.Name == schemaName {
			return fixture.ID, nil
		}
	}
	return 0, fmt.Errorf("unknown benchmark schema %q", schemaName)
}
