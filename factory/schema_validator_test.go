package factory

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// validatorTestConfig builds the minimal config the unit construction path needs,
// over the supplied registry.
func validatorTestConfig(t *testing.T, registry forma.SchemaRegistry) *forma.Config {
	t.Helper()
	config := forma.DefaultConfig(registry)
	config.Database.TableNames = forma.TableNames{
		SchemaRegistry: "schema_registry",
		EAVData:        "eav_data",
		EntityMain:     "entity_main",
	}
	config.Entity.SchemaDirectory = t.TempDir()
	return config
}

// TestNewEntityManagerWithConfig_Unit_UnparseableSchemaFailsClosed pins the
// headline #314 behaviour at the layer that owns it: a registered schema whose
// document cannot be parsed aborts construction instead of yielding a manager
// with validation silently absent.
//
// Without this, the only evidence of fail-closed would be that unrelated tests
// happened to break once, so a later change that swallowed the error to
// "unbreak startup" would restore the original bug unnoticed.
func TestNewEntityManagerWithConfig_Unit_UnparseableSchemaFailsClosed(t *testing.T) {
	t.Parallel()

	registry := newMockSchemaRegistry()
	registry.schemaBody = `{`

	deps := buildUnitEntityManagerDeps(schemameta.NewMetadataCache())
	em, err := newEntityManagerWithConfigContext(
		context.Background(), validatorTestConfig(t, registry), nil, deps)

	require.Error(t, err)
	assert.Nil(t, em, "no manager may be returned when validation cannot be built")
	assert.Contains(t, err.Error(), "failed to build schema validator")
	// The offending schema must be named, so an operator can act on the message.
	assert.Contains(t, err.Error(), "test")
}

// TestNewEntityManagerWithConfig_Unit_MissingSchemaDocumentFailsClosed
// characterises the real upgrade risk of #314, and is the executable form of the
// release note.
//
// The shipped fileSchemaRegistry deliberately tolerates a schema_registry row
// whose <name>.json is absent: it registers the attribute cache and simply does
// not store a document, after which GetSchemaByName returns ErrNotFound. That was
// a supported configuration before this change. schemavalidate.New iterates every
// name from ListSchemas, so such a row now refuses startup.
//
// If this behaviour is ever intentionally relaxed, this test is the one to
// revisit — it is asserting a deliberate breaking change, not merely a bug guard.
func TestNewEntityManagerWithConfig_Unit_MissingSchemaDocumentFailsClosed(t *testing.T) {
	t.Parallel()

	registry := newMockSchemaRegistry()
	registry.schemaDocMissing = true

	deps := buildUnitEntityManagerDeps(schemameta.NewMetadataCache())
	em, err := newEntityManagerWithConfigContext(
		context.Background(), validatorTestConfig(t, registry), nil, deps)

	require.Error(t, err)
	assert.Nil(t, em)
	assert.Contains(t, err.Error(), "failed to build schema validator")
	assert.ErrorIs(t, err, forma.ErrNotFound,
		"the registry's not-found cause must survive wrapping so the operator can tell "+
			"a missing schema document from an unparseable one")
}

// requiredRelationRootSchemaDir is the committed two-schema pair whose child
// lists its x-relation property in root-level "required" — the shape
// internal.ValidateRelationSchemas refuses. It is the same directory
// internal/relation_index_guard_test.go asserts the rule against, read here by
// relative path so the two packages cannot drift apart (the same way
// internal/entity_write_validation_relations_test.go reads
// ../cmd/server/schemas).
//
// The mock registry serves its own document and never references this directory,
// so pointing Entity.SchemaDirectory at it does not disturb validator
// construction — only the relation guard reads it.
const requiredRelationRootSchemaDir = "../internal/testdata/relation_required_root"

// TestNewEntityManagerWithConfig_Unit_RequiredRelationRootFailsClosed pins the
// factory's call to internal.ValidateRelationSchemas — the wiring that makes the
// #318 guard a *startup* guard rather than a function nobody runs.
//
// internal/relation_index_guard_test.go covers the rule itself, but it calls
// LoadRelationIndex and ValidateRelationSchemas directly: deleting the call from
// factory.go left the whole suite green, and with it the promise
// entity_crud_service.go makes to future readers ("the guard handles it") would
// have silently become false. This test fails when that call is removed.
func TestNewEntityManagerWithConfig_Unit_RequiredRelationRootFailsClosed(t *testing.T) {
	t.Parallel()

	config := validatorTestConfig(t, newMockSchemaRegistry())
	config.Entity.SchemaDirectory = requiredRelationRootSchemaDir

	deps := buildUnitEntityManagerDeps(schemameta.NewMetadataCache())
	em, err := newEntityManagerWithConfigContext(context.Background(), config, nil, deps)

	require.Error(t, err)
	assert.Nil(t, em, "no manager may be returned when a relation declaration is unhonourable")
	assert.Contains(t, err.Error(), "failed to validate schema relations",
		"the failure must come from the factory's relation guard, not from another check")
	// The offending schema and property must be named, so an operator can act.
	assert.Contains(t, err.Error(), "child")
	assert.Contains(t, err.Error(), "contactSnapshot")
}

// TestNewEntityManagerWithConfig_Unit_ValidatorBuiltBeforeReadSurface pins the
// ordering that keeps fail-closed leak-free. The validator is built before the
// DuckDB client is opened, so its failure path owns no resources and cannot leak
// them — the mirror of the parquet-source path, which does have to close the
// client on the way out (see ..._ParquetSourceErrorClosesDuckDBClient).
//
// Asserted structurally: with DuckDB enabled and an unresolvable schema, the
// DuckDB client factory must never be reached.
func TestNewEntityManagerWithConfig_Unit_ValidatorBuiltBeforeReadSurface(t *testing.T) {
	t.Parallel()

	registry := newMockSchemaRegistry()
	registry.schemaBody = `{`

	duckDBFactoryCalled := false
	deps := buildUnitEntityManagerDeps(schemameta.NewMetadataCache())
	deps.newDuckDBClient = func(context.Context, forma.DuckDBConfig) (*federated.DuckDBClient, error) {
		duckDBFactoryCalled = true
		return nil, nil
	}

	config := validatorTestConfig(t, registry)
	config.DuckDB.Enabled = true

	_, err := newEntityManagerWithConfigContext(context.Background(), config, nil, deps)

	require.Error(t, err)
	assert.False(t, duckDBFactoryCalled,
		"schema validation must fail closed before any read surface is opened, so the "+
			"failure path holds no resources to leak")
}
