package factory

import (
	"context"
	"os"
	"path/filepath"
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

// readRelationFixtureChild reads one committed fixture's child.json. The fixtures
// live in internal/testdata and are the same bytes
// internal/relation_index_guard_test.go asserts the rule against, read here by
// relative path so the two packages cannot drift apart (the same way
// internal/entity_write_validation_relations_test.go reads
// ../cmd/server/schemas).
func readRelationFixtureChild(t *testing.T, fixture string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "internal", "testdata", fixture, "child.json"))
	require.NoError(t, err)
	return string(body)
}

// TestNewEntityManagerWithConfig_Unit_RequiredRelationRootFailsClosed pins two
// things at once: that the factory runs the relation guard
// (internal.LoadRelationIndex, which validates as it builds) at all, and that it
// reads the *registry*.
//
// The wiring half matters because internal/relation_index_guard_test.go calls
// the guard directly — deleting the call from factory.go left the whole suite
// green, and with it the promise entity_crud_service.go makes to future readers
// ("the guard handles it") would silently have become false.
//
// The byte-source half is the divergence itself. The registry serves a child
// document that lists its x-relation property in root "required"; SCHEMA_DIR
// holds a *different* child, declaring the same relation without requiring it.
// Analysing the directory passes and boots a deployment whose every write is
// then rejected by a validator built from the other document. Only reading the
// registry catches it.
//
// SCHEMA_DIR still has to hold the fixture's parent.json, because that is where
// the served document's "$ref": "parent.json#/..." resolves from.
func TestNewEntityManagerWithConfig_Unit_RequiredRelationRootFailsClosed(t *testing.T) {
	t.Parallel()

	registry := newMockSchemaRegistry()
	registry.schemaBody = readRelationFixtureChild(t, "relation_required_root")

	config := validatorTestConfig(t, registry)
	config.Entity.SchemaDirectory = "../internal/testdata/relation_ok_not"

	deps := buildUnitEntityManagerDeps(schemameta.NewMetadataCache())
	em, err := newEntityManagerWithConfigContext(context.Background(), config, nil, deps)

	require.Error(t, err)
	assert.Nil(t, em, "no manager may be returned when a relation declaration is unhonourable")
	assert.Contains(t, err.Error(), "failed to validate schema relations",
		"the failure must come from the factory's relation guard, not from another check")
	// The offending schema and property must be named, so an operator can act.
	// Asserted on the full phrase rather than the bare name "test": that substring
	// also occurs in the fixture paths this test hands the factory, so it would
	// pass on an error that never identified a schema at all.
	assert.Contains(t, err.Error(), "registered schema test")
	assert.Contains(t, err.Error(), `requires relation root "contactSnapshot"`)
}

// TestNewEntityManagerWithConfig_Unit_UnregisteredSchemaFileIsNotGuarded is the
// converse, and the reason the guard must not scan SCHEMA_DIR: a document
// sitting in that directory under a name the registry never registers is read by
// nothing — not the validator, not the manager — so refusing to boot over it
// aborts startup for a file that has no effect on any write.
//
// The offending document here is the very one the test above proves fatal when
// it is registered.
func TestNewEntityManagerWithConfig_Unit_UnregisteredSchemaFileIsNotGuarded(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "unregistered.json"),
		[]byte(readRelationFixtureChild(t, "relation_required_root")), 0o600))

	config := validatorTestConfig(t, newMockSchemaRegistry())
	config.Entity.SchemaDirectory = dir

	deps := buildUnitEntityManagerDeps(schemameta.NewMetadataCache())
	em, err := newEntityManagerWithConfigContext(context.Background(), config, nil, deps)

	require.NoError(t, err)
	assert.NotNil(t, em)
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

// TestNewEntityManagerWithConfig_Unit_RelationIndexBuiltOnce pins the wiring the
// #318 review asked for: the factory builds the relation index once and hands
// that instance to the manager, instead of validating one and letting
// NewEntityManager load a second.
//
// Two loads were not merely wasteful. forma.SchemaRegistry is a public extension
// point that may serve documents from a database or over a network, so the
// second read can answer differently or fail outright — and when it fails
// NewEntityManager warns and continues with a nil index, leaving stripping off
// for the process lifetime behind a preflight that passed.
//
// Counted rather than observed through behaviour, because the manager exposes no
// way to ask which index it holds. Exactly two readers of the document remain,
// and both are named here so a future change to either is visible: schema
// validator construction (schemavalidate.New) and this single relation-index
// build. A third read means the manager started loading its own again.
func TestNewEntityManagerWithConfig_Unit_RelationIndexBuiltOnce(t *testing.T) {
	t.Parallel()

	registry := newMockSchemaRegistry()
	registry.schemaBody = readRelationFixtureChild(t, "relation_ok_not")

	config := validatorTestConfig(t, registry)
	config.Entity.SchemaDirectory = "../internal/testdata/relation_ok_not"

	deps := buildUnitEntityManagerDeps(schemameta.NewMetadataCache())
	em, err := newEntityManagerWithConfigContext(context.Background(), config, nil, deps)

	require.NoError(t, err)
	require.NotNil(t, em)
	assert.Equal(t, 2, registry.getSchemaByNameCalls,
		"the registered document may be read once by the validator and once for the relation "+
			"index; a third read means the manager is loading an index of its own again")
}
