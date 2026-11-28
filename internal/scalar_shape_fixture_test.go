package internal

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/lychee-technology/forma/internal/transform"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Fixtures for the real-PostgreSQL scalar-shape tests (#924): one
// environment's tables and registry written through a manager per update
// validation mode, and a row's whole stored state for before/after
// comparison.

// updateValidationMode is how a manager validates updates. Creates validate
// strictly whenever a validator is configured.
type updateValidationMode struct {
	name      string
	validator bool
	strict    bool
}

var updateValidationModes = []updateValidationMode{
	{name: "strict", validator: true, strict: true},
	{name: "report-only", validator: true},
	{name: "no validator"},
}

// withUpdateValidation answers an environment over e's tables and registry
// whose manager and transactor validate updates by mode. catalog, when not
// nil, is enforced as the unique-key managers enforce it.
func (e *txEnv) withUpdateValidation(t *testing.T, mode updateValidationMode, schemaDir string, catalog *UniqueKeyCatalog) *txEnv {
	t.Helper()
	return e.withUpdateValidationEmitting(t, mode, schemaDir, catalog, nil)
}

// withUpdateValidationEmitting is withUpdateValidation with emitter, when
// not nil, receiving the manager's metrics.
func (e *txEnv) withUpdateValidationEmitting(t *testing.T, mode updateValidationMode, schemaDir string, catalog *UniqueKeyCatalog, emitter forma.MetricEmitter) *txEnv {
	t.Helper()
	var validator *schemavalidate.Validator
	if mode.validator {
		var err error
		validator, err = schemavalidate.New(SnapshotSchemaDocuments(e.registry), schemaDir)
		require.NoError(t, err)
	}
	config := &forma.Config{
		Database: forma.DatabaseConfig{TableNames: forma.TableNames{
			SchemaRegistry: e.schemaTable,
			EntityMain:     e.tables.EntityMain,
			EAVData:        e.tables.EAVData,
			ChangeLog:      e.tables.ChangeLog,
		}},
		Query:       forma.QueryConfig{DefaultPageSize: 20, MaxPageSize: 100},
		Entity:      forma.EntityConfig{SchemaDirectory: schemaDir, ValidateUpdatesStrict: mode.strict},
		Transaction: forma.TransactionConfig{DefaultTimeout: 30 * time.Second},
		Metrics:     forma.MetricsConfig{Emitter: emitter},
	}
	var repoOpts []RepoOption
	var managerOpts []EntityManagerOption
	if catalog != nil {
		repoOpts = append(repoOpts, WithUniqueKeyCatalog(catalog))
		managerOpts = append(managerOpts, WithUniqueKeys(catalog))
	}
	repo := NewDBPersistentRecordRepository(e.postgresPool, e.metadata, repoOpts...)
	env := *e.integrationEnv
	env.manager = mustNewEntityManager(t, transform.NewPersistentRecordTransformer(e.registry), repo, nil, e.registry, config, validator, managerOpts...)
	txr, err := NewTransactor(env.manager)
	require.NoError(t, err)
	return &txEnv{integrationEnv: &env, txr: txr.(*transactor)}
}

// scalarContainers are the values a scalar attribute is refused, built
// fresh for every call.
func scalarContainers() map[string]func() any {
	return map[string]func() any{
		"{}":        func() any { return map[string]any{} },
		"[]":        func() any { return []any{} },
		"object":    func() any { return map[string]any{"x": "y"} },
		"array":     func() any { return []any{"a@x"} },
		"nested []": func() any { return []any{[]any{"a@x"}} },
	}
}

// storedState is everything an update could change about one row: its
// entity_main row (the version stamp included), its EAV rows, its change_log
// entries, and the whole unique-key table.
type storedState struct {
	row  rowSnapshot
	keys []keyEntry
}

func (e *txEnv) storedState(t *testing.T, schema string, rowID uuid.UUID) storedState {
	t.Helper()
	return storedState{row: e.snapshot(t, schema, rowID), keys: e.keyEntries(t)}
}

// requireUnchangedState asserts that nothing about rowID, nor any key entry,
// changed since before.
func (e *txEnv) requireUnchangedState(t *testing.T, schema string, rowID uuid.UUID, before storedState, msg string) {
	t.Helper()
	after := e.storedState(t, schema, rowID)
	require.Equal(t, before.row, after.row, "%s: the row's stored state changed", msg)
	require.Equal(t, before.keys, after.keys, "%s: the key table changed", msg)
}

// requireScalarShapeRefusal asserts err is the scalar-shape refusal of attr:
// ErrInvalidInput, published, naming the attribute, its type and the
// container.
func requireScalarShapeRefusal(t *testing.T, err error, attr string, vt forma.ValueType, msg string) {
	t.Helper()
	require.Error(t, err, msg)
	require.ErrorIs(t, err, forma.ErrInvalidInput, msg)
	published, ok := forma.ResolvePublicMessage(err)
	require.True(t, ok, "%s: the refusal is published", msg)
	assert.Contains(t, published, "invalid value for attribute '"+attr+"'", msg)
	assert.Contains(t, published, "expected a scalar of type "+string(vt), msg)
}
