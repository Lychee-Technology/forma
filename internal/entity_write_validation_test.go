package internal

import (
	"context"
	"fmt"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/lychee-technology/forma/internal/transform"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// TestNewEntityManagerAcceptsNilValidator pins that validation is opt-in at the
// wiring layer. Both e2e harnesses and every existing test construct a manager
// without a validator, and none of them may start failing.
func TestNewEntityManagerAcceptsNilValidator(t *testing.T) {
	require.NotPanics(t, func() {
		_ = NewEntityManager(nil, nil, nil, nil, forma.DefaultConfig(nil), nil)
	})
}

// validationRegistry is a SchemaRegistry whose attribute cache and JSON Schema
// agree, so the write path and the validator see the same entity. Schema name
// and id match the package's shared stub so the rest of the harness fits.
type validationRegistry struct{}

const validationSchemaJSON = `{
  "type": "object",
  "properties": {
    "name":   {"type": "string", "enum": ["open", "won"]},
    "person": {"type": "object", "properties": {"name": {"type": "string"}}}
  },
  "required": ["name"]
}`

func (validationRegistry) GetSchemaAttributeCacheByName(name string) (int16, forma.SchemaAttributeCache, error) {
	if name != "test" {
		return 0, nil, fmt.Errorf("schema %s not found", name)
	}
	return 100, forma.SchemaAttributeCache{
		"name":        {AttributeID: 1, ValueType: forma.ValueTypeText},
		"person.name": {AttributeID: 3, ValueType: forma.ValueTypeText},
	}, nil
}

func (v validationRegistry) GetSchemaAttributeCacheByID(int16) (string, forma.SchemaAttributeCache, error) {
	_, cache, err := v.GetSchemaAttributeCacheByName("test")
	return "test", cache, err
}

func (validationRegistry) ListSchemas() []string { return []string{"test"} }

func (validationRegistry) GetSchemaByName(string) (int16, forma.JSONSchema, error) {
	return 100, forma.JSONSchema{ID: 100, Name: "test", Schema: validationSchemaJSON}, nil
}

func (validationRegistry) GetSchemaByID(int16) (string, forma.JSONSchema, error) {
	return "test", forma.JSONSchema{ID: 100, Name: "test", Schema: validationSchemaJSON}, nil
}

// writeSpy wraps the real transformer and records what the write path handed to
// ToPersistentRecord: the map header pointer, and the map's own key set.
//
// The pointer is the load-bearing part. NormalizeDottedKeys' output is for the
// validator only, and for most payload shapes it produces records identical to
// the caller's map — so an equality assertion stays green while the normalized
// document is being written and #312's spelling-based precedence is silently
// gone. Only identity tells the two maps apart.
type writeSpy struct {
	inner model.PersistentRecordTransformer
	seen  []spiedWrite
}

type spiedWrite struct {
	pointer uintptr
	keys    map[string]struct{}
}

func (w *writeSpy) ToPersistentRecord(
	ctx context.Context, schemaID int16, rowID uuid.UUID, jsonData any,
) (*model.PersistentRecord, error) {
	if doc, ok := jsonData.(map[string]any); ok {
		keys := make(map[string]struct{}, len(doc))
		for key := range doc {
			keys[key] = struct{}{}
		}
		w.seen = append(w.seen, spiedWrite{pointer: mapPointer(doc), keys: keys})
	}
	return w.inner.ToPersistentRecord(ctx, schemaID, rowID, jsonData)
}

func (w *writeSpy) FromPersistentRecord(ctx context.Context, record *model.PersistentRecord) (map[string]any, error) {
	return w.inner.FromPersistentRecord(ctx, record)
}

// newValidationHarness builds a manager over validationRegistry. When validate
// is false the manager gets no validator at all, which is the baseline the
// stored-rows comparison needs.
func newValidationHarness(
	t *testing.T, strict, validate bool,
) (forma.EntityManager, *mockPersistentRecordRepository, *writeSpy) {
	t.Helper()
	registry := validationRegistry{}

	var validator *schemavalidate.Validator
	if validate {
		built, err := schemavalidate.New(registry, t.TempDir())
		require.NoError(t, err)
		validator = built
	}

	config := createTestConfig()
	config.Entity.ValidateUpdatesStrict = strict

	spy := &writeSpy{inner: transform.NewPersistentRecordTransformer(registry)}
	repo := newMockPersistentRecordRepository()
	manager := NewEntityManager(spy, repo, nil, registry, config, validator)
	return manager, repo, spy
}

func newValidatingManager(t *testing.T, strict bool) (forma.EntityManager, *mockPersistentRecordRepository) {
	t.Helper()
	manager, repo, _ := newValidationHarness(t, strict, true)
	return manager, repo
}

func createOp(data map[string]any) *forma.EntityOperation {
	return &forma.EntityOperation{
		Type:             forma.OperationCreate,
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "test"},
		Data:             data,
	}
}

func updateOp(rowID uuid.UUID, updates map[string]any) *forma.EntityOperation {
	return &forma.EntityOperation{
		Type:             forma.OperationUpdate,
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "test", RowID: rowID},
		Updates:          updates,
	}
}

// TestCreateRejectsEnumViolation is issue #314's own example end to end: a
// declared enum must actually reject a value outside it. Before this change the
// value was a known attribute, satisfied required, coerced to text, and
// persisted — the enum was decorative.
func TestCreateRejectsEnumViolation(t *testing.T) {
	manager, repo := newValidatingManager(t, false)

	_, err := manager.Create(context.Background(), createOp(map[string]any{"name": "banana"}))

	require.ErrorIs(t, err, forma.ErrInvalidInput)
	require.Empty(t, repo.insertedRecords, "a rejected create must not reach storage")
}

// TestCreateAcceptsValidPayload pins that enforcement does not reject good data.
func TestCreateAcceptsValidPayload(t *testing.T) {
	manager, repo := newValidatingManager(t, false)

	_, err := manager.Create(context.Background(), createOp(map[string]any{"name": "open"}))

	require.NoError(t, err)
	require.Len(t, repo.insertedRecords, 1)
}

// TestCreateRejectsDottedKeyTypeViolation pins the closed bypass. A literal
// dotted key is an unknown property to JSON Schema, so before normalization
// {"person.name": 99999} passed validation with its value never examined.
func TestCreateRejectsDottedKeyTypeViolation(t *testing.T) {
	manager, _ := newValidatingManager(t, false)

	_, err := manager.Create(context.Background(), createOp(map[string]any{
		"name":        "open",
		"person.name": 99999,
	}))

	require.ErrorIs(t, err, forma.ErrInvalidInput)
}

// TestBatchCreateAtomicRejectsEnumViolation covers the second create seam. The
// non-atomic batch paths delegate to the CRUD service, but batchCreateAtomic
// transforms and writes on its own.
func TestBatchCreateAtomicRejectsEnumViolation(t *testing.T) {
	manager, repo := newValidatingManager(t, false)

	_, err := manager.BatchCreate(context.Background(), &forma.BatchOperation{
		Atomic:     true,
		Operations: []forma.EntityOperation{*createOp(map[string]any{"name": "banana"})},
	})

	require.ErrorIs(t, err, forma.ErrInvalidInput)
	require.Empty(t, repo.insertedRecords)
}

// TestUpdateReportOnlyAcceptsViolation pins the staged rollout: with
// ValidateUpdatesStrict false, a merged document that violates the schema is
// accepted and only logged, because rows written before #314 may already
// violate it and must stay updatable.
func TestUpdateReportOnlyAcceptsViolation(t *testing.T) {
	manager, _ := newValidatingManager(t, false)
	created, err := manager.Create(context.Background(), createOp(map[string]any{"name": "open"}))
	require.NoError(t, err)

	_, err = manager.Update(context.Background(), updateOp(created.RowID, map[string]any{"name": "banana"}))

	require.NoError(t, err)
}

// TestUpdateStrictRejectsViolation pins the other half of the flag.
func TestUpdateStrictRejectsViolation(t *testing.T) {
	manager, _ := newValidatingManager(t, true)
	created, err := manager.Create(context.Background(), createOp(map[string]any{"name": "open"}))
	require.NoError(t, err)

	_, err = manager.Update(context.Background(), updateOp(created.RowID, map[string]any{"name": "banana"}))

	require.ErrorIs(t, err, forma.ErrInvalidInput)
}

// TestBatchUpdateAtomicStrictRejectsViolation covers the second update seam.
func TestBatchUpdateAtomicStrictRejectsViolation(t *testing.T) {
	manager, _ := newValidatingManager(t, true)
	created, err := manager.Create(context.Background(), createOp(map[string]any{"name": "open"}))
	require.NoError(t, err)

	_, err = manager.BatchUpdate(context.Background(), &forma.BatchOperation{
		Atomic:     true,
		Operations: []forma.EntityOperation{*updateOp(created.RowID, map[string]any{"name": "banana"})},
	})

	require.ErrorIs(t, err, forma.ErrInvalidInput)
}

// TestUpdateOfUnrelatedFieldKeepsRequiredSatisfied is the load-bearing update
// test. The schema requires "name", and a partial update that does not mention
// it must still succeed — which holds only because the *merged* document is
// validated, not the request fragment. Validating the fragment would reject
// essentially every update.
func TestUpdateOfUnrelatedFieldKeepsRequiredSatisfied(t *testing.T) {
	manager, _ := newValidatingManager(t, true)
	created, err := manager.Create(context.Background(), createOp(map[string]any{"name": "open"}))
	require.NoError(t, err)

	_, err = manager.Update(context.Background(), updateOp(created.RowID, map[string]any{
		"person": map[string]any{"name": "ada"},
	}))

	require.NoError(t, err)
}
