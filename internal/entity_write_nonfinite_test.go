package internal

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/lychee-technology/forma/internal/transform"
	"github.com/stretchr/testify/require"
)

// numericValidationRegistry mirrors validationRegistry but declares a numeric
// attribute, which the shared fixture deliberately lacks. File-local so the
// shared fixture's assertions stay untouched.
type numericValidationRegistry struct{}

const numericSchemaJSON = `{
  "type": "object",
  "properties": {
    "name":  {"type": "string"},
    "score": {"type": "number"}
  },
  "required": ["name"]
}`

func (numericValidationRegistry) GetSchemaAttributeCacheByName(name string) (int16, forma.SchemaAttributeCache, error) {
	if name != "test" {
		return 0, nil, fmt.Errorf("schema %s not found", name)
	}
	return 100, forma.SchemaAttributeCache{
		"name":  {AttributeID: 1, ValueType: forma.ValueTypeText},
		"score": {AttributeID: 2, ValueType: forma.ValueTypeNumeric},
	}, nil
}

func (r numericValidationRegistry) GetSchemaAttributeCacheByID(int16) (string, forma.SchemaAttributeCache, error) {
	_, cache, err := r.GetSchemaAttributeCacheByName("test")
	return "test", cache, err
}

func (numericValidationRegistry) ListSchemas() []string { return []string{"test"} }

func (numericValidationRegistry) GetSchemaByName(string) (int16, forma.JSONSchema, error) {
	return 100, forma.JSONSchema{ID: 100, Name: "test", Schema: numericSchemaJSON}, nil
}

func (numericValidationRegistry) GetSchemaByID(int16) (string, forma.JSONSchema, error) {
	return "test", forma.JSONSchema{ID: 100, Name: "test", Schema: numericSchemaJSON}, nil
}

func newNumericValidatingManager(t *testing.T, strict bool) (forma.EntityManager, *mockPersistentRecordRepository) {
	t.Helper()
	registry := numericValidationRegistry{}

	validator, err := schemavalidate.New(registry, t.TempDir())
	require.NoError(t, err)

	config := createTestConfig()
	config.Entity.ValidateUpdatesStrict = strict

	transformer := transform.NewPersistentRecordTransformer(registry)
	repo := newMockPersistentRecordRepository()
	manager := mustNewEntityManager(t, transformer, repo, nil, registry, config, validator)
	return manager, repo
}

// TestCreateClassifiesEmbedderNaNAsInvalidInput is issue #322's headline: a Go
// embedder handing math.NaN() to Create gets invalid input (4xx), not an
// operator fault (5xx). The validator's marshal step fires first on create, so
// this exercises the schemavalidate reclassification end to end.
func TestCreateClassifiesEmbedderNaNAsInvalidInput(t *testing.T) {
	manager, _ := newNumericValidatingManager(t, false)

	_, err := manager.Create(context.Background(),
		createOp(map[string]any{"name": "x", "score": math.NaN()}))

	require.ErrorIs(t, err, forma.ErrInvalidInput)
}

// TestReportOnlyUpdateStillRejectsNonFinite pins the interaction that makes
// #322's reclassification safe. Report-only mode now absorbs the validator's
// marshal carrier like any violation, so the transform guard is all that
// stands between an absorbed NaN and a stored row that 500s on every
// subsequent read (the response json.Marshal cannot represent it). The string
// spelling is the HTTP-reachable shape: strconv.ParseFloat accepts "NaN", so
// before the guard a report-only update could poison a row over HTTP.
func TestReportOnlyUpdateStillRejectsNonFinite(t *testing.T) {
	manager, repo := newNumericValidatingManager(t, false)
	created, err := manager.Create(context.Background(),
		createOp(map[string]any{"name": "x", "score": 1.5}))
	require.NoError(t, err)

	for name, value := range map[string]any{
		"embedder float64": math.NaN(),
		"HTTP string":      "NaN",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := manager.Update(context.Background(),
				updateOp(created.RowID, map[string]any{"score": value}))

			require.ErrorIs(t, err, forma.ErrInvalidInput)
			require.Contains(t, err.Error(), "score",
				"the transform rejection names the attribute; a marshal error cannot")
		})
	}

	stored, err := transform.NewPersistentRecordTransformer(numericValidationRegistry{}).
		FromPersistentRecord(context.Background(), repo.records[100][created.RowID])
	require.NoError(t, err)
	require.EqualValues(t, 1.5, stored["score"], "the finite value must have survived both rejected updates")
}
