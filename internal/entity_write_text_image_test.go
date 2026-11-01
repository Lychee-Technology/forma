package internal

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/lychee-technology/forma/internal/transform"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// changingText is a fmt.Stringer whose String() answers the next of its
// images on each call, the last one from then on: a Go caller's value whose
// text is not a function of the value (#773). Its JSON form is a fixed
// string, so schema validation of a string attribute accepts it.
type changingText struct {
	images []string
	calls  *int
}

func newChangingText(images ...string) changingText {
	return changingText{images: images, calls: new(int)}
}

func (c changingText) String() string {
	i := min(*c.calls, len(c.images)-1)
	*c.calls++
	return c.images[i]
}

func (changingText) MarshalJSON() ([]byte, error) { return []byte(`"changing text"`), nil }

// textImageRegistry declares "at" a string that must spell an RFC 3339
// time. A time.Time marshals to that spelling, while its text image,
// time.String(), does not match the pattern: validation passes the time only
// while it judges the value's JSON form.
type textImageRegistry struct{}

const textImageSchemaJSON = `{
  "type": "object",
  "properties": {
    "name": {"type": "string"},
    "at":   {"type": "string", "format": "date-time", "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T"}
  }
}`

func (textImageRegistry) GetSchemaAttributeCacheByName(name string) (int16, forma.SchemaAttributeCache, error) {
	if name != "test" {
		return 0, nil, fmt.Errorf("schema %s not found", name)
	}
	return 100, forma.SchemaAttributeCache{
		"name": {AttributeID: 1, ValueType: forma.ValueTypeText},
		"at":   {AttributeID: 2, ValueType: forma.ValueTypeText},
	}, nil
}

func (r textImageRegistry) GetSchemaAttributeCacheByID(int16) (string, forma.SchemaAttributeCache, error) {
	_, cache, err := r.GetSchemaAttributeCacheByName("test")
	return "test", cache, err
}

func (textImageRegistry) ListSchemas() []string { return []string{"test"} }

func (textImageRegistry) GetSchemaByName(string) (int16, forma.JSONSchema, error) {
	return 100, forma.JSONSchema{ID: 100, Name: "test", Schema: textImageSchemaJSON}, nil
}

func (textImageRegistry) GetSchemaByID(int16) (string, forma.JSONSchema, error) {
	return "test", forma.JSONSchema{ID: 100, Name: "test", Schema: textImageSchemaJSON}, nil
}

// newTextImageManager answers a manager validating updates strictly, over a
// mock repository holding one row.
func newTextImageManager(t *testing.T) (forma.EntityManager, uuid.UUID) {
	t.Helper()
	registry := textImageRegistry{}
	validator, err := schemavalidate.New(registry, t.TempDir())
	require.NoError(t, err)
	config := createTestConfig()
	config.Entity.ValidateUpdatesStrict = true
	transformer := transform.NewPersistentRecordTransformer(registry)
	repo := newMockPersistentRecordRepository()
	rowID := uuid.New()
	repo.storeRecord(buildPersistentRecord(t, transformer, 100, rowID, map[string]any{"name": "stored"}))
	return mustNewEntityManager(t, transformer, repo, nil, registry, config, validator), rowID
}

// TestUpdateWritesTheTextImageIntakeChecked: Update and an atomic
// BatchUpdate store the text image their intake judged, so a Stringer that
// answers valid text once and NUL after is stored as its first answer rather
// than refused after the row lock, and one that changes between valid
// strings is stored as the string that was checked. String() is called once.
// The caller's Updates are not written to.
func TestUpdateWritesTheTextImageIntakeChecked(t *testing.T) {
	ctx := context.Background()
	at := time.Date(2026, 11, 1, 5, 49, 22, 0, time.UTC)
	routes := map[string]func(forma.EntityManager, uuid.UUID, map[string]any) (*forma.DataRecord, error){
		"Update": func(em forma.EntityManager, rowID uuid.UUID, updates map[string]any) (*forma.DataRecord, error) {
			return em.Update(ctx, &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: "test", RowID: rowID},
				Type: forma.OperationUpdate, Updates: updates})
		},
		"atomic BatchUpdate": func(em forma.EntityManager, rowID uuid.UUID, updates map[string]any) (*forma.DataRecord, error) {
			result, err := em.BatchUpdate(ctx, &forma.BatchOperation{Atomic: true, Operations: []forma.EntityOperation{atomicUpdateOp(rowID, updates)}})
			if err != nil {
				return nil, err
			}
			return result.Successful[0], nil
		},
	}
	for route, update := range routes {
		for name, value := range map[string]changingText{
			"valid, then NUL":           newChangingText("first", "a\x00b"),
			"changing between valid":    newChangingText("one", "two", "three"),
			"valid, then invalid UTF-8": newChangingText("first", "a\xffb"),
		} {
			em, rowID := newTextImageManager(t)
			updates := map[string]any{"name": value, "at": at}
			record, err := update(em, rowID, updates)
			require.NoError(t, err, "%s, %s", route, name)
			assert.Equal(t, value.images[0], record.Attributes["name"], "%s, %s: the checked image is stored", route, name)
			assert.Equal(t, at.String(), record.Attributes["at"], "%s, %s: a time.Time's text image is stored as before", route, name)
			assert.Equal(t, 1, *value.calls, "%s, %s: String() is called once", route, name)
			assert.Equal(t, map[string]any{"name": value, "at": at}, updates, "%s, %s: the caller's Updates as sent", route, name)
		}
	}
}

// TestUpdateValidationStillJudgesTheJSONForm: carrying the text image does
// not change what validation judges. The pattern refuses a time's text image,
// so a time.Time at a string attribute passes only because validation still
// sees its RFC 3339 JSON form; a plain string with the text image is refused.
func TestUpdateValidationStillJudgesTheJSONForm(t *testing.T) {
	at := time.Date(2026, 11, 1, 5, 49, 22, 0, time.UTC)
	em, rowID := newTextImageManager(t)
	op := func(value any) *forma.EntityOperation {
		return &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: "test", RowID: rowID},
			Type: forma.OperationUpdate, Updates: map[string]any{"at": value}}
	}
	_, err := em.Update(context.Background(), op(at))
	require.NoError(t, err, "validation judges the time's JSON form")
	_, err = em.Update(context.Background(), op(at.String()))
	require.ErrorIs(t, err, forma.ErrInvalidInput, "the time's text image fails the pattern")
}
