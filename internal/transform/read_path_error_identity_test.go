package transform

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// The read path rebuilds a row through FromEAVRecord (EAV rows) and
// readFromMainColumn (column-bound attributes). Before #405 neither funnel's
// error named the row: `convert record attrID=3: non-finite …` is enough for
// a single-entity GET, whose row is the request's, but a list or query
// enrichment read left the operator with an attribute and no row. These tests
// pin that every consistency error on that path names the row.

// TestFromEAVRecordNamesRecordIdentity pins the hop that holds the record:
// it wraps the extraction error with schema, row, and attrID, and for a list
// element the array indices, while keeping the inner error's own prose.
func TestFromEAVRecordNamesRecordIdentity(t *testing.T) {
	c := NewAttributeConverter(nil)
	rowID := uuid.Must(uuid.NewV7())
	nan := math.NaN()
	text := "not a number"

	t.Run("non-finite bool", func(t *testing.T) {
		_, err := c.FromEAVRecord(model.EAVRecord{
			SchemaID: 7, RowID: rowID, AttrID: 3, ValueNumeric: &nan,
		}, forma.ValueTypeBool)
		require.Error(t, err)
		require.Contains(t, err.Error(), "record schema=7 row="+rowID.String()+" attrID=3: ")
		require.Contains(t, err.Error(), "has no truth value")
		require.NotContains(t, err.Error(), "arrayIndices", "a scalar has no indices to name")
	})

	t.Run("storage mismatch on a list element", func(t *testing.T) {
		_, err := c.FromEAVRecord(model.EAVRecord{
			SchemaID: 7, RowID: rowID, AttrID: 19, ArrayIndices: "2", ValueText: &text,
		}, forma.ValueTypeInteger)
		require.Error(t, err)
		require.Contains(t, err.Error(), "record schema=7 row="+rowID.String()+" attrID=19 arrayIndices=2: ")
		require.Contains(t, err.Error(), "storage type mismatch for integer")
	})
}

// TestFromEAVRecordsNamesAttributeAndRow pins the outer hop: it adds the
// attribute name, which only it has resolved, and does not repeat the
// identity FromEAVRecord already rendered.
func TestFromEAVRecordsNamesAttributeAndRow(t *testing.T) {
	converter := NewAttributeConverter(newListStubRegistry())
	rowID := uuid.Must(uuid.NewV7())
	text := "seven"

	_, err := converter.FromEAVRecords([]model.EAVRecord{
		{SchemaID: 100, RowID: rowID, AttrID: 19, ArrayIndices: "0", ValueText: &text},
	})
	require.Error(t, err)
	msg := err.Error()
	require.Contains(t, msg, "convert attribute 'nums': record schema=100 row="+rowID.String()+" attrID=19 arrayIndices=0: ")
	require.Contains(t, msg, "storage type mismatch for integer")
	require.Equal(t, 1, strings.Count(msg, "attrID="), "the identity is rendered once, at the hop that holds the record")
	require.Equal(t, 1, strings.Count(msg, rowID.String()), "the row is rendered once")
}

// TestFromEAVRecordsRequiredPolicyErrorNamesRow pins the third read-path
// consistency error raised from the same function: a persisted row missing a
// required EAV row used to name the attribute and schema only.
func TestFromEAVRecordsRequiredPolicyErrorNamesRow(t *testing.T) {
	registry := &stubSchemaRegistry{
		schemaID:   405,
		schemaName: "required_schema",
		cache: forma.SchemaAttributeCache{
			"id":    {AttributeID: 1, ValueType: forma.ValueTypeText, Required: true},
			"email": {AttributeID: 2, ValueType: forma.ValueTypeText, Required: true},
		},
	}
	converter := NewAttributeConverter(registry)
	rowID := uuid.Must(uuid.NewV7())
	idValue := "row-1"

	_, err := converter.FromEAVRecords([]model.EAVRecord{
		{SchemaID: 405, RowID: rowID, AttrID: 1, ValueText: &idValue},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "required-policy check for schema 405 row "+rowID.String()+": ")
	require.Contains(t, err.Error(), "missing required attribute 'email' (attrID=2)")
}

// TestFromPersistentRecordConsistencyErrorsNameRow drives the issue's own
// scenario end to end through FromPersistentRecord, once per funnel: a NaN
// stored for an EAV bool, and an off-contract bool_text main column (#404).
// Both must name the row the operator has to go and look at.
func TestFromPersistentRecordConsistencyErrorsNameRow(t *testing.T) {
	ctx := context.Background()
	registry := newPersistentTransformerRegistry()
	transformer := NewPersistentRecordTransformer(registry)
	schemaID, cache, err := registry.GetSchemaAttributeCacheByName("persistent_test")
	require.NoError(t, err)
	rowID := uuid.Must(uuid.NewV7())

	t.Run("EAV funnel", func(t *testing.T) {
		nan := math.NaN()
		_, err := transformer.FromPersistentRecord(ctx, &model.PersistentRecord{
			RowID:    rowID,
			SchemaID: schemaID,
			OtherAttributes: []model.EAVRecord{{
				SchemaID: schemaID, RowID: rowID, AttrID: cache["jobs.active"].AttributeID, ArrayIndices: "1", ValueNumeric: &nan,
			}},
		})
		require.Error(t, err)
		msg := err.Error()
		require.Contains(t, msg, "failed to convert EAVRecords to EntityAttributes: convert attribute 'jobs.active': record schema=201 row="+rowID.String()+" attrID=13 arrayIndices=1: ")
		require.Contains(t, msg, "has no truth value")
	})

	t.Run("main-column funnel", func(t *testing.T) {
		_, err := transformer.FromPersistentRecord(ctx, &model.PersistentRecord{
			RowID:     rowID,
			SchemaID:  schemaID,
			TextItems: map[string]string{string(forma.MainColumnText02): "true"},
		})
		require.Error(t, err)
		msg := err.Error()
		require.Contains(t, msg, "failed to read attribute isActiveText of row "+rowID.String()+" from main column: ")
		require.Contains(t, msg, "column "+string(forma.MainColumnText02))
	})
}

// TestFromPersistentRecordReconstructionErrorsNameRow pins the last hop of
// the rebuild, jsonTransformer.FromAttributes, and the metadata lookup ahead
// of both funnels. Neither holds the row on its own: a persisted list element
// with malformed array_indices passes FromEAVRecord (which does not parse
// indices) and fails only when the JSON tree is assembled, so the row has to
// be named by the FromPersistentRecord wrap.
func TestFromPersistentRecordReconstructionErrorsNameRow(t *testing.T) {
	ctx := context.Background()
	rowID := uuid.Must(uuid.NewV7())

	t.Run("malformed persisted array indices", func(t *testing.T) {
		registry := newPersistentTransformerRegistry()
		transformer := NewPersistentRecordTransformer(registry)
		schemaID, cache, err := registry.GetSchemaAttributeCacheByName("persistent_test")
		require.NoError(t, err)
		active := 1.0

		_, err = transformer.FromPersistentRecord(ctx, &model.PersistentRecord{
			RowID:    rowID,
			SchemaID: schemaID,
			OtherAttributes: []model.EAVRecord{{
				SchemaID: schemaID, RowID: rowID, AttrID: cache["jobs.active"].AttributeID, ArrayIndices: "x", ValueNumeric: &active,
			}},
		})
		require.Error(t, err)
		msg := err.Error()
		require.Contains(t, msg, "failed to convert attributes of row "+rowID.String()+" to JSON: parse array indices for attribute 'jobs.active': invalid index 'x'")
		require.Equal(t, 1, strings.Count(msg, rowID.String()), "the row is rendered once")
	})

	t.Run("schema metadata lookup", func(t *testing.T) {
		transformer := NewPersistentRecordTransformer(nil)

		_, err := transformer.FromPersistentRecord(ctx, &model.PersistentRecord{RowID: rowID, SchemaID: 201})
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to load schema 201 metadata for row "+rowID.String()+": ")
		require.Contains(t, err.Error(), "schema registry is not configured")
	})
}

// secondCacheReadFailsRegistry answers GetSchemaAttributeCacheByID normally
// once and fails every later call. FromPersistentRecord reads the metadata
// itself and then FromEAVRecords reads it again (#569); forma.SchemaRegistry
// promises nothing about repeated reads, so the converter's own lookups can
// fail after the transformer's succeeded, and must still name the row.
type secondCacheReadFailsRegistry struct {
	*stubSchemaRegistry
	cacheCalls int
}

func (r *secondCacheReadFailsRegistry) GetSchemaAttributeCacheByID(id int16) (string, forma.SchemaAttributeCache, error) {
	r.cacheCalls++
	if r.cacheCalls > 1 {
		return "", nil, errors.New("registry read a second time within one rebuild")
	}
	return r.stubSchemaRegistry.GetSchemaAttributeCacheByID(id)
}

// failingSchemaByIDRegistry fails GetSchemaByID, the read relationRootsFor
// makes when a relation-root lookup is installed, while answering the
// attribute-cache reads normally.
type failingSchemaByIDRegistry struct {
	*stubSchemaRegistry
}

func (r *failingSchemaByIDRegistry) GetSchemaByID(int16) (string, forma.JSONSchema, error) {
	return "", forma.JSONSchema{}, errors.New("schema name lookup failed")
}

// TestFromEAVRecordsLookupErrorsNameRow pins the converter's two lookups
// ahead of the per-record loop: the relation-root resolution and the schema
// metadata read. Both hold the records, so both name the row from them, and
// a list read that fails there is attributable like every other rebuild
// failure. Driven through FromPersistentRecord so the transformer's own
// metadata read succeeds first and only the converter's fails.
func TestFromEAVRecordsLookupErrorsNameRow(t *testing.T) {
	ctx := context.Background()
	rowID := uuid.Must(uuid.NewV7())
	base := newPersistentTransformerRegistry()
	schemaID, cache, err := base.GetSchemaAttributeCacheByName("persistent_test")
	require.NoError(t, err)
	active := 1.0
	record := &model.PersistentRecord{
		RowID:    rowID,
		SchemaID: schemaID,
		OtherAttributes: []model.EAVRecord{{
			SchemaID: schemaID, RowID: rowID, AttrID: cache["jobs.active"].AttributeID, ArrayIndices: "0", ValueNumeric: &active,
		}},
	}

	t.Run("schema metadata read", func(t *testing.T) {
		transformer := NewPersistentRecordTransformer(&secondCacheReadFailsRegistry{stubSchemaRegistry: base})

		_, err := transformer.FromPersistentRecord(ctx, record)
		require.Error(t, err)
		msg := err.Error()
		require.Contains(t, msg, "load schema metadata for schema 201 row "+rowID.String()+": ")
		require.Contains(t, msg, "registry read a second time within one rebuild")
		require.Equal(t, 1, strings.Count(msg, rowID.String()), "the row is rendered once")
	})

	t.Run("relation-root resolution", func(t *testing.T) {
		transformer := NewPersistentRecordTransformer(&failingSchemaByIDRegistry{stubSchemaRegistry: base})
		transformer.(RelationRootsAware).SetRelationRoots(visitLikeRelationRoots)

		_, err := transformer.FromPersistentRecord(ctx, record)
		require.Error(t, err)
		msg := err.Error()
		require.Contains(t, msg, "resolve relation roots for required-policy check of schema 201 row "+rowID.String()+": ")
		require.Contains(t, msg, "schema name lookup failed")
		require.Equal(t, 1, strings.Count(msg, rowID.String()), "the row is rendered once")
	})
}
