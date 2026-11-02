package transform

import (
	"context"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// boundPersistentAttr builds a registry attribute stored in a main-table column.
func boundPersistentAttr(id int16, valueType forma.ValueType, column forma.MainColumn, encoding forma.MainColumnEncoding) forma.AttributeMetadata {
	return forma.AttributeMetadata{
		AttributeID:   id,
		ValueType:     valueType,
		ColumnBinding: &forma.MainColumnBinding{ColumnName: column, Encoding: encoding},
	}
}

// unboundPersistentAttr builds a registry attribute stored as EAV rows.
func unboundPersistentAttr(id int16, valueType forma.ValueType) forma.AttributeMetadata {
	return forma.AttributeMetadata{AttributeID: id, ValueType: valueType}
}

func newPersistentTransformerRegistry() *stubSchemaRegistry {
	return &stubSchemaRegistry{
		schemaID:   201,
		schemaName: "persistent_test",
		cache: forma.SchemaAttributeCache{
			"name":               boundPersistentAttr(1, forma.ValueTypeText, forma.MainColumnText01, forma.MainColumnEncodingDefault),
			"age":                boundPersistentAttr(2, forma.ValueTypeNumeric, forma.MainColumnInteger01, forma.MainColumnEncodingDefault),
			"height":             boundPersistentAttr(3, forma.ValueTypeNumeric, forma.MainColumnDouble01, forma.MainColumnEncodingDefault),
			"createdAt":          boundPersistentAttr(4, forma.ValueTypeDate, forma.MainColumnBigint01, forma.MainColumnEncodingUnixMs),
			"isActiveInt":        boundPersistentAttr(5, forma.ValueTypeBool, forma.MainColumnSmallint01, forma.MainColumnEncodingBoolInt),
			"isActiveText":       boundPersistentAttr(6, forma.ValueTypeBool, forma.MainColumnText02, forma.MainColumnEncodingBoolText),
			"expiresAt":          boundPersistentAttr(7, forma.ValueTypeDate, forma.MainColumnText03, forma.MainColumnEncodingISO8601),
			"notes":              unboundPersistentAttr(8, forma.ValueTypeText),
			"profile.name":       boundPersistentAttr(9, forma.ValueTypeText, forma.MainColumnText04, forma.MainColumnEncodingDefault),
			"profile.rank":       boundPersistentAttr(10, forma.ValueTypeNumeric, forma.MainColumnSmallint02, forma.MainColumnEncodingDefault),
			"tags":               unboundPersistentAttr(11, forma.ValueTypeText),
			"jobs.title":         unboundPersistentAttr(12, forma.ValueTypeText),
			"jobs.active":        unboundPersistentAttr(13, forma.ValueTypeBool),
			"metadata.updatedAt": unboundPersistentAttr(14, forma.ValueTypeDate),
		},
	}
}

// persistentRoundTripTimes are the timestamps the round-trip payload carries;
// both directions of TestPersistentRecordTransformer_RoundTrip check them.
type persistentRoundTripTimes struct {
	createdAt, expiresAt, updatedAt time.Time
}

func newPersistentRoundTripTimes() persistentRoundTripTimes {
	createdAt := time.Date(2024, time.March, 14, 9, 26, 0, 0, time.UTC)
	return persistentRoundTripTimes{
		createdAt: createdAt,
		expiresAt: createdAt.Add(48 * time.Hour),
		updatedAt: createdAt.Add(24 * time.Hour),
	}
}

// payload exercises every storage shape of the registry: bound columns of
// each encoding, a nested object, a scalar array, an object array and an
// unbound date.
func (ts persistentRoundTripTimes) payload() map[string]any {
	return map[string]any{
		"name":         "Tester",
		"age":          33,
		"height":       172.5,
		"createdAt":    ts.createdAt,
		"isActiveInt":  true,
		"isActiveText": false,
		"expiresAt":    ts.expiresAt.Format(time.RFC3339),
		"notes":        "memo",
		"profile": map[string]any{
			"name": "Nested Name",
			"rank": 4,
		},
		"tags": []any{"gopher", "builder"},
		"jobs": []any{
			map[string]any{"title": "dev", "active": true},
			map[string]any{"title": "lead", "active": false},
		},
		"metadata": map[string]any{
			"updatedAt": ts.updatedAt,
		},
	}
}

func TestPersistentRecordTransformer_RoundTrip(t *testing.T) {
	ctx := context.Background()
	registry := newPersistentTransformerRegistry()
	transformer := NewPersistentRecordTransformer(registry)

	schemaID, _, err := registry.GetSchemaAttributeCacheByName("persistent_test")
	require.NoError(t, err)

	rowID := uuid.Must(uuid.NewV7())
	ts := newPersistentRoundTripTimes()

	start := time.Now().UnixMilli()
	record, err := transformer.ToPersistentRecord(ctx, schemaID, rowID, ts.payload())
	require.NoError(t, err)
	end := time.Now().UnixMilli()

	assert.Equal(t, schemaID, record.SchemaID)
	assert.Equal(t, rowID, record.RowID)
	assert.GreaterOrEqual(t, record.CreatedAt, start)
	assert.LessOrEqual(t, record.CreatedAt, end)
	assert.Equal(t, record.CreatedAt, record.UpdatedAt)

	assertPersistentMainColumns(t, record, ts)
	assertPersistentEAVRows(t, record)

	result, err := transformer.FromPersistentRecord(ctx, record)
	require.NoError(t, err)

	assertPersistentTopLevelAttributes(t, result, ts)
	assertPersistentNestedAttributes(t, result, ts)
}

// assertPersistentMainColumns checks the bound attributes landed in their
// main-table columns with their encodings applied.
func assertPersistentMainColumns(t *testing.T, record *model.PersistentRecord, ts persistentRoundTripTimes) {
	t.Helper()
	assert.Equal(t, map[string]string{
		"text_01": "Tester",
		"text_02": "0",
		"text_03": ts.expiresAt.Format(time.RFC3339),
		"text_04": "Nested Name",
	}, record.TextItems)
	assert.Equal(t, map[string]int16{
		"smallint_01": 1,
		"smallint_02": 4,
	}, record.Int16Items)
	assert.Equal(t, map[string]int32{
		"integer_01": 33,
	}, record.Int32Items)
	assert.Equal(t, map[string]int64{
		"bigint_01": ts.createdAt.UnixMilli(),
	}, record.Int64Items)
	assert.Equal(t, map[string]float64{
		"double_01": 172.5,
	}, record.Float64Items)
}

// assertPersistentEAVRows checks the unbound attributes became one EAV row
// per value, addressed by attribute ID and array indices.
func assertPersistentEAVRows(t *testing.T, record *model.PersistentRecord) {
	t.Helper()
	require.Len(t, record.OtherAttributes, 8)

	lookup := func(attrID int16, indices string) model.EAVRecord {
		for _, attr := range record.OtherAttributes {
			if attr.AttrID == attrID && attr.ArrayIndices == indices {
				return attr
			}
		}
		return model.EAVRecord{}
	}

	textRows := []struct {
		attrID  int16
		indices string
		want    string
	}{
		{attrID: 8, indices: "", want: "memo"},
		{attrID: 11, indices: "0", want: "gopher"},
		{attrID: 11, indices: "1", want: "builder"},
		{attrID: 12, indices: "0", want: "dev"},
		{attrID: 12, indices: "1", want: "lead"},
	}
	for _, row := range textRows {
		attr := lookup(row.attrID, row.indices)
		require.NotNil(t, attr.ValueText, "attr %d indices %q", row.attrID, row.indices)
		assert.Equal(t, row.want, *attr.ValueText, "attr %d indices %q", row.attrID, row.indices)
	}

	job0Active := lookup(13, "0")
	require.NotNil(t, job0Active.ValueNumeric)
	assert.True(t, (*job0Active.ValueNumeric) > 0.5)

	job1Active := lookup(13, "1")
	require.NotNil(t, job1Active.ValueNumeric)
	assert.False(t, (*job1Active.ValueNumeric) > 0.5)
}

// assertPersistentTopLevelAttributes checks the read-back scalars, bound and
// unbound alike, at the document root.
func assertPersistentTopLevelAttributes(t *testing.T, result map[string]any, ts persistentRoundTripTimes) {
	t.Helper()
	assert.Equal(t, "Tester", result["name"])
	assert.Equal(t, float64(33), result["age"])
	assert.Equal(t, float64(172.5), result["height"])

	gotCreatedAt, ok := result["createdAt"].(time.Time)
	require.True(t, ok)
	assert.True(t, ts.createdAt.Equal(gotCreatedAt))

	gotExpiresAt, ok := result["expiresAt"].(time.Time)
	require.True(t, ok)
	assert.True(t, ts.expiresAt.Equal(gotExpiresAt))

	assert.Equal(t, true, result["isActiveInt"])
	assert.Equal(t, false, result["isActiveText"])
	assert.Equal(t, "memo", result["notes"])
}

// assertPersistentNestedAttributes checks the read-back rebuilt the nested
// object, both arrays and the unbound nested date.
func assertPersistentNestedAttributes(t *testing.T, result map[string]any, ts persistentRoundTripTimes) {
	t.Helper()
	profile, ok := result["profile"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Nested Name", profile["name"])
	assert.Equal(t, float64(4), profile["rank"])

	tags, ok := result["tags"].([]any)
	require.True(t, ok)
	assert.Equal(t, []any{"gopher", "builder"}, tags)

	jobs, ok := result["jobs"].([]any)
	require.True(t, ok)
	require.Len(t, jobs, 2)
	firstJob, ok := jobs[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "dev", firstJob["title"])
	assert.Equal(t, true, firstJob["active"])
	secondJob, ok := jobs[1].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "lead", secondJob["title"])
	assert.Equal(t, false, secondJob["active"])

	metadata, ok := result["metadata"].(map[string]any)
	require.True(t, ok)
	gotUpdated, ok := metadata["updatedAt"].(time.Time)
	require.True(t, ok)
	assert.WithinDuration(t, ts.updatedAt, gotUpdated, time.Millisecond)
}

func TestPersistentRecordTransformer_NilInputs(t *testing.T) {
	ctx := context.Background()
	registry := newPersistentTransformerRegistry()
	transformer := NewPersistentRecordTransformer(registry)

	schemaID, _, err := registry.GetSchemaAttributeCacheByName("persistent_test")
	require.NoError(t, err)

	_, err = transformer.ToPersistentRecord(ctx, schemaID, uuid.Must(uuid.NewV7()), nil)
	require.Error(t, err)

	_, err = transformer.FromPersistentRecord(ctx, nil)
	require.Error(t, err)
}

func TestPersistentRecordTransformer_InjectsBaseTimestamps(t *testing.T) {
	ctx := context.Background()
	schemaID := int16(301)
	registry := &stubSchemaRegistry{
		schemaID:   schemaID,
		schemaName: "ts_schema",
		cache: forma.SchemaAttributeCache{
			"createdAt": {AttributeID: 1, ValueType: forma.ValueTypeDate, ColumnBinding: &forma.MainColumnBinding{
				ColumnName: forma.MainColumnCreatedAt,
				Encoding:   forma.MainColumnEncodingUnixMs,
			}},
			"updatedAt": {AttributeID: 2, ValueType: forma.ValueTypeDate, ColumnBinding: &forma.MainColumnBinding{
				ColumnName: forma.MainColumnUpdatedAt,
				Encoding:   forma.MainColumnEncodingUnixMs,
			}},
		},
	}

	transformer := NewPersistentRecordTransformer(registry)

	created := time.Date(2024, time.January, 2, 15, 4, 5, 0, time.UTC).UnixMilli()
	updated := created + int64(time.Minute/time.Millisecond)
	record := &model.PersistentRecord{
		SchemaID:  schemaID,
		RowID:     uuid.Must(uuid.NewV7()),
		CreatedAt: created,
		UpdatedAt: updated,
	}

	attrs, err := transformer.FromPersistentRecord(ctx, record)
	require.NoError(t, err)

	createdAt, ok := attrs["createdAt"].(time.Time)
	require.True(t, ok)
	assert.Equal(t, created, createdAt.UnixMilli())

	updatedAt, ok := attrs["updatedAt"].(time.Time)
	require.True(t, ok)
	assert.Equal(t, updated, updatedAt.UnixMilli())
}

// #459 shape 1 end to end: an out-of-range value for a narrow bound column
// is refused by ToPersistentRecord as published invalid input and never
// reaches Int32Items wrapped.
func TestPersistentRecordTransformer_RejectsOutOfRangeBoundColumn(t *testing.T) {
	ctx := context.Background()
	registry := newPersistentTransformerRegistry()
	transformer := NewPersistentRecordTransformer(registry)
	schemaID, _, err := registry.GetSchemaAttributeCacheByName("persistent_test")
	require.NoError(t, err)

	_, err = transformer.ToPersistentRecord(ctx, schemaID, uuid.Must(uuid.NewV7()), map[string]any{
		"name": "Tester",
		"age":  float64(3e9),
	})
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	msg, ok := forma.ResolvePublicMessage(err)
	require.True(t, ok)
	assert.Contains(t, msg, "attribute 'age'")
	assert.Contains(t, msg, "bound column integer_01 (integer)")
}
