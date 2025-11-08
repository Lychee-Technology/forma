package internal

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newPersistentTransformerRegistry() *stubSchemaRegistry {
	return &stubSchemaRegistry{
		schemaID:   201,
		schemaName: "persistent_test",
		cache: SchemaAttributeCache{
			"name": {
				AttributeID: 1,
				ValueType:   ValueTypeText,
				Storage: &AttributeStorageMetadata{
					Location: AttributeStorageLocationMain,
					ColumnBinding: &MainColumnBinding{
						ColumnName: MainColumnText01,
						Encoding:   MainColumnEncodingDefault,
					},
					Fallback: AttributeFallbackMetadata{Kind: AttributeFallbackKindNone},
				},
			},
			"age": {
				AttributeID: 2,
				ValueType:   ValueTypeNumeric,
				Storage: &AttributeStorageMetadata{
					Location: AttributeStorageLocationMain,
					ColumnBinding: &MainColumnBinding{
						ColumnName: MainColumnInteger01,
						Encoding:   MainColumnEncodingDefault,
					},
					Fallback: AttributeFallbackMetadata{Kind: AttributeFallbackKindNone},
				},
			},
			"height": {
				AttributeID: 3,
				ValueType:   ValueTypeNumeric,
				Storage: &AttributeStorageMetadata{
					Location: AttributeStorageLocationMain,
					ColumnBinding: &MainColumnBinding{
						ColumnName: MainColumnDouble01,
						Encoding:   MainColumnEncodingDefault,
					},
					Fallback: AttributeFallbackMetadata{Kind: AttributeFallbackKindNone},
				},
			},
			"createdAt": {
				AttributeID: 4,
				ValueType:   ValueTypeDate,
				Storage: &AttributeStorageMetadata{
					Location: AttributeStorageLocationMain,
					ColumnBinding: &MainColumnBinding{
						ColumnName: MainColumnBigint01,
						Encoding:   MainColumnEncodingUnixMs,
					},
					Fallback: AttributeFallbackMetadata{Kind: AttributeFallbackKindNone},
				},
			},
			"isActiveInt": {
				AttributeID: 5,
				ValueType:   ValueTypeBool,
				Storage: &AttributeStorageMetadata{
					Location: AttributeStorageLocationMain,
					ColumnBinding: &MainColumnBinding{
						ColumnName: MainColumnSmallint01,
						Encoding:   MainColumnEncodingBoolInt,
					},
					Fallback: AttributeFallbackMetadata{Kind: AttributeFallbackKindNone},
				},
			},
			"isActiveText": {
				AttributeID: 6,
				ValueType:   ValueTypeBool,
				Storage: &AttributeStorageMetadata{
					Location: AttributeStorageLocationMain,
					ColumnBinding: &MainColumnBinding{
						ColumnName: MainColumnText02,
						Encoding:   MainColumnEncodingBoolText,
					},
					Fallback: AttributeFallbackMetadata{Kind: AttributeFallbackKindNone},
				},
			},
			"expiresAt": {
				AttributeID: 7,
				ValueType:   ValueTypeDate,
				Storage: &AttributeStorageMetadata{
					Location: AttributeStorageLocationMain,
					ColumnBinding: &MainColumnBinding{
						ColumnName: MainColumnText03,
						Encoding:   MainColumnEncodingISO8601,
					},
					Fallback: AttributeFallbackMetadata{Kind: AttributeFallbackKindNone},
				},
			},
			"notes": {
				AttributeID: 8,
				ValueType:   ValueTypeText,
				Storage: &AttributeStorageMetadata{
					Location:      AttributeStorageLocationEAV,
					ColumnBinding: nil,
					Fallback:      AttributeFallbackMetadata{Kind: AttributeFallbackKindNone},
				},
			},
			"profile.name": {
				AttributeID: 9,
				ValueType:   ValueTypeText,
				Storage: &AttributeStorageMetadata{
					Location: AttributeStorageLocationMain,
					ColumnBinding: &MainColumnBinding{
						ColumnName: MainColumnText04,
						Encoding:   MainColumnEncodingDefault,
					},
					Fallback: AttributeFallbackMetadata{Kind: AttributeFallbackKindNone},
				},
			},
			"profile.rank": {
				AttributeID: 10,
				ValueType:   ValueTypeNumeric,
				Storage: &AttributeStorageMetadata{
					Location: AttributeStorageLocationMain,
					ColumnBinding: &MainColumnBinding{
						ColumnName: MainColumnSmallint02,
						Encoding:   MainColumnEncodingDefault,
					},
					Fallback: AttributeFallbackMetadata{Kind: AttributeFallbackKindNone},
				},
			},
			"tags": {
				AttributeID: 11,
				ValueType:   ValueTypeText,
				Storage: &AttributeStorageMetadata{
					Location:      AttributeStorageLocationEAV,
					ColumnBinding: nil,
					Fallback:      AttributeFallbackMetadata{Kind: AttributeFallbackKindNone},
				},
			},
			"jobs.title": {
				AttributeID: 12,
				ValueType:   ValueTypeText,
				Storage: &AttributeStorageMetadata{
					Location:      AttributeStorageLocationEAV,
					ColumnBinding: nil,
					Fallback:      AttributeFallbackMetadata{Kind: AttributeFallbackKindNone},
				},
			},
			"jobs.active": {
				AttributeID: 13,
				ValueType:   ValueTypeBool,
				Storage: &AttributeStorageMetadata{
					Location:      AttributeStorageLocationEAV,
					ColumnBinding: nil,
					Fallback:      AttributeFallbackMetadata{Kind: AttributeFallbackKindNone},
				},
			},
			"metadata.updatedAt": {
				AttributeID: 14,
				ValueType:   ValueTypeDate,
				Storage: &AttributeStorageMetadata{
					Location:      AttributeStorageLocationEAV,
					ColumnBinding: nil,
					Fallback:      AttributeFallbackMetadata{Kind: AttributeFallbackKindNone},
				},
			},
		},
	}
}

func TestPersistentRecordTransformer_RoundTrip(t *testing.T) {
	ctx := context.Background()
	registry := newPersistentTransformerRegistry()
	transformer := NewPersistentRecordTransformer(registry)

	schemaID, _, err := registry.GetSchemaByName("persistent_test")
	require.NoError(t, err)

	rowID := uuid.Must(uuid.NewV7())
	createdAt := time.Date(2024, time.March, 14, 9, 26, 0, 0, time.UTC)
	expiresAt := createdAt.Add(48 * time.Hour)
	updatedAt := createdAt.Add(24 * time.Hour)

	payload := map[string]any{
		"name":         "Tester",
		"age":          33,
		"height":       172.5,
		"createdAt":    createdAt,
		"isActiveInt":  true,
		"isActiveText": false,
		"expiresAt":    expiresAt.Format(time.RFC3339),
		"notes":        "memo",
		"profile": map[string]any{
			"name": "Nested Name",
			"rank": 4,
		},
		"tags": []any{"gopher", "builder"},
		"jobs": []any{
			map[string]any{
				"title":  "dev",
				"active": true,
			},
			map[string]any{
				"title":  "lead",
				"active": false,
			},
		},
		"metadata": map[string]any{
			"updatedAt": updatedAt,
		},
	}

	start := time.Now().UnixMilli()
	record, err := transformer.ToPersistentRecord(ctx, schemaID, rowID, payload)
	require.NoError(t, err)
	end := time.Now().UnixMilli()

	assert.Equal(t, schemaID, record.SchemaID)
	assert.Equal(t, rowID, record.RowID)
	assert.GreaterOrEqual(t, record.CreatedAt, start)
	assert.LessOrEqual(t, record.CreatedAt, end)
	assert.Equal(t, record.CreatedAt, record.UpdatedAt)

	assert.Equal(t, map[string]string{
		"text_01": "Tester",
		"text_02": "0",
		"text_03": expiresAt.Format(time.RFC3339),
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
		"bigint_01": createdAt.UnixMilli(),
	}, record.Int64Items)
	assert.Equal(t, map[string]float64{
		"double_01": 172.5,
	}, record.Float64Items)

	require.Len(t, record.OtherAttributes, 8)

	lookup := func(attrID int16, indices string) EAVRecord {
		for _, attr := range record.OtherAttributes {
			if attr.AttrID == attrID && attr.ArrayIndices == indices {
				return attr
			}
		}
		return EAVRecord{}
	}

	notesAttr := lookup(8, "")
	require.NotNil(t, notesAttr.ValueText)
	assert.Equal(t, "memo", *notesAttr.ValueText)

	tag0 := lookup(11, "0")
	require.NotNil(t, tag0.ValueText)
	assert.Equal(t, "gopher", *tag0.ValueText)

	tag1 := lookup(11, "1")
	require.NotNil(t, tag1.ValueText)
	assert.Equal(t, "builder", *tag1.ValueText)

	job0Title := lookup(12, "0")
	require.NotNil(t, job0Title.ValueText)
	assert.Equal(t, "dev", *job0Title.ValueText)

	job1Title := lookup(12, "1")
	require.NotNil(t, job1Title.ValueText)
	assert.Equal(t, "lead", *job1Title.ValueText)

	job0Active := lookup(13, "0")
	require.NotNil(t, job0Active.ValueNumeric)
	assert.True(t, (*job0Active.ValueNumeric) > 0.5)

	job1Active := lookup(13, "1")
	require.NotNil(t, job1Active.ValueNumeric)
	assert.False(t, (*job1Active.ValueNumeric) > 0.5)

	result, err := transformer.FromPersistentRecord(ctx, record)
	require.NoError(t, err)

	assert.Equal(t, "Tester", result["name"])
	assert.Equal(t, float64(33), result["age"])
	assert.Equal(t, float64(172.5), result["height"])

	gotCreatedAt, ok := result["createdAt"].(time.Time)
	require.True(t, ok)
	assert.True(t, createdAt.Equal(gotCreatedAt))

	gotExpiresAt, ok := result["expiresAt"].(time.Time)
	require.True(t, ok)
	assert.True(t, expiresAt.Equal(gotExpiresAt))

	assert.Equal(t, true, result["isActiveInt"])
	assert.Equal(t, false, result["isActiveText"])
	assert.Equal(t, "memo", result["notes"])

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
	assert.WithinDuration(t, updatedAt, gotUpdated, time.Millisecond)
}

func TestPersistentRecordTransformer_NilInputs(t *testing.T) {
	ctx := context.Background()
	registry := newPersistentTransformerRegistry()
	transformer := NewPersistentRecordTransformer(registry)

	schemaID, _, err := registry.GetSchemaByName("persistent_test")
	require.NoError(t, err)

	_, err = transformer.ToPersistentRecord(ctx, schemaID, uuid.Must(uuid.NewV7()), nil)
	require.Error(t, err)

	_, err = transformer.FromPersistentRecord(ctx, nil)
	require.Error(t, err)
}
