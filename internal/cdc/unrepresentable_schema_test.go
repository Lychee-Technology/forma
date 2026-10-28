package cdc

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// objectArrayAttrCache is testAttrCache plus a member of the array of
// objects items, which the parquet tiers cannot hold (#704).
func objectArrayAttrCache() forma.SchemaAttributeCache {
	cache := testAttrCache()
	cache["items.sku"] = forma.AttributeMetadata{
		AttributeName: "items.sku", AttributeID: 12, ValueType: forma.ValueTypeText, ArrayAncestors: []string{"items"},
	}
	return cache
}

// perIDSchemaRegistry answers each schema ID's own cache.
type perIDSchemaRegistry struct {
	stubSchemaRegistry
	caches map[int16]forma.SchemaAttributeCache
}

func (r perIDSchemaRegistry) GetSchemaAttributeCacheByID(id int16) (string, forma.SchemaAttributeCache, error) {
	return "", r.caches[id], nil
}

func objectArrayRegistry() perIDSchemaRegistry {
	return perIDSchemaRegistry{caches: map[int16]forma.SchemaAttributeCache{
		7: testAttrCache(), 8: objectArrayAttrCache(), 9: testAttrCache(),
	}}
}

// assertSkipWarning: the run logged one warning naming schema 8 and the
// attribute that keeps it out of the parquet tiers.
func assertSkipWarning(t *testing.T, logs *observer.ObservedLogs) {
	t.Helper()
	entries := logs.FilterMessageSnippet("schema not exported").All()
	require.Len(t, entries, 1)
	require.Equal(t, zapcore.WarnLevel, entries[0].Level)
	fields := entries[0].ContextMap()
	require.EqualValues(t, 8, fields["schema_id"])
	require.Contains(t, fields["error"], `"items.sku" lies inside the array of objects "items"`)
}

// TestProcessSchemas_SkipsASchemaTheParquetTiersCannotHold pins #704 on the
// flush: the schema holding an array of objects is neither exported nor
// marked flushed, its neighbours flush, and the run does not fail.
func TestProcessSchemas_SkipsASchemaTheParquetTiersCannotHold(t *testing.T) {
	core, logs := observer.New(zapcore.InfoLevel)
	var processed []int16
	flushCtx := &schemaFlushContext{
		logger:         zap.New(core),
		schemaRegistry: objectArrayRegistry(),
		processSchemaFn: func(_ context.Context, schemaID int16) error {
			processed = append(processed, schemaID)
			return nil
		},
	}

	require.NoError(t, flushCtx.processSchemas(context.Background(), []int64{7, 8, 9}))
	require.Equal(t, []int16{7, 9}, processed)
	assertSkipWarning(t, logs)
}

// TestProcessInitSchemas_SkipsASchemaTheParquetTiersCannotHold is the
// cdc-init leg of the same skip.
func TestProcessInitSchemas_SkipsASchemaTheParquetTiersCannotHold(t *testing.T) {
	core, logs := observer.New(zapcore.InfoLevel)
	var initialized []int16
	runCtx := &initRunContext{
		logger:         zap.New(core),
		schemaRegistry: objectArrayRegistry(),
		locker: SchemaLockerFunc(func(ctx context.Context, _ int16) (context.Context, func(), bool, error) {
			return ctx, func() {}, true, nil
		}),
		initSchemaFn: func(_ context.Context, _ *initRunContext, sid int16) (int64, int, error) {
			initialized = append(initialized, sid)
			return 1, 1, nil
		},
	}

	summary, err := processInitSchemas(context.Background(), runCtx, []int64{7, 8, 9})
	require.NoError(t, err)
	require.Equal(t, []int16{7, 9}, initialized)
	require.Equal(t, InitSummary{TotalRowsExported: 2, TotalFilesCreated: 2}, summary)
	assertSkipWarning(t, logs)
}

// TestBuildSchemaDrivenProjection_RefusesAnObjectArrayMember: an export
// built for such a schema by a caller that skipped the run's filter is
// refused before any side effect.
func TestBuildSchemaDrivenProjection_RefusesAnObjectArrayMember(t *testing.T) {
	_, err := buildSchemaDrivenProjection(objectArrayAttrCache())
	require.ErrorContains(t, err, `refusing to export: attribute "items.sku" lies inside the array of objects "items"`)
}
