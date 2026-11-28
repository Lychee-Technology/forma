package cdc

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// underivedRegistry answers schema 7 with the flat testAttrCache and 8, 9 and
// 10 with the file registry's caches of the three schemas of
// internal/testdata/underived_ancestry whose array ancestry it cannot derive
// (#937), each beside the array of objects items. It returns the attribute
// each of those schemas leaves unplaced.
func underivedRegistry(t *testing.T) (perIDSchemaRegistry, map[int16]string) {
	t.Helper()
	registry, err := schemameta.NewFileSchemaRegistryFromDirectory("../testdata/underived_ancestry")
	require.NoError(t, err)
	caches := map[int16]forma.SchemaAttributeCache{7: testAttrCache()}
	unplaced := map[int16]string{8: "choice.kind", 9: "matrix.cell", 10: "contact.phone"}
	for sid, schema := range map[int16]string{8: "ua_union", 9: "ua_nested", 10: "ua_ref"} {
		_, cache, err := registry.GetSchemaAttributeCacheByName(schema)
		require.NoError(t, err, schema)
		caches[sid] = cache
	}
	return perIDSchemaRegistry{caches: caches}, unplaced
}

// assertUnderivedSkips: the run reported schemas 8, 9 and 10 as skipped,
// each naming the attribute the registry could not place, and logged one
// warning for each.
func assertUnderivedSkips(t *testing.T, skipped []SkippedSchema, logs *observer.ObservedLogs, unplaced map[int16]string) {
	t.Helper()
	require.Len(t, skipped, len(unplaced))
	for _, s := range skipped {
		require.Contains(t, s.Reason, `could not place attribute "`+unplaced[s.SchemaID]+`"`, "schema %d", s.SchemaID)
	}
	require.Len(t, logs.FilterMessageSnippet("schema not exported").All(), len(unplaced))
}

// TestProcessSchemas_SkipsASchemaWhoseAncestryWasNotDerived pins #937 on the
// flush: a schema the registry could not derive ancestry for is neither
// exported nor marked flushed, like one holding a derived array of objects
// (#704), and the flat neighbour still flushes.
func TestProcessSchemas_SkipsASchemaWhoseAncestryWasNotDerived(t *testing.T) {
	registry, unplaced := underivedRegistry(t)
	core, logs := observer.New(zapcore.InfoLevel)
	var processed []int16
	flushCtx := &schemaFlushContext{
		logger:         zap.New(core),
		schemaRegistry: registry,
		processSchemaFn: func(_ context.Context, schemaID int16) error {
			processed = append(processed, schemaID)
			return nil
		},
	}

	report, err := flushCtx.processSchemas(context.Background(), []int64{7, 8, 9, 10})
	require.NoError(t, err)
	require.Equal(t, []int16{7}, processed)
	assertUnderivedSkips(t, report.Skipped, logs, unplaced)
}

// TestProcessInitSchemas_SkipsASchemaWhoseAncestryWasNotDerived is the
// cdc-init leg of the same skip, and a run whose --schema-id names such a
// schema is refused with ErrSchemaNotExportable.
func TestProcessInitSchemas_SkipsASchemaWhoseAncestryWasNotDerived(t *testing.T) {
	registry, unplaced := underivedRegistry(t)
	core, logs := observer.New(zapcore.InfoLevel)
	var initialized []int16
	runCtx := &initRunContext{
		logger:         zap.New(core),
		schemaRegistry: registry,
		locker: SchemaLockerFunc(func(ctx context.Context, _ int16) (context.Context, func(), bool, error) {
			return ctx, func() {}, true, nil
		}),
		initSchemaFn: func(_ context.Context, _ *initRunContext, sid int16) (SchemaInit, error) {
			initialized = append(initialized, sid)
			return SchemaInit{SchemaID: sid, Rows: 1, Files: 1}, nil
		},
	}

	summary, err := processInitSchemas(context.Background(), runCtx, []int64{7, 8, 9, 10})
	require.NoError(t, err)
	require.Equal(t, []int16{7}, initialized)
	assertUnderivedSkips(t, summary.Skipped, logs, unplaced)

	err = refuseNamedSkippedSchema(9, summary.Skipped[1:2])
	require.ErrorIs(t, err, ErrSchemaNotExportable)
	require.ErrorContains(t, err, `could not place attribute "matrix.cell"`)
}

// TestBuildSchemaDrivenProjection_RefusesAnUnderivedSchema: an export built
// for such a schema by a caller that bypassed the run's filter is refused
// before any side effect.
func TestBuildSchemaDrivenProjection_RefusesAnUnderivedSchema(t *testing.T) {
	registry, _ := underivedRegistry(t)
	_, err := buildSchemaDrivenProjection(registry.caches[10])
	require.ErrorContains(t, err, `refusing to export: the schema registry could not place attribute "contact.phone"`)
}
