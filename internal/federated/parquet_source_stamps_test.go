package federated

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/stretchr/testify/require"
)

// stampedSystemCols is a write-time footer stamp that satisfies the
// parquetcheck invariant — what a #256 exporter records in the manifest.
func stampedSystemCols() map[string]string {
	return map[string]string{"row_id": "UUID", "changed_at": "BIGINT", "deleted_at": "BIGINT"}
}

// stampsTestMetadataCache registers schema 7 with two attributes so the #255
// cold-missing computation can discriminate: "city" is a column the stamps
// report, "age" is one no scanned generation carries yet.
func stampsTestMetadataCache(t *testing.T) *schemameta.MetadataCache {
	t.Helper()
	mc := schemameta.NewMetadataCache()
	require.NoError(t, mc.RegisterSchema("test", 7, forma.SchemaAttributeCache{
		"age": {
			AttributeID:   6,
			ValueType:     forma.ValueTypeInteger,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnInteger01},
		},
		"city": {
			AttributeID:   7,
			ValueType:     forma.ValueTypeInteger,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnInteger02},
		},
	}))
	return mc
}

// TestResolveScanSourcesUsesStampsWithoutProbing is the cold-start win #256
// exists for: resolveScanSources feeds the source-authored stamps to the
// validator, so a fully stamped path set validates and computes the #255
// cold-missing column set with ZERO DuckDB footer probes. Without the
// plumbing every listed object costs one DESCRIBE on the first query.
func TestResolveScanSourcesUsesStampsWithoutProbing(t *testing.T) {
	paths := []string{"s3://b/7/a.parquet", "s3://b/7/b.parquet"}
	// Only the second object's generation carries the "city" column; both
	// stamps satisfy the system-column invariant.
	secondCols := stampedSystemCols()
	secondCols["city"] = "BIGINT"
	src := &fakeParquetSource{
		paths: paths,
		stamps: map[string]map[string]string{
			paths[0]: stampedSystemCols(),
			paths[1]: secondCols,
		},
	}
	duck := &fakeDuckDBExecutor{}
	e := NewDBFederatedQueryEngine(nil, nil, duck, nil, forma.DuckDBConfig{},
		stampsTestMetadataCache(t), "", WithParquetSource(src))

	sc, err := e.resolveScanSources(context.Background(), exclusionTestQuery())

	require.NoError(t, err)
	require.Equal(t, paths, sc.paths)
	require.True(t, sc.fromSource)
	require.Equal(t, 0, duck.describeCalls,
		"a fully stamped path set must validate without a single footer probe")
	// The stamps did not merely skip the probes, they fed the footer union, and
	// that union is COMPLETE (an incomplete one may not drive #255 at all, so
	// coldMissing would be empty). "city" came from the second stamp and is
	// therefore not augmented; "age" appears in neither stamp and is.
	require.Len(t, sc.coldMissing, 1,
		"the stamped union must suppress augmentation of the column it reports")
	require.Equal(t, "age", sc.coldMissing[0].Name)
}

// TestResolveScanSourcesHintPathsHaveNoStamps: hint-authored paths never pass
// through the manifest source, so they carry no stamps and probe exactly as
// today. The source here holds a stamp for the very path the hint names —
// leaking it into the validator would skip a probe the operator-pinned set
// has no manifest evidence for.
func TestResolveScanSourcesHintPathsHaveNoStamps(t *testing.T) {
	const hinted = "s3://hinted/x.parquet"
	src := &fakeParquetSource{
		paths:  []string{"s3://b/7/a.parquet"},
		stamps: map[string]map[string]string{hinted: stampedSystemCols()},
	}
	duck := &fakeDuckDBExecutor{}
	e := NewDBFederatedQueryEngine(nil, nil, duck, nil, forma.DuckDBConfig{},
		testMetadataCacheSchema7(t), "", WithParquetSource(src))

	q := exclusionTestQuery()
	q.DuckDBHints = &model.DuckDBRenderHints{S3ParquetPathTemplate: hinted}
	sc, err := e.resolveScanSources(context.Background(), q)

	require.NoError(t, err)
	require.Equal(t, []string{hinted}, sc.paths)
	require.False(t, sc.fromSource)
	require.Equal(t, 0, src.pathsCalls, "an explicit hint must not consult the source")
	require.Equal(t, 1, duck.describeCalls, "hint-authored paths probe as today")
}
