package federated

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/stretchr/testify/require"
)

// buildStampedSystemCols is a write-time footer stamp that satisfies the
// parquetcheck invariant — what a #256 exporter records in the manifest.
func buildStampedSystemCols() map[string]string {
	return map[string]string{"row_id": "UUID", "changed_at": "BIGINT", "deleted_at": "BIGINT", "ltbase_created_at": "BIGINT"}
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
// exists for: a pass feeds the manifest's footer columns to the validator, so
// a generation whose objects are all stamped validates and computes the #255
// cold-missing column set with ZERO DuckDB footer probes. V2 manifests stamp
// every object, so a published pass never probes a footer to validate it.
func TestResolveScanSourcesUsesStampsWithoutProbing(t *testing.T) {
	paths := []string{"s3://b/7/a.parquet", "s3://b/7/b.parquet"}
	// Only the second object's generation carries the "city" column; both
	// stamps satisfy the system-column invariant.
	secondCols := buildStampedSystemCols()
	secondCols["city"] = "INTEGER" // the column-bound integer export type; anything else would be pinned (#371)
	src := &fakeParquetSource{
		paths: paths,
		stamps: map[string]map[string]string{
			paths[0]: buildStampedSystemCols(),
			paths[1]: secondCols,
		},
	}
	duck := &fakeDuckDBExecutor{}
	e := NewDBFederatedQueryEngine(nil, duck, nil, forma.DuckDBConfig{},
		stampsTestMetadataCache(t), withParquetSource(src), withFakeHotLeg(nil))

	sc, err := e.resolveScanSources(context.Background(), model.StorageTables{}, exclusionTestQuery())

	require.NoError(t, err)
	require.Equal(t, paths, sc.paths)
	require.Equal(t, 0, duck.describeCalls,
		"a fully stamped path set must validate without a single footer probe")
	// The stamps did not merely skip the probes, they fed the footer union, and
	// that union is COMPLETE (an incomplete one may not drive #255 at all, so
	// cold.missing would be empty). "city" came from the second stamp and is
	// therefore not augmented; "age" appears in neither stamp and is.
	require.Len(t, sc.cold.missing, 1,
		"the stamped union must suppress augmentation of the column it reports")
	require.Equal(t, "age", sc.cold.missing[0].Name)
	require.Empty(t, sc.cold.pinned, "a healthy stamped set pins nothing (#371)")
}
