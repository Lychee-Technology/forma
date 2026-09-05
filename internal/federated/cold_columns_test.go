package federated

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/queryplan"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/stretchr/testify/require"
)

func testUnion(types map[string]string, mixed ...string) columnUnion {
	u := newColumnUnion()
	for k, v := range types {
		u.types[k] = v
	}
	for _, m := range mixed {
		u.mixed[m] = struct{}{}
	}
	return u
}

func TestColdScanColumnsDetectsAbsentAttributes(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"name":  {AttributeID: 1, ValueType: forma.ValueTypeText},
		"score": {AttributeID: 3, ValueType: forma.ValueTypeInteger},
		"tags":  {AttributeID: 4, ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeBigInt},
	}
	union := testUnion(map[string]string{
		"row_id": "UUID", "changed_at": "BIGINT", "deleted_at": "BIGINT", "ltbase_created_at": "BIGINT",
		"name": "VARCHAR",
	})
	got := coldScanColumns(cache, union)
	require.Equal(t, []sqlgen.ScanColumn{
		// EAV-only integer augments at storage width DOUBLE (#384).
		{Name: "score", DuckDBType: "DOUBLE"},
		{Name: "tags", DuckDBType: "BIGINT[]"},
	}, got.missing, "absent attrs detected, present ones skipped, sorted by name")
	require.Empty(t, got.pinned, "a present column at the expected type is not pinned")
}

func TestColdScanColumnsEmptyOnUnknownUnionOrEmptyCache(t *testing.T) {
	cache := forma.SchemaAttributeCache{"score": {AttributeID: 3, ValueType: forma.ValueTypeInteger}}
	require.True(t, coldScanColumns(cache, columnUnion{}).empty(), "unknown union must neither augment nor pin")
	require.True(t, coldScanColumns(nil, newColumnUnion()).empty(), "no metadata, nothing to augment or pin")
}

// Dotted attribute names must probe the FOLDED parquet column (#260), not
// the raw name — otherwise a flushed dotted attribute reads as missing and
// gets shadow-augmented.
func TestColdScanColumnsUsesFoldedParquetColumnNames(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"user.name": {AttributeID: 7, ValueType: forma.ValueTypeText},
	}
	union := testUnion(map[string]string{sqlgen.ParquetAttrColumn("user.name"): "VARCHAR"})
	require.True(t, coldScanColumns(cache, union).empty())
}

// #371: a column whose first-seen parquet type disagrees with the schema's
// scan type (the quiet INTEGER→VARCHAR drift a stale delta generation
// leaves behind), or that differs between files, is pinned at the schema
// type. Columns at the expected type, and the VARCHAR/UUID dual encoding of
// #147, are left alone so a healthy scan set renders no CAST.
func TestColdScanColumnsPinsDriftedAndMixedTypes(t *testing.T) {
	bound := &forma.MainColumnBinding{ColumnName: forma.MainColumn("integer_01")}
	cache := forma.SchemaAttributeCache{
		"score":  {AttributeID: 1, ValueType: forma.ValueTypeInteger, ColumnBinding: bound},
		"amount": {AttributeID: 2, ValueType: forma.ValueTypeNumeric},
		"name":   {AttributeID: 3, ValueType: forma.ValueTypeText},
		"owner":  {AttributeID: 4, ValueType: forma.ValueTypeUUID, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("uuid_01")}},
		"ref":    {AttributeID: 5, ValueType: forma.ValueTypeUUID},
	}
	union := testUnion(map[string]string{
		"score":  "VARCHAR", // stale generation: drifted, first-seen wins the union
		"amount": "DOUBLE",  // expected type, but seen at two types across the set
		"name":   "VARCHAR", // healthy
		"owner":  "UUID",    // column-bound uuid exports as parquet UUID (#147)
		"ref":    "VARCHAR", // EAV uuid exports as VARCHAR (#147)
	}, "amount")
	got := coldScanColumns(cache, union)
	require.Empty(t, got.missing)
	require.Equal(t, []sqlgen.ScanColumn{
		{Name: "amount", DuckDBType: "DOUBLE"},
		{Name: "score", DuckDBType: "INTEGER"},
	}, got.pinned, "drifted and mixed columns are pinned at the schema scan type, sorted by name")
}

// #255 plan-cache poisoning guard: the missing set participates in the
// scope hash, so the same query shape over the same path STRING (a glob
// hint does not change on flush) re-keys when the first flush lands the
// column.
func TestDuckPlanScopePartsIncludeColdMissingSet(t *testing.T) {
	tables := model.StorageTables{EAVData: "eav_data", EntityMain: "entity_main", ChangeLog: "change_log"}
	paths := []string{"s3://b/schema/1/**/*.parquet"}
	absent := duckPlanScopeParts(tables, "conn", 10, 0, false, paths, nil,
		coldScanSet{missing: []sqlgen.ScanColumn{{Name: "score", DuckDBType: "INTEGER"}}})
	present := duckPlanScopeParts(tables, "conn", 10, 0, false, paths, nil, coldScanSet{})
	require.NotEqual(t,
		queryplan.HashScopeParts(absent...),
		queryplan.HashScopeParts(present...),
		"cold-absent and cold-present shapes must occupy different plan-cache entries")
}

// #371: the pinned set is its own scope component. The same column in the
// missing role and in the pinned role must not share a skeleton (one
// projects NULL, the other CASTs a real column), and a pinned shape must
// not be served from the skeleton compiled before the stale generation was
// purged, nor the other way round.
func TestDuckPlanScopePartsIncludeColdPinnedSet(t *testing.T) {
	tables := model.StorageTables{EAVData: "eav_data", EntityMain: "entity_main", ChangeLog: "change_log"}
	paths := []string{"s3://b/schema/1/**/*.parquet"}
	col := []sqlgen.ScanColumn{{Name: "score", DuckDBType: "INTEGER"}}
	pinned := queryplan.HashScopeParts(duckPlanScopeParts(tables, "conn", 10, 0, false, paths, nil, coldScanSet{pinned: col})...)
	missing := queryplan.HashScopeParts(duckPlanScopeParts(tables, "conn", 10, 0, false, paths, nil, coldScanSet{missing: col})...)
	healthy := queryplan.HashScopeParts(duckPlanScopeParts(tables, "conn", 10, 0, false, paths, nil, coldScanSet{})...)
	require.NotEqual(t, pinned, healthy, "pinned and healthy shapes must occupy different plan-cache entries")
	require.NotEqual(t, pinned, missing, "the missing and pinned roles of one column must not alias")
}

// #371 review P2, pinned as the trust boundary rather than fixed: the pin
// consumes the validator's union, and a stamp that passes the system-column
// invariant feeds that union with ZERO footer probes (#256). So the stamp's
// attribute types decide whether a column is pinned — a stamp that reports
// the drift earns the CAST, and a stamp that reports the export type earns
// none, whatever the bytes at the path say. A stamp that misreports the
// bytes is a write-path fault (a same-key rewrite under an unchanged
// stamp, which no current writer performs — #416 minted write-once keys),
// detected offline by `manifest-reconcile --verify-stamps`.
func TestColdScanPinFollowsTrustedStampWithoutProbe(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"score": {AttributeID: 1, ValueType: forma.ValueTypeInteger, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("integer_01")}},
	}
	for stampType, wantPinned := range map[string]bool{"VARCHAR": true, "INTEGER": false} {
		exec := &scriptedDescribeExecutor{} // any probe would fail loudly here
		v := newParquetSchemaValidator()
		stamps := map[string]map[string]string{
			"s3://b/1/a.parquet": stampCols(append(buildValidSystemCols(), [2]string{"score", stampType})),
		}
		union, complete, err := v.Validate(context.Background(), exec, []string{"s3://b/1/a.parquet"}, stamps)
		require.NoError(t, err)
		require.True(t, complete)
		require.Empty(t, exec.probes, "stamp %s: the pin is decided without touching the footer", stampType)

		got := coldScanColumns(cache, union)
		require.Empty(t, got.missing)
		if wantPinned {
			require.Equal(t, []sqlgen.ScanColumn{{Name: "score", DuckDBType: "INTEGER"}}, got.pinned, "a stamp reporting the drift earns the CAST")
		} else {
			require.Empty(t, got.pinned, "a stamp reporting the export type is trusted; no probe re-checks it")
		}
	}
}
