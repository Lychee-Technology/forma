package federated

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/queryplan"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/stretchr/testify/require"
)

func TestColdMissingColumnsDetectsAbsentAttributes(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"name":  {AttributeID: 1, ValueType: forma.ValueTypeText},
		"score": {AttributeID: 3, ValueType: forma.ValueTypeInteger},
		"tags":  {AttributeID: 4, ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeBigInt},
	}
	union := map[string]string{
		"row_id": "UUID", "changed_at": "BIGINT", "deleted_at": "BIGINT",
		"name": "VARCHAR",
	}
	got := coldMissingColumns(cache, union)
	require.Equal(t, []sqlgen.NullScanColumn{
		// EAV-only integer augments at storage width DOUBLE (#384).
		{Name: "score", DuckDBType: "DOUBLE"},
		{Name: "tags", DuckDBType: "BIGINT[]"},
	}, got, "absent attrs detected, present ones skipped, sorted by name")
}

func TestColdMissingColumnsNilOnUnknownUnionOrEmptyCache(t *testing.T) {
	cache := forma.SchemaAttributeCache{"score": {AttributeID: 3, ValueType: forma.ValueTypeInteger}}
	require.Nil(t, coldMissingColumns(cache, nil), "unknown union must not augment")
	require.Nil(t, coldMissingColumns(nil, map[string]string{}), "no metadata, nothing to augment")
}

// Dotted attribute names must probe the FOLDED parquet column (#260), not
// the raw name — otherwise a flushed dotted attribute reads as missing and
// gets shadow-augmented.
func TestColdMissingColumnsUsesFoldedParquetColumnNames(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"user.name": {AttributeID: 7, ValueType: forma.ValueTypeText},
	}
	union := map[string]string{sqlgen.ParquetAttrColumn("user.name"): "VARCHAR"}
	require.Nil(t, coldMissingColumns(cache, union))
}

// #255 plan-cache poisoning guard: the missing set participates in the
// scope hash, so the same query shape over the same path STRING (a glob
// hint does not change on flush) re-keys when the first flush lands the
// column.
func TestDuckPlanScopePartsIncludeColdMissingSet(t *testing.T) {
	tables := model.StorageTables{EAVData: "eav_data", EntityMain: "entity_main", ChangeLog: "change_log"}
	paths := []string{"s3://b/schema/1/**/*.parquet"}
	absent := duckPlanScopeParts(tables, "conn", 10, 0, false, paths, nil,
		[]sqlgen.NullScanColumn{{Name: "score", DuckDBType: "INTEGER"}})
	present := duckPlanScopeParts(tables, "conn", 10, 0, false, paths, nil, nil)
	require.NotEqual(t,
		queryplan.HashScopeParts(absent...),
		queryplan.HashScopeParts(present...),
		"cold-absent and cold-present shapes must occupy different plan-cache entries")
}
