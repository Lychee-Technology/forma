package federated

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/queryplan"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/stretchr/testify/require"
)

// capturePlanPathSQL runs one federated query through an engine and captures the
// SQL+args handed to DuckDB.
func capturePlanPathSQL(t *testing.T, withCache bool, schemaID int16, mc *schemameta.MetadataCache) (string, []any) {
	t.Helper()
	duck := &fakeDuckDBExecutor{rows: &singleDuckDBRow{}}
	opts := []EngineOption{}
	if withCache {
		opts = append(opts, WithPlanCache(queryplan.NewCache(16)))
	}
	e := NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, &fakeDirtyIDFetcher{}, duck, nil,
		hybridDuckConfig(),
		mc, "host=pg dbname=x", opts...)
	q := &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{SchemaID: schemaID, Limit: 25}}
	q.PreferredTiers = []model.DataTier{model.DataTierHot, model.DataTierCold}
	q.DuckDBHints = &model.DuckDBRenderHints{S3ParquetPathTemplate: "s3://b/bench/{schema_id}/base.parquet"}
	orders := []model.AttributeOrder{{AttrID: 3, AttrName: "symbol", ColumnName: "text_01",
		StorageLocation: forma.AttributeStorageLocationMain, SortOrder: forma.SortOrderDesc, ValueType: forma.ValueTypeText}}
	tables := model.StorageTables{EntityMain: "entity_main_b", EAVData: "eav_b", ChangeLog: "cl_b"}
	_, _, _ = e.ExecuteDuckDBFederatedQuery(t.Context(), tables, q, 25, 0, orders, nil)
	return duck.lastSQL, duck.lastArgs
}

func TestEnginePlanCachePathParity(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()
	mc := schemameta.NewMetadataCache()
	require.NoError(t, mc.RegisterSchema("bench", 100, forma.SchemaAttributeCache{
		"symbol": {AttributeID: 3, ValueType: forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_01")}},
	}))

	directSQL, directArgs := capturePlanPathSQL(t, false, 100, mc)
	cachedSQL, cachedArgs := capturePlanPathSQL(t, true, 100, mc)

	// Each request stamps its own #252 cutoff, the sole legitimate byte
	// difference between the two paths; normalize it before comparing.
	require.Equal(t, normalizeFlushGraceCutoff(directSQL), normalizeFlushGraceCutoff(cachedSQL),
		"engine cache path must produce byte-identical SQL to the direct builder")
	require.Equal(t, directArgs, cachedArgs)
}
