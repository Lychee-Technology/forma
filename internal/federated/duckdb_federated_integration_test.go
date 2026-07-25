package federated

import (
	"context"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/stretchr/testify/require"
)

func TestEvaluateRoutingPolicy_VariousStrategies(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled: true,
		Routing: forma.RoutingPolicy{
			Strategy:          forma.RoutingStrategyHybrid,
			MaxDuckDBScanRows: 5000,
			AllowS3Fallback:   true,
		},
	}

	dec := EvaluateRoutingPolicy(cfg, nil, nil)
	require.True(t, dec.UseDuckDB, "hybrid should use duckdb by default")

	fq := &model.FederatedAttributeQuery{PreferHot: true}
	dec = EvaluateRoutingPolicy(cfg, fq, nil)
	require.False(t, dec.UseDuckDB, "PreferHot should disable duckdb")

	cfg.Routing.Strategy = forma.RoutingStrategyCostFirst
	dec = EvaluateRoutingPolicy(cfg, nil, &model.FederatedQueryOptions{MaxRows: 100000})
	require.True(t, dec.UseDuckDB, "cost-first large scan should enable duckdb")

	cfg.Enabled = false
	dec = EvaluateRoutingPolicy(cfg, nil, nil)
	require.False(t, dec.UseDuckDB, "disabled config should not use duckdb")
}

func TestEvaluateRoutingPolicy_QueryShapeAware(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled: true,
		Routing: forma.RoutingPolicy{
			Strategy: forma.RoutingStrategyHybrid,
		},
	}

	fq := &model.FederatedAttributeQuery{PreferHot: true}
	dec := EvaluateRoutingPolicy(cfg, fq, nil)
	require.False(t, dec.UseDuckDB)
	require.Equal(t, []model.DataTier{model.DataTierHot}, dec.Tiers)

	fq = &model.FederatedAttributeQuery{PreferredTiers: []model.DataTier{model.DataTierWarm, model.DataTierCold}}
	dec = EvaluateRoutingPolicy(cfg, fq, nil)
	require.True(t, dec.UseDuckDB)

	fq = &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{Limit: 20, Offset: 0}}
	dec = EvaluateRoutingPolicy(cfg, fq, nil)
	require.False(t, dec.UseDuckDB)

	fq = &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{Limit: 2000, Offset: 0}}
	dec = EvaluateRoutingPolicy(cfg, fq, nil)
	require.True(t, dec.UseDuckDB)

	fq = &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{Limit: 20, Offset: 10000}}
	dec = EvaluateRoutingPolicy(cfg, fq, nil)
	require.True(t, dec.UseDuckDB)

	dec = EvaluateRoutingPolicy(cfg, nil, nil)
	require.True(t, dec.UseDuckDB)

	fq = &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{Limit: 50, Offset: 0}}
	dec = EvaluateRoutingPolicy(cfg, fq, nil)
	require.False(t, dec.UseDuckDB)

	fq = &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{Limit: 10000, Offset: 0}}
	dec = EvaluateRoutingPolicy(cfg, fq, nil)
	require.True(t, dec.UseDuckDB)

	fq = &model.FederatedAttributeQuery{PreferHot: true, AttributeQuery: model.AttributeQuery{Limit: 10000, Offset: 0}}
	dec = EvaluateRoutingPolicy(cfg, fq, nil)
	require.False(t, dec.UseDuckDB)
}

func TestNewDuckDBClient_HealthCheck(t *testing.T) {
	skipIfNoDuckDB(t)
	client, err := NewDuckDBClient(forma.DuckDBConfig{Enabled: true, DBPath: ":memory:"})
	require.NoError(t, err)
	defer client.Close()

	err = client.HealthCheck(context.Background())
	require.NoError(t, err)
}

func TestExecuteDuckDBFederatedQuery_NilQuery(t *testing.T) {
	engine := &DBFederatedQueryEngine{}
	_, _, err := engine.ExecuteDuckDBFederatedQuery(context.Background(), model.StorageTables{}, nil, 10, 0, nil, nil)
	require.Error(t, err)
}

func TestFinalizeDuckDBExecutionPlan_CaptureDisabled(t *testing.T) {
	engine := &DBFederatedQueryEngine{}
	planCtx := &duckDBExecutionPlanContext{opts: &model.FederatedQueryOptions{}, startTotal: time.Now()}
	engine.finalizeDuckDBExecutionPlan(context.Background(), planCtx, nil, 0, 0)
}

func TestBuildDuckDBQuery_AdvancedTemplate(t *testing.T) {
	tmpl := sqlgen.AdvancedQueryTemplateDuckDB
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{
			SchemaID: 1,
			Limit:    20,
			Offset:   0,
		},
	}
	params := map[string]any{
		"EAVTable":             "eav_data",
		"MainTable":            "entity_main",
		"ChangeLogTable":       "change_log",
		"MainProjection":       "ltbase_schema_id, ltbase_row_id",
		"SchemaID":             1,
		"UseMainTableAsAnchor": false,
		"Anchor":               map[string]any{"Condition": "1=1"},
		"SortKeys":             []model.AttributeOrder{},
		"Limit":                20,
		"Offset":               0,
		"PageSize":             20,
	}
	// The advanced template refuses to render without schema projection params
	// (#222 retired the toy defaults); derive them like injectSchemaProjections
	// does, from the same fixture shape design_doc_sql_test.go pins.
	sp, err := sqlgen.BuildSchemaProjection(1, forma.SchemaAttributeCache{
		"name": {AttributeID: 1, ValueType: forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_01")}},
		"age": {AttributeID: 2, ValueType: forma.ValueTypeInteger,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("integer_01")}},
		"tag": {AttributeID: 205, ValueType: forma.ValueTypeText},
	})
	require.NoError(t, err)
	params["S3SourceSelect"] = sp.S3SourceSelect
	params["PGSourceSelect"] = sp.PGSourceSelect
	params["PGGroupBy"] = sp.PGGroupBy
	params["EAVPivotSelect"] = sp.EAVPivotSelect
	params["EAVPivotAttrs"] = sp.EAVPivotAttrs
	params["HasEAVPivot"] = sp.EAVPivotAttrs != ""
	params["OuterSelect"] = sp.OuterSelect
	// And a resolved parquet path set (#299): an unbound S3_PATHS is rejected
	// instead of rendering read_parquet(<no value>).
	params["DuckDBS3Paths"] = []string{"s3://bucket/1/a.parquet"}
	sql, args, err := sqlgen.BuildDuckDBQuery(tmpl, params, q, []uuid.UUID{}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, sql)
	require.NotNil(t, args)
}

func TestAppendDirtyExclusion(t *testing.T) {
	baseClause := "1=1"
	aliceID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	clause, args := sqlgen.AppendDirtyExclusion(baseClause, []uuid.UUID{aliceID})
	require.Contains(t, clause, "NOT IN")
	require.Len(t, args, 1)
}

func skipIfNoDuckDB(t *testing.T) {
	t.Helper()
}
