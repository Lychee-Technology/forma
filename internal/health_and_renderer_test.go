package internal

import (
	"context"
	"testing"
	"text/template"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sqlgen"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// Basic config validation tests for Postgres and S3 helpers.
func TestValidatePostgresAndS3Config(t *testing.T) {
	// Postgres config validation
	pg := forma.DatabaseConfig{
		Host:           "",
		Port:           5432,
		MaxConnections: 5,
	}
	err := ValidatePostgresConfig(pg)
	require.Error(t, err, "empty host should fail validation")

	pg.Host = "localhost"
	err = ValidatePostgresConfig(pg)
	require.NoError(t, err, "valid postgres config should pass")

	// S3 config validation
	s3 := forma.DuckDBConfig{EnableS3: true}
	err = ValidateS3Config(s3)
	require.Error(t, err, "enableS3 without endpoint or creds should fail")

	s3.S3Endpoint = "http://localhost:9000"
	err = ValidateS3Config(s3)
	require.NoError(t, err, "endpoint-only S3 config allowed for basic checks")

	// credential mismatch
	s3 = forma.DuckDBConfig{EnableS3: true, S3AccessKey: "k"}
	err = ValidateS3Config(s3)
	require.Error(t, err, "access key without secret should fail")
}

// Postgres health check should error on empty DSN.
func TestPostgresHealthCheck_EmptyDSN(t *testing.T) {
	err := PostgresHealthCheck(context.Background(), "", 0)
	require.Error(t, err)
}

// Ensure sqlgen.BuildDuckDBQuery injects PgMainClause and Anchor.Condition when dual clauses are provided.
func TestBuildDuckDBQuery_Injection(t *testing.T) {
	tpl := template.Must(template.New("test").Parse("PGCLAUSE:{{.PgMainClause}}|COND:{{.Anchor.Condition}}"))
	dual := &sqlgen.DualClauses{
		DuckClause:   "age > ?",
		DuckArgs:     []any{18},
		PgMainClause: "integer_01 > 18",
		PgMainArgs:   []any{18},
	}

	sql, args, err := sqlgen.BuildDuckDBQuery(tpl, map[string]any{}, nil, nil, dual)
	require.NoError(t, err)
	require.Contains(t, sql, "PGCLAUSE:integer_01 > 18")
	require.Contains(t, sql, "COND:age > ?")
	require.Len(t, args, 2)
	require.Equal(t, 18, args[0])
	require.Equal(t, 18, args[1])
}

func TestBuildDuckDBQuery_PreservesInjectedProductionTemplateParams(t *testing.T) {
	tpl := template.Must(template.New("test").Parse("PG={{.PG_CONN}}|S3={{.S3_PATHS}}|SCHEMA={{.SCHEMA_ID}}|PAGE={{.PAGE_SIZE}}|OFFSET={{.OFFSET}}|LOGICAL={{.LOGICAL_WHERE_CLAUSE}}|PUSH={{.PG_WHERE_CLAUSE}}"))
	dual := &sqlgen.DualClauses{
		DuckClause:   "age > ?",
		DuckArgs:     []any{18},
		PgMainClause: "integer_01 > 18",
	}
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 42, Limit: 25, Offset: 5},
	}

	sql, args, err := sqlgen.BuildDuckDBQuery(tpl, map[string]any{"DuckDBPGConnString": "host=pg port=5432", "S3_PATHS": "'s3://bucket/prefix/42/base/*.parquet'", "Anchor": map[string]any{}}, q, nil, dual)
	require.NoError(t, err)
	require.Contains(t, sql, "PG=host=pg port=5432")
	require.Contains(t, sql, "S3='s3://bucket/prefix/42/base/*.parquet'")
	require.Contains(t, sql, "SCHEMA=42")
	require.Contains(t, sql, "PAGE=25")
	require.Contains(t, sql, "OFFSET=5")
	require.Contains(t, sql, "LOGICAL=age > ?")
	require.Contains(t, sql, "PUSH=integer_01 > 18")
	require.Len(t, args, 1)
	require.Equal(t, 18, args[0])
}

func TestBuildDuckDBQuery_AdvancedTemplateUsesConfiguredTableNames(t *testing.T) {
	dual := &sqlgen.DualClauses{
		DuckClause:   "1=1",
		PgMainClause: "1=1",
	}
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 42, Limit: 25, Offset: 5},
		DuckDBHints:    &model.DuckDBRenderHints{S3ParquetPathTemplate: "s3://bucket/prefix/{{.SchemaID}}/base/*.parquet"},
	}
	params := map[string]any{
		"DuckDBPGConnString": "host=pg port=5432",
		"ChangeLogSchema":    "public",
		"ChangeLogScanTable": "change_log",
		"MainSchema":         "public",
		"MainScanTable":      "entity_main",
		"EAVSchema":          "public",
		"EAVScanTable":       "eav_data",
		"Anchor":             map[string]any{},
	}
	// The advanced template refuses to render without schema projection params
	// (#222 retired the toy defaults); derive them like the engine's
	// injectSchemaProjections does. The EAV attribute keeps the eav_data pivot
	// scan in the rendered SQL, which this test asserts on.
	sp, err := sqlgen.BuildSchemaProjection(42, forma.SchemaAttributeCache{
		"name": {AttributeID: 1, ValueType: forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_01")}},
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
	// A resolved parquet path set is required too (#299): the renderer rejects an
	// unbound S3_PATHS rather than emitting read_parquet(<no value>). The query's
	// DuckDBHints template is resolved by the engine, not by the renderer, so a
	// direct renderer call must bind the paths itself.
	params["DuckDBS3Paths"] = []string{"s3://bucket/prefix/42/base/a.parquet"}

	sql, _, err := sqlgen.BuildDuckDBQuery(sqlgen.AdvancedQueryTemplateDuckDB, params, q, nil, dual)
	require.NoError(t, err)
	require.Contains(t, sql, "postgres_scan('host=pg port=5432', 'public', 'change_log')")
	require.Contains(t, sql, "JOIN postgres_scan('host=pg port=5432',")
	require.Contains(t, sql, "'entity_main'")
	require.Contains(t, sql, "postgres_scan('host=pg port=5432', 'public', 'eav_data')")
	require.NotContains(t, sql, "change_log_dev")
	require.NotContains(t, sql, "entity_main_dev")
	require.NotContains(t, sql, "eav_data_dev")
}
