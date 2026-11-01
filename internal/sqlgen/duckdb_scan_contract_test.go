package sqlgen

import (
	"testing"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sqlgen/sqlgentest"
	"github.com/stretchr/testify/require"
)

// TestAdvancedTemplate_PostgresScanContract pins the runtime scan contract
// that docs/federated-query/design.md §5 documents (#214, #752): every hot-leg
// scan is a plain table reference into the attached PostgreSQL catalog,
// <alias>."schema"."table", and flushed_at = 0 is expressed only as a WHERE
// predicate. The rendered query carries the catalog's alias and never the
// connection string: the attach that holds it runs ahead of the query
// (PostgresAttachSQL).
func TestAdvancedTemplate_PostgresScanContract(t *testing.T) {
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 1, Limit: 10, Offset: 0},
	}
	dual := &DualClauses{DuckClause: "1=1"}
	const conn = "host=localhost dbname=forma password=hunter2-752"
	// Mirror the production params from buildDuckDBQueryWithPlan
	// (internal/federated/duckdb_query.go); the schema projection params come
	// from the shared fixture (HasEAVPivot = true via the EAV tag attribute),
	// mirroring the engine's injectSchemaProjections (#222).
	params := injectTestRenderParams(t, map[string]any{
		"DuckDBPGConnString":   conn,
		"ChangeLogSchema":      "public",
		"ChangeLogScanTable":   "change_log",
		"MainSchema":           "public",
		"MainScanTable":        "entity_main_dev",
		"EAVSchema":            "public",
		"EAVScanTable":         "eav_data_dev",
		"S3_PATHS":             "['s3://bucket/base/*.parquet']",
		"LOGICAL_WHERE_CLAUSE": "1=1",
	}, 1)

	sqlText, _, err := BuildDuckDBQuery(AdvancedQueryTemplateDuckDB, params, q, nil, dual)
	require.NoError(t, err)

	refs := sqlgentest.FindPostgresTableRefs(sqlText)
	var tables []string
	for _, ref := range refs {
		require.Equal(t, PostgresCatalogAlias(conn), ref.Catalog,
			"every scan must read the catalog attached for the engine's connection string")
		require.Equal(t, "public", ref.Schema)
		tables = append(tables, ref.Table)
	}
	require.Equal(t, []string{"change_log", "change_log", "entity_main_dev", "eav_data_dev"}, tables,
		"dirty_ids, pg_source change_log, entity_main, and the EAV pivot must all scan the attached catalog")
	require.NotContains(t, sqlText, "postgres_scan(",
		"DuckDB 2.0's postgres extension has no postgres_scan table function (#752)")
	require.NotContains(t, sqlText, "hunter2-752",
		"the rendered query must not carry the connection string or its password")

	require.Contains(t, sqlText, "AND (flushed_at = 0 OR flushed_at >= ",
		"dirty_ids must restrict to unflushed-or-grace rows via a WHERE predicate (#252)")
	require.Contains(t, sqlText, "AND (cl.flushed_at = 0 OR cl.flushed_at >= ",
		"pg_source must restrict to unflushed-or-grace rows via a WHERE predicate (#252)")
}

// TestAdvancedTemplate_ColdOnlyOmitsPgSource pins the #184 tier-hint contract:
// when PreferredTiers excludes hot, the rendered SQL must not contain the
// pg_source data CTE (no main-table or EAV postgres_scan) and must not bind
// the PgMainArgs slice — while the dirty_ids consistency barrier (the sole
// remaining change_log scan) is retained, so unflushed rows stay consistently
// invisible instead of resurfacing as stale parquet versions.
func TestAdvancedTemplate_ColdOnlyOmitsPgSource(t *testing.T) {
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 1, Limit: 10, Offset: 0},
		PreferredTiers: []model.DataTier{model.DataTierWarm, model.DataTierCold},
	}
	dual := &DualClauses{
		DuckClause: "age > ?", DuckArgs: []any{int64(10)},
		PgMainClause: "m.integer_01 > ?", PgMainArgs: []any{int64(10)},
	}
	params := injectTestRenderParams(t, map[string]any{
		"DuckDBPGConnString":   "dbname=forma host=localhost",
		"ChangeLogSchema":      "public",
		"ChangeLogScanTable":   "change_log",
		"MainSchema":           "public",
		"MainScanTable":        "entity_main_dev",
		"EAVSchema":            "public",
		"EAVScanTable":         "eav_data_dev",
		"S3_PATHS":             "['s3://bucket/base/*.parquet']",
		"LOGICAL_WHERE_CLAUSE": "age > ?",
	}, 1)

	sqlText, args, err := BuildDuckDBQuery(AdvancedQueryTemplateDuckDB, params, q, nil, dual)
	require.NoError(t, err)

	refs := sqlgentest.FindPostgresTableRefs(sqlText)
	require.Len(t, refs, 1,
		"a hot-excluded query must keep only the dirty_ids change_log scan")
	require.Equal(t, "change_log", refs[0].Table,
		"the sole remaining PostgreSQL scan must be the dirty-id consistency barrier")
	require.NotContains(t, sqlText, "pg_source",
		"hot excluded: the pg_source data CTE must not render")
	require.NotContains(t, sqlText, "UNION ALL",
		"hot excluded: unified must read from s3_source alone")
	require.Contains(t, sqlText, "read_parquet",
		"warm/cold requested: the parquet source must render")
	require.Contains(t, sqlText, "AND (flushed_at = 0)",
		"the dirty_ids barrier must survive tier pruning UNWIDENED: with "+
			"pg_source pruned there is no hot server for grace-discarded rows (#252)")
	require.NotContains(t, sqlText, "OR flushed_at >= ",
		"hot-excluded shapes must keep the strict flushed_at = 0 barrier")
	require.Equal(t, []any{int64(10), int64(10)}, args,
		"PgMainArgs must be dropped with the pg_source CTE: "+
			"binds are the s3 semijoin and visible occurrences of DuckArgs only")
}
