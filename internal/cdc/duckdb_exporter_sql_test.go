package cdc

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

func TestBuildExportSQL_UsesRowIDsAndConfig(t *testing.T) {
	cfg := CDCConfig{ChangeLogTable: "change_log_dev", DuckMemLimit: "2GB", ParquetCompression: "zstd", ParquetCompressionLevel: 5}
	rowID := uuid.MustParse("019bed54-48eb-7cdc-aed3-8d38ec9c1394")

	sql, clQuery, mQuery, eQuery, err := buildExportSQL("s3://bucket/prefix/1/_tmp/tmp.parquet", cfg, 1, 1700000000000, []uuid.UUID{rowID}, testAttrCache())
	if err != nil {
		t.Fatalf("buildExportSQL returned error: %v", err)
	}

	// The batch's row ids travel as one uuid[] parameter, never as SQL
	// literals (#785).
	rowFilter := "row_id = ANY($1::uuid[])"
	if !strings.Contains(clQuery, rowFilter) {
		t.Fatalf("change log query missing row filter: %s", clQuery)
	}
	if !strings.Contains(mQuery, "ltbase_row_id = ANY($1::uuid[])") {
		t.Fatalf("main query missing row filter: %s", mQuery)
	}
	if !strings.Contains(eQuery, rowFilter) {
		t.Fatalf("eav query missing row filter: %s", eQuery)
	}
	for _, q := range []string{clQuery, mQuery, eQuery, sql} {
		require.NotContains(t, q, rowID.String())
	}
	if !strings.Contains(sql, "PRAGMA memory_limit='2GB'") {
		t.Fatalf("sql missing configured memory limit: %s", sql)
	}
	if !strings.Contains(sql, "PARQUET_VERSION V2") {
		t.Fatalf("sql missing parquet v2 export option: %s", sql)
	}
	if !strings.Contains(sql, "changed_at") || !strings.Contains(sql, "flag") {
		t.Fatalf("sql missing projected columns (changed_at/flag): %s", sql)
	}
}

func TestBuildExportSQL_ErrorsWithoutAttrCache(t *testing.T) {
	rowID := uuid.MustParse("019bed54-48eb-7cdc-aed3-8d38ec9c1394")
	_, _, _, _, err := buildExportSQL("s3://bucket/prefix/1/_tmp/tmp.parquet", CDCConfig{}, 1, 1700000000000, []uuid.UUID{rowID}, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSchemaAttrCacheUnavailable)
	require.Contains(t, err.Error(), "1")
}

func TestBuildExportSQL_ErrorsOnEmptyRowIDs(t *testing.T) {
	_, _, _, _, err := buildExportSQL("s3://bucket/prefix/1/_tmp/tmp.parquet", CDCConfig{}, 1, 0, nil, nil)
	if err == nil {
		t.Fatalf("expected error for empty row ids")
	}
}

func TestBuildExportSQL_WithSchemaCacheProjectsColumns(t *testing.T) {
	attrCache := forma.SchemaAttributeCache{
		"name": {
			AttributeName: "name",
			AttributeID:   10,
			ValueType:     forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText01},
		},
		"flag": {
			AttributeName: "flag",
			AttributeID:   11,
			ValueType:     forma.ValueTypeBool,
		},
	}
	rowID := uuid.MustParse("019bed54-48eb-7cdc-aed3-8d38ec9c1394")

	sql, clQuery, mQuery, eQuery, err := buildExportSQL("s3://bucket/prefix/1/_tmp/tmp.parquet", CDCConfig{}, 1, 1700000000000, []uuid.UUID{rowID}, attrCache)
	if err != nil {
		t.Fatalf("buildExportSQL returned error: %v", err)
	}

	if !strings.Contains(mQuery, "text_01") {
		t.Fatalf("main query missing bound column: %s", mQuery)
	}
	if !strings.Contains(sql, "m.text_01") || !strings.Contains(sql, "name") {
		t.Fatalf("sql missing main projection for name: %s", sql)
	}
	if !strings.Contains(eQuery, "attr_id IN (11)") {
		t.Fatalf("eav query missing attr_id filter: %s", eQuery)
	}
	if !strings.Contains(sql, "flag") {
		t.Fatalf("sql missing eav projection for flag: %s", sql)
	}
	if !strings.Contains(clQuery, "changed_at") {
		t.Fatalf("cl query malformed: %s", clQuery)
	}
}

// TestExportSQLEscapesQuotedLiterals pins the CDC half of the #310
// consolidation. The S3 tmp path is deployment-controlled text inside a
// single-quoted literal — COPY (…) TO '<path>' (…) — and must pass through
// sqlutil.EscapeLiteral first, or a quoted S3 prefix terminates the literal
// early and turns deployment-controlled text into SQL structure. No other
// cdc test passes an S3 path containing a quote. The PostgreSQL DSN, the
// export's other such literal until #785, no longer enters the SQL at all.
func TestExportSQLEscapesQuotedLiterals(t *testing.T) {
	const hostileS3TmpPath = `s3://bucket/pre'fix/1/_tmp/tmp.parquet`
	const wantS3LiteralBody = `s3://bucket/pre''fix/1/_tmp/tmp.parquet`
	// The trailing "' (" anchors the literal's closing quote to the start of
	// the COPY options list, so a malformed target that merely contains the
	// escaped path (e.g. an unterminated or doubled-quote tail) cannot match.
	const wantCopyTarget = `TO '` + wantS3LiteralBody + `' (`

	rowID := uuid.MustParse("019bed54-48eb-7cdc-aed3-8d38ec9c1394")

	builders := []struct {
		name  string
		build func() (string, error)
	}{
		{
			name: "snapshot export",
			build: func() (string, error) {
				sql, _, _, _, err := buildExportSQL(hostileS3TmpPath, CDCConfig{}, 1, 1700000000000, []uuid.UUID{rowID}, testAttrCache())
				return sql, err
			},
		},
		{
			name: "base export",
			build: func() (string, error) {
				sql, _, _, err := buildBaseExportSQL(hostileS3TmpPath, CDCConfig{}, 1, []uuid.UUID{rowID}, testAttrCache())
				return sql, err
			},
		},
	}

	for _, b := range builders {
		t.Run(b.name, func(t *testing.T) {
			sql, err := b.build()
			require.NoError(t, err)
			require.NotContains(t, sql, "ATTACH")

			// The raw path's bare quote would close the literal early.
			require.Contains(t, sql, wantCopyTarget)
			require.NotContains(t, sql, "TO '"+hostileS3TmpPath+"'")

			// Structural check: exactly one outer quote pair, and undoubling
			// its body round-trips back to the raw S3 path.
			const copyOpenMarker = ") TO '"
			const copyCloseMarker = "' ("
			copyStart := strings.Index(sql, copyOpenMarker)
			require.NotEqual(t, -1, copyStart, "COPY target not found: %s", sql)
			copyBody := sql[copyStart+len(copyOpenMarker):]
			copyEnd := strings.Index(copyBody, copyCloseMarker)
			require.NotEqual(t, -1, copyEnd, "COPY literal not terminated: %s", sql)
			copyBody = copyBody[:copyEnd]

			require.Equal(t, wantS3LiteralBody, copyBody)
			require.Equal(t, hostileS3TmpPath, strings.ReplaceAll(copyBody, "''", "'"))
		})
	}
}

func TestBuildExportSQL_UsesCustomTableNames(t *testing.T) {
	cfg := CDCConfig{
		ChangeLogTable:  "change_log_dev",
		EntityMainTable: "entity_main_dev",
		EAVDataTable:    "eav_data_dev",
	}
	rowID := uuid.MustParse("019bed54-48eb-7cdc-aed3-8d38ec9c1394")

	sql, clQuery, mQuery, eQuery, err := buildExportSQL("s3://bucket/prefix/1/_tmp/tmp.parquet", cfg, 1, 1700000000000, []uuid.UUID{rowID}, testAttrCache())
	if err != nil {
		t.Fatalf("buildExportSQL returned error: %v", err)
	}

	if !strings.Contains(clQuery, `FROM "change_log_dev"`) {
		t.Fatalf("change log query not using custom table name: %s", clQuery)
	}
	if !strings.Contains(mQuery, `FROM "entity_main_dev"`) {
		t.Fatalf("main query not using custom table name: %s", mQuery)
	}
	if !strings.Contains(eQuery, `FROM "eav_data_dev"`) {
		t.Fatalf("eav query not using custom table name: %s", eQuery)
	}
	_ = sql
}

// TestExportToTmpWrapsPlanErrors pins the caller-side context on both export
// entry points: a plan failure must name the operation the caller was
// performing, not arrive as a bare pass-through (#276, coding-standard.md).
// The cheapest plan failures return before any DB or S3 use, so a zero-value
// DuckExporter is enough: empty rowIDs for the snapshot (a base export with
// no row ids is a legitimate zero-row export since #519) and a missing
// attribute cache for the base.
func TestExportToTmpWrapsPlanErrors(t *testing.T) {
	exporter := &DuckExporter{}
	cfg := CDCConfig{}

	t.Run("snapshot export", func(t *testing.T) {
		err := exporter.ExportSnapshotToTmp(context.Background(), cfg,
			nil, "s3://bucket/7/_tmp/f.parquet", 7, 0, nil, testAttrCache())
		require.Error(t, err)
		require.Contains(t, err.Error(), "export snapshot to tmp for schema 7")
		require.Contains(t, err.Error(), "no row ids provided")
	})

	t.Run("base export", func(t *testing.T) {
		err := exporter.ExportBaseFileToTmp(context.Background(), cfg,
			nil, "s3://bucket/base/7/_tmp/f.parquet", 7, nil, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "export base file to tmp for schema 7")
		require.ErrorIs(t, err, ErrSchemaAttrCacheUnavailable)
	})
}

// The projection reads only the staged DuckDB temp tables, each through
// the CTE name it joins on, and nothing in the export reaches PostgreSQL from
// DuckDB (#785). Each staged table is created with its full column set, so a
// stage that returns no rows still projects every column (#519). Both export
// shapes are covered: the delta export (change_log driven) and the base
// export (entity_main only).
func TestBuildExportSQL_ReadsOnlyStagedTables(t *testing.T) {
	rowID := uuid.MustParse("019bed54-48eb-7cdc-aed3-8d38ec9c1394")
	delta, err := buildDeltaExportPlan(stagedTransport, "s3://b/1/_tmp/d.parquet", CDCConfig{}, 1, 1700000000000, []uuid.UUID{rowID}, testAttrCache())
	require.NoError(t, err)
	base, err := buildBaseExportPlan(stagedTransport, "s3://b/1/_tmp/b.parquet", CDCConfig{}, 1, []uuid.UUID{rowID}, testAttrCache())
	require.NoError(t, err)

	for name, tc := range map[string]struct {
		plan   exportSQLPlan
		stages map[string]string
	}{
		"delta": {delta, map[string]string{"cl": stageTableChangeLog, "m": stageTableMain, "e_rows": stageTableEAV}},
		"base":  {base, map[string]string{"m": stageTableMain, "e_rows": stageTableEAV}},
	} {
		all := tc.plan.setupSQL + tc.plan.copySQL
		for _, banned := range []string{"postgres_query", "ATTACH", "pg_db"} {
			require.NotContains(t, all, banned, name)
		}
		require.Len(t, tc.plan.stages, len(tc.stages), name)
		for _, stage := range tc.plan.stages {
			require.Equal(t, tc.stages[stage.alias], stage.table, "%s: stage %s", name, stage.alias)
			require.Contains(t, tc.plan.copySQL, stage.alias+" AS (SELECT * FROM temp.main."+stage.table+")", "%s: CTE %s", name, stage.alias)
			require.Contains(t, tc.plan.setupSQL, "CREATE OR REPLACE TEMP TABLE "+stage.table+" (", "%s: table %s", name, stage.table)
			require.Contains(t, buildStageDropSQL(tc.plan.stages), "DROP TABLE IF EXISTS temp.main."+stage.table+";", name)
			require.True(t, strings.HasPrefix(stage.query, "SELECT "+strings.Join(stageColumnNames(stage.columns), ", ")+" FROM "),
				"%s: stage %s reads exactly its staged columns, in order: %s", name, stage.alias, stage.query)
		}
	}
}
