package cdc

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

func TestBuildExportSQL_UsesRowIDsAndConfig(t *testing.T) {
	cfg := CDCConfig{ChangeLogTable: "change_log_dev", DuckMemLimit: "2GB", ParquetCompression: "zstd", ParquetCompressionLevel: 5}
	rowID := uuid.MustParse("019bed54-48eb-7cdc-aed3-8d38ec9c1394")

	sql, clQuery, mQuery, eQuery, err := buildExportSQL("host=pg", "s3://bucket/prefix/1/_tmp/tmp.parquet", cfg, 1, 1700000000000, []uuid.UUID{rowID}, testAttrCache())
	if err != nil {
		t.Fatalf("buildExportSQL returned error: %v", err)
	}

	rowFilter := "row_id IN (UUID '019bed54-48eb-7cdc-aed3-8d38ec9c1394')"
	if !strings.Contains(clQuery, rowFilter) {
		t.Fatalf("change log query missing row filter: %s", clQuery)
	}
	if !strings.Contains(mQuery, "ltbase_row_id IN (UUID '019bed54-48eb-7cdc-aed3-8d38ec9c1394')") {
		t.Fatalf("main query missing row filter: %s", mQuery)
	}
	if !strings.Contains(eQuery, rowFilter) {
		t.Fatalf("eav query missing row filter: %s", eQuery)
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
	_, _, _, _, err := buildExportSQL("host=pg", "s3://bucket/prefix/1/_tmp/tmp.parquet", CDCConfig{}, 1, 1700000000000, []uuid.UUID{rowID}, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSchemaAttrCacheUnavailable)
	require.Contains(t, err.Error(), "1")
}

func TestBuildExportSQL_ErrorsOnEmptyRowIDs(t *testing.T) {
	_, _, _, _, err := buildExportSQL("pg", "s3://bucket/prefix/1/_tmp/tmp.parquet", CDCConfig{}, 1, 0, nil, nil)
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

	sql, clQuery, mQuery, eQuery, err := buildExportSQL("host=pg", "s3://bucket/prefix/1/_tmp/tmp.parquet", CDCConfig{}, 1, 1700000000000, []uuid.UUID{rowID}, attrCache)
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

// TestExportSQLEscapesQuotedLiterals pins the CDC half of the #310 consolidation.
//
// Both export builders embed two deployment-controlled strings inside
// single-quoted SQL literals: the Postgres DSN — ATTACH IF NOT EXISTS '<dsn>'
// AS pg_db (…) — and the S3 tmp path — COPY (…) TO '<path>' (…). Both must
// pass through sqlutil.EscapeLiteral first. Without that, a quoted DSN (the
// form federated.DuckDBPostgresConnStringFromPool emits, and any password
// containing a quote) or a quoted S3 prefix terminates the literal early and
// turns deployment-controlled text into SQL structure. No other cdc test
// passes a DSN or S3 path containing a quote, so nothing else would catch a
// regression here.
func TestExportSQLEscapesQuotedLiterals(t *testing.T) {
	const hostileDSN = `host='h' password='p''w' dbname=forma`
	const wantDSNLiteralBody = `host=''h'' password=''p''''w'' dbname=forma`
	const wantAttach = `ATTACH IF NOT EXISTS '` + wantDSNLiteralBody + `' AS pg_db (TYPE postgres, READ_ONLY);`

	const hostileS3TmpPath = `s3://bucket/pre'fix/1/_tmp/tmp.parquet`
	const wantCopyTarget = `TO 's3://bucket/pre''fix/1/_tmp/tmp.parquet'`

	rowID := uuid.MustParse("019bed54-48eb-7cdc-aed3-8d38ec9c1394")

	builders := []struct {
		name  string
		build func() (string, error)
	}{
		{
			name: "snapshot export",
			build: func() (string, error) {
				sql, _, _, _, err := buildExportSQL(hostileDSN, hostileS3TmpPath, CDCConfig{}, 1, 1700000000000, []uuid.UUID{rowID}, testAttrCache())
				return sql, err
			},
		},
		{
			name: "base export",
			build: func() (string, error) {
				sql, _, _, err := buildBaseExportSQL(hostileDSN, hostileS3TmpPath, CDCConfig{}, 1, []uuid.UUID{rowID}, testAttrCache())
				return sql, err
			},
		},
	}

	for _, b := range builders {
		t.Run(b.name, func(t *testing.T) {
			sql, err := b.build()
			require.NoError(t, err)
			require.Contains(t, sql, wantAttach)

			// The raw DSN must never be embedded verbatim: its bare quotes
			// would close the outer literal early.
			require.NotContains(t, sql, "ATTACH IF NOT EXISTS '"+hostileDSN+"'")

			// The COPY target is the second deployment-controlled literal.
			// The raw path's bare quote would close it early.
			require.Contains(t, sql, wantCopyTarget)
			require.NotContains(t, sql, "TO '"+hostileS3TmpPath+"'")

			// Structural check: the ATTACH literal is delimited by exactly one
			// outer quote pair, and undoubling its body round-trips back to the
			// original DSN.
			const openMarker = "ATTACH IF NOT EXISTS '"
			const closeMarker = "' AS pg_db"
			start := strings.Index(sql, openMarker)
			require.NotEqual(t, -1, start, "ATTACH statement not found: %s", sql)
			body := sql[start+len(openMarker):]
			end := strings.Index(body, closeMarker)
			require.NotEqual(t, -1, end, "ATTACH literal not terminated: %s", sql)
			body = body[:end]

			require.Equal(t, wantDSNLiteralBody, body)
			require.Equal(t, hostileDSN, strings.ReplaceAll(body, "''", "'"))
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

	sql, clQuery, mQuery, eQuery, err := buildExportSQL("host=pg", "s3://bucket/prefix/1/_tmp/tmp.parquet", cfg, 1, 1700000000000, []uuid.UUID{rowID}, testAttrCache())
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
