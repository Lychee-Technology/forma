package federated

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/stretchr/testify/require"
)

// #460 regression: the parquet legs used to alias the LWW version stamp into
// the created_at slot, so a created→updated→flushed row reported its LAST
// UPDATE time as its creation time, and the default sort key
// (created_at DESC, row_id ASC) changed value the instant a row was flushed.
//
// These tests execute the RUNTIME template (not the §5 sketch — that is
// design_doc_sql_test.go's job) on an in-memory DuckDB, with each
// postgres_scan rewritten to a local relation. Nothing here needs a container.

// createdAtFixtureCache is the canonical name/age/tag schema shared with the
// sqlgen renderer fixtures and the design-doc guard.
func createdAtFixtureCache() forma.SchemaAttributeCache {
	return forma.SchemaAttributeCache{
		"name": {AttributeID: 1, ValueType: forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_01")}},
		"age": {AttributeID: 2, ValueType: forma.ValueTypeInteger,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("integer_01")}},
		"tag": {AttributeID: 205, ValueType: forma.ValueTypeText},
	}
}

// renderRuntimeQueryForLocalDuckDB renders the production advanced template
// over the given parquet path and rewrites it to run standalone: every
// postgres_scan(conn, schema, table) collapses to `table`, which the caller
// has created as a local relation.
func renderRuntimeQueryForLocalDuckDB(t *testing.T, parquetPath string, limit, offset int) string {
	t.Helper()

	sp, err := sqlgen.BuildSchemaProjection(1, createdAtFixtureCache())
	require.NoError(t, err)

	params := map[string]any{
		"PG_CONN":            "dbname=forma host=localhost",
		"ChangeLogSchema":    "public",
		"ChangeLogScanTable": "change_log",
		"MainSchema":         "public",
		"MainScanTable":      "entity_main_dev",
		"EAVSchema":          "public",
		"EAVScanTable":       "eav_data_dev",
		"DuckDBS3Paths":      []string{parquetPath},
		// Grace disabled so an already-flushed row keeps its out-of-dirty-set
		// semantics (#252) and really is served from parquet.
		"FlushGraceCutoffMs": sqlgen.FlushGraceCutoffDisabled,
		"S3SourceSelect":     sp.S3SourceSelect,
		"PGSourceSelect":     sp.PGSourceSelect,
		"PGGroupBy":          sp.PGGroupBy,
		"EAVPivotSelect":     sp.EAVPivotSelect,
		"EAVPivotAttrs":      sp.EAVPivotAttrs,
		"HasEAVPivot":        sp.EAVPivotAttrs != "",
		"OuterSelect":        sp.OuterSelect,
	}
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 1, Limit: limit, Offset: offset},
	}

	sqlText, args, err := sqlgen.BuildDuckDBQuery(sqlgen.AdvancedQueryTemplateDuckDB, params, q,
		nil, &sqlgen.DualClauses{DuckClause: "1=1"})
	require.NoError(t, err)
	require.Empty(t, args, "the 1=1 fixture clause must bind no placeholders")

	return collapsePostgresScans(t, sqlText)
}

// collapsePostgresScans rewrites every postgres_scan(conn, schema, table) call
// to a bare `table` reference. It re-walks the text with the same paren
// matching sqlgentest uses, because the runtime template spells its
// entity_main scan across several lines — a substring replacement keyed on the
// trimmed argument text would not match.
func collapsePostgresScans(t *testing.T, sqlText string) string {
	t.Helper()

	const marker = "postgres_scan("
	var b strings.Builder
	for i := 0; ; {
		idx := strings.Index(sqlText[i:], marker)
		if idx < 0 {
			b.WriteString(sqlText[i:])
			break
		}
		b.WriteString(sqlText[i : i+idx])
		start := i + idx + len(marker)
		depth := 1
		j := start
		for ; j < len(sqlText) && depth > 0; j++ {
			switch sqlText[j] {
			case '(':
				depth++
			case ')':
				depth--
			}
		}
		args := strings.TrimSpace(sqlText[start : j-1])
		parts := strings.Split(args, ",")
		require.Len(t, parts, 3, "postgres_scan(%s) is not the 3-arg runtime form", args)
		b.WriteString(strings.Trim(strings.TrimSpace(parts[2]), "'"))
		i = j
	}
	return b.String()
}

// createdAtRow is one row of the runtime query's output, read by column name
// so the test does not depend on the positional scan contract (#147).
type createdAtRow struct {
	rowID     string
	createdAt int64
	updatedAt int64
}

// runRuntimeQuery executes the rewritten runtime query and returns its rows in
// result order.
func runRuntimeQuery(t *testing.T, db *sql.DB, query string) []createdAtRow {
	t.Helper()

	rows, err := db.Query(query)
	require.NoError(t, err, "the runtime template must execute as rendered (query=%s)", query)
	defer func() { require.NoError(t, rows.Close()) }()

	cols, err := rows.Columns()
	require.NoError(t, err)

	var out []createdAtRow
	for rows.Next() {
		cells := make([]any, len(cols))
		var r createdAtRow
		var createdAt, updatedAt sql.NullInt64
		var rowID any
		for i, name := range cols {
			switch name {
			case "ltbase_row_id":
				cells[i] = &rowID
			case "ltbase_created_at":
				cells[i] = &createdAt
			case "ltbase_updated_at":
				cells[i] = &updatedAt
			default:
				cells[i] = new(any)
			}
		}
		require.NoError(t, rows.Scan(cells...))

		require.True(t, createdAt.Valid, "ltbase_created_at must never be NULL for a visible row")
		require.True(t, updatedAt.Valid, "ltbase_updated_at must never be NULL for a visible row")
		r.createdAt = createdAt.Int64
		r.updatedAt = updatedAt.Int64
		r.rowID = formatScannedUUID(t, rowID)
		out = append(out, r)
	}
	require.NoError(t, rows.Err())
	return out
}

// formatScannedUUID renders a scanned UUID column as its canonical text form.
// The DuckDB driver hands UUIDs back as their 16 raw bytes.
func formatScannedUUID(t *testing.T, v any) string {
	t.Helper()
	switch typed := v.(type) {
	case string:
		id, err := uuid.FromBytes([]byte(typed))
		require.NoError(t, err)
		return id.String()
	case []byte:
		id, err := uuid.FromBytes(typed)
		require.NoError(t, err)
		return id.String()
	case uuid.UUID:
		return typed.String()
	default:
		require.Failf(t, "unexpected row_id type", "%T", v)
		return ""
	}
}

// uuidLit renders the nth fixture row id; the outer SELECT casts row_id to
// UUID, so the seeds must be real UUIDs.
func uuidLit(n int) string {
	return fmt.Sprintf("018f0000-0000-7000-8000-%012d", n)
}

// seedFlushBoundary creates the local relations and the parquet object for the
// created→updated→flushed scenario.
//
// Every row was created at 1000+n and last updated at 9000+n, so the creation
// stamp and the version stamp are never equal and never even ordered the same
// way: creation ascends with n while the update stamps are deliberately given
// the reverse order. A reader that reports the version stamp as created_at
// therefore produces the REVERSE default order, which no tie-break can hide.
//
//   - rows 1..3 are flushed: they live only in parquet, with their
//     change_log entry marked flushed_at > 0 (out of the dirty set).
//   - rows 4..6 are hot: unflushed change_log entries plus entity_main rows.
//     Their parquet copies exist too (stale versions the dirty set discards),
//     mirroring a row that is being re-written after its first flush.
func seedFlushBoundary(t *testing.T, db *sql.DB, parquetPath string) {
	t.Helper()

	createdAt := func(n int) int64 { return int64(1000 + n) }
	// Reverse-ordered update stamps: row 1 is the oldest creation but the
	// newest update.
	updatedAt := func(n int) int64 { return int64(9000 - n) }

	stmts := []string{
		`CREATE TABLE change_log (
			row_id UUID, schema_id SMALLINT, flushed_at BIGINT,
			changed_at BIGINT, deleted_at BIGINT)`,
		`CREATE TABLE entity_main_dev (
			ltbase_row_id UUID, ltbase_schema_id SMALLINT,
			ltbase_created_at BIGINT, ltbase_updated_at BIGINT, ltbase_deleted_at BIGINT,
			text_01 VARCHAR, integer_01 INTEGER)`,
		`CREATE TABLE eav_data_dev (
			row_id UUID, schema_id SMALLINT, attr_id INTEGER,
			array_indices VARCHAR, value_text VARCHAR, value_numeric DECIMAL(38,9))`,
	}

	var parquetRows []string
	for n := 1; n <= 6; n++ {
		flushed := int64(0)
		if n <= 3 {
			flushed = 5000
		}
		stmts = append(stmts, fmt.Sprintf(
			`INSERT INTO change_log VALUES (UUID '%s', 1, %d, %d, 0)`,
			uuidLit(n), flushed, updatedAt(n)))
		if n > 3 {
			stmts = append(stmts, fmt.Sprintf(
				`INSERT INTO entity_main_dev VALUES (UUID '%s', 1, %d, %d, NULL, 'row-%d', %d)`,
				uuidLit(n), createdAt(n), updatedAt(n), n, n))
			stmts = append(stmts, fmt.Sprintf(
				`INSERT INTO eav_data_dev VALUES (UUID '%s', 1, 205, '', 'developer', NULL)`,
				uuidLit(n)))
		}
		// The parquet copy carries the exported ltbase_created_at alongside the
		// version stamp; for the hot rows it is a stale version the dirty-set
		// anti-join discards.
		version := updatedAt(n)
		if n > 3 {
			version = updatedAt(n) - 100
		}
		parquetRows = append(parquetRows, fmt.Sprintf(
			`(UUID '%s', CAST(%d AS BIGINT), CAST(%d AS BIGINT), CAST(0 AS BIGINT), 'row-%d', %d, 'developer')`,
			uuidLit(n), createdAt(n), version, n, n))
	}

	stmts = append(stmts, fmt.Sprintf(`COPY (
		SELECT * FROM (VALUES %s)
		AS t(row_id, ltbase_created_at, changed_at, deleted_at, name, age, tag)
	) TO '%s' (FORMAT PARQUET)`, strings.Join(parquetRows, ", "), parquetPath))

	for _, stmt := range stmts {
		_, err := db.Exec(stmt)
		require.NoError(t, err, "seed statement failed: %s", stmt)
	}
}

func newFlushBoundaryDuckDB(t *testing.T) (*sql.DB, string) {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	parquetPath := filepath.Join(t.TempDir(), "delta.parquet")
	seedFlushBoundary(t, db, parquetPath)
	return db, parquetPath
}

// TestFederatedCreatedAt_ReportsCreationTimeNotUpdateTime is the first
// acceptance criterion: a created→updated→flushed row reports its creation
// time through the federated route.
func TestFederatedCreatedAt_ReportsCreationTimeNotUpdateTime(t *testing.T) {
	db, parquetPath := newFlushBoundaryDuckDB(t)

	got := runRuntimeQuery(t, db, renderRuntimeQueryForLocalDuckDB(t, parquetPath, 10, 0))
	require.Len(t, got, 6, "every seeded row must be visible")

	for _, r := range got {
		var n int
		_, err := fmt.Sscanf(r.rowID, "018f0000-0000-7000-8000-%012d", &n)
		require.NoError(t, err)
		require.Equal(t, int64(1000+n), r.createdAt,
			"row %s must report its creation time, not its version stamp (#460)", r.rowID)
		require.NotEqual(t, r.updatedAt, r.createdAt,
			"row %s must not collapse created_at onto the LWW version stamp (#460)", r.rowID)
	}
}

// TestFederatedCreatedAt_DefaultOrderIsStableAcrossFlushBoundary is the second
// acceptance criterion: the default ORDER BY created_at DESC, row_id ASC must
// rank flushed (parquet-winning) and unflushed (hot-winning) rows by the same
// quantity, so a page window does not duplicate or skip rows when a flush
// lands mid-pagination.
func TestFederatedCreatedAt_DefaultOrderIsStableAcrossFlushBoundary(t *testing.T) {
	db, parquetPath := newFlushBoundaryDuckDB(t)

	got := runRuntimeQuery(t, db, renderRuntimeQueryForLocalDuckDB(t, parquetPath, 10, 0))

	ids := make([]string, 0, len(got))
	for _, r := range got {
		ids = append(ids, r.rowID)
	}
	// created_at ascends with n, so DESC ranks 6 → 1 regardless of which tier
	// served each row. Under the #460 alias the parquet-winning rows 1..3
	// carried their (reverse-ordered) version stamps instead and led the page.
	want := []string{uuidLit(6), uuidLit(5), uuidLit(4), uuidLit(3), uuidLit(2), uuidLit(1)}
	require.Equal(t, want, ids,
		"the default order must rank hot and parquet rows by the same quantity (#460)")

	// The same order, walked one page at a time, must reproduce the full set
	// with no duplicate and no skip.
	var paged []string
	for offset := 0; offset < 6; offset += 2 {
		page := runRuntimeQuery(t, db, renderRuntimeQueryForLocalDuckDB(t, parquetPath, 2, offset))
		for _, r := range page {
			paged = append(paged, r.rowID)
		}
	}
	require.Equal(t, want, paged,
		"LIMIT/OFFSET windows over the default sort key must tile the result set exactly (#460)")
}
