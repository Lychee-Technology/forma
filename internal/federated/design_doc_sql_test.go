package federated

import (
	"database/sql"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
)

// These tests are the #214 executable-documentation guard for the §5 "SQL
// Execution Template" sketch in docs/federated-query/design.md. The sketch
// calls itself the core logic of the Federated Query Engine, so it must stay
// executable against the runtime source contract:
//
//   - every postgres_scan is the 3-arg (connection, schema, table) form used
//     by AdvancedQueryTemplateDuckDB — never a 2-arg form or a predicate/query
//     smuggled into the table argument;
//   - the S3 projection uses the runtime parquet columns from
//     buildS3Projection (changed_at AS created_at / ver_ts, deleted_at AS
//     deleted_ts), not phantom ltbase_* columns;
//   - the documented query preserves the #173/#178 filter-after-LWW
//     semantics that PR #211 encoded.
//
// TestDesignDocSQL_ExecutesWithLWWFilterSemantics enforces the above
// behaviorally: postgres_scan calls are mapped to same-named local relations
// (a malformed call maps to nothing and fails the parse assertion), the
// $S3_PATHS placeholder points at a real parquet file, and the whole block
// runs on an in-memory DuckDB against seed data built for the #173 scenarios.

const designDocRelPath = "../../docs/federated-query/design.md"

// extractDesignDocSection5SQL returns the fenced SQL block of design.md §5.
// It anchors on the section heading (not line numbers, which drift) and
// captures the first ```sql fence after it.
func extractDesignDocSection5SQL(t *testing.T) string {
	t.Helper()

	raw, err := os.ReadFile(filepath.Clean(designDocRelPath))
	require.NoError(t, err, "design.md must be readable from internal/federated")
	doc := string(raw)

	heading := regexp.MustCompile(`(?m)^##\s+\*\*5\..*$`)
	loc := heading.FindStringIndex(doc)
	require.NotNil(t, loc, "design.md must contain the §5 heading (## **5. ...**)")

	fence := regexp.MustCompile("(?is)```sql\\s*\n(.*?)```")
	m := fence.FindStringSubmatch(doc[loc[1]:])
	require.NotNil(t, m, "design.md §5 must contain a fenced SQL block")
	return m[1]
}

// findPostgresScanCalls returns the raw argument text of every postgres_scan
// invocation in sqlText, matching parentheses so multi-line and nested forms
// are captured whole.
func findPostgresScanCalls(sqlText string) []string {
	const marker = "postgres_scan("
	var calls []string
	for i := 0; ; {
		idx := strings.Index(sqlText[i:], marker)
		if idx < 0 {
			break
		}
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
		calls = append(calls, strings.TrimSpace(sqlText[start:j-1]))
		i = j
	}
	return calls
}

// threeArgScanRe is the only valid documented postgres_scan shape: the
// $PG_CONN placeholder plus two single-quoted identifiers (schema, table).
// A predicate or a dynamic SELECT in any argument does not match.
var threeArgScanRe = regexp.MustCompile(
	`^\$PG_CONN\s*,\s*'([A-Za-z_][A-Za-z0-9_]*)'\s*,\s*'([A-Za-z_][A-Za-z0-9_]*)'$`)

// TestDesignDocSQL_PostgresScanIsThreeArg pins the runtime scan arity: every
// documented postgres_scan must be the 3-arg (connection, schema, table) form
// of AdvancedQueryTemplateDuckDB.
func TestDesignDocSQL_PostgresScanIsThreeArg(t *testing.T) {
	sqlText := extractDesignDocSection5SQL(t)

	calls := findPostgresScanCalls(sqlText)
	require.NotEmpty(t, calls, "§5 must document postgres_scan usage")

	for _, args := range calls {
		require.Regexp(t, threeArgScanRe, args,
			"every §5 postgres_scan must be the 3-arg ($PG_CONN, 'schema', 'table') runtime form; "+
				"predicates such as flushed_at = 0 belong in WHERE, not in a scan argument")
	}
}

// TestDesignDocSQL_S3ProjectionAliasesMatchRuntime pins the §5 S3 source
// projection to the runtime parquet schema from buildS3Projection
// (duckdb_schema_projection.go): one physical changed_at column feeds both
// created_at and ver_ts, and deleted_at feeds deleted_ts.
func TestDesignDocSQL_S3ProjectionAliasesMatchRuntime(t *testing.T) {
	sqlText := extractDesignDocSection5SQL(t)

	for _, alias := range []string{
		"changed_at AS created_at",
		"changed_at AS ver_ts",
		"deleted_at AS deleted_ts",
	} {
		require.Contains(t, sqlText, alias,
			"§5 S3 projection must use the runtime parquet column aliases")
	}
	for _, phantom := range []string{"ltbase_updated_at", "ltbase_deleted_at"} {
		require.NotContains(t, sqlText, phantom,
			"parquet files have no %s column; §5 must not project it", phantom)
	}
}

// rewriteDocSQLForLocalExecution turns the §5 sketch into a self-contained
// DuckDB statement: each postgres_scan($PG_CONN, 'schema', 'table') collapses
// to its table name (bound to a local seed relation), and the remaining $
// placeholders get concrete values. The table-name mapping is derived from
// the call itself, so renames in the doc adapt automatically — while any
// malformed scan form fails the arity assertion here.
func rewriteDocSQLForLocalExecution(t *testing.T, sqlText, parquetPath string) string {
	t.Helper()

	for _, args := range findPostgresScanCalls(sqlText) {
		m := threeArgScanRe.FindStringSubmatch(args)
		require.NotNil(t, m,
			"cannot map postgres_scan(%s) to a local relation: not the 3-arg runtime form", args)
		sqlText = strings.Replace(sqlText, "postgres_scan("+args+")", m[2], 1)
	}

	replacements := [][2]string{
		{"$S3_PATHS", "['" + parquetPath + "']"},
		{"$SCHEMA_ID", "1"},
		{"$PG_WHERE_CLAUSE", "1 = 1"},
		{"$PAGE_SIZE", "100"},
		{"$OFFSET", "0"},
		// Scan calls collapse to local relations above; this only clears the
		// parameter-legend comment lines.
		{"$PG_CONN", "(local)"},
	}
	for _, r := range replacements {
		sqlText = strings.ReplaceAll(sqlText, r[0], r[1])
	}
	require.NotContains(t, sqlText, "$",
		"all §5 placeholders must be substituted before execution")
	return sqlText
}

// seedDesignDocScenario builds the local relations the rewritten §5 SQL scans
// and a real parquet file for read_parquet. The data encodes the #173/#178
// scenarios; the documented logical filter is
// (age > 18 AND name LIKE 'John%' AND tag = 'developer'):
//
//   - s1: old parquet version matches, newer one fails the filter — the newer
//     version must win LWW and the row must NOT surface (#173).
//   - s2: old live version matches, newer version is a tombstone — the
//     tombstone must win dedup and then be dropped.
//   - s3: single matching parquet version, plus an already-flushed change_log
//     row — it must surface from S3 (flushed_at = 0 keeps it out of the
//     dirty set).
//   - s4: single non-matching parquet version — the semijoin excludes it.
//   - d1: stale matching parquet version AND an unflushed hot version — the
//     dirty-set anti-join must serve the hot values (age 30, created_at 90).
//   - d2: unflushed hot version that fails the filter — dropped post-LWW.
func seedDesignDocScenario(t *testing.T, db *sql.DB, parquetPath string) {
	t.Helper()

	stmts := []string{
		`CREATE TABLE change_log (
			row_id VARCHAR, schema_id INTEGER, flushed_at BIGINT,
			changed_at BIGINT, deleted_at BIGINT)`,
		`INSERT INTO change_log VALUES
			('d1', 1, 0, 300, 0),
			('d2', 1, 0, 300, 0),
			('s3', 1, 500, 100, 0)`,
		`CREATE TABLE entity_main_dev (
			ltbase_row_id VARCHAR, ltbase_schema_id INTEGER, ltbase_created_at BIGINT,
			text_01 VARCHAR, integer_01 INTEGER)`,
		`INSERT INTO entity_main_dev VALUES
			('d1', 1, 90, 'John Dirty', 30),
			('d2', 1, 95, 'Bob', 40)`,
		`CREATE TABLE eav_data_dev (
			row_id VARCHAR, schema_id INTEGER, attr_id INTEGER, value_text VARCHAR)`,
		`INSERT INTO eav_data_dev VALUES
			('d1', 1, 205, 'developer'),
			('d2', 1, 205, 'developer')`,
		`COPY (
			SELECT * FROM (VALUES
				('s1', CAST(100 AS BIGINT), CAST(0 AS BIGINT), 'John Stale', 25, 'developer'),
				('s1', CAST(200 AS BIGINT), CAST(0 AS BIGINT), 'John Newer', 15, 'developer'),
				('s2', CAST(100 AS BIGINT), CAST(0 AS BIGINT), 'John Alive', 30, 'developer'),
				('s2', CAST(200 AS BIGINT), CAST(200 AS BIGINT), 'John Gone', 30, 'developer'),
				('s3', CAST(100 AS BIGINT), CAST(0 AS BIGINT), 'John Base', 40, 'developer'),
				('s4', CAST(100 AS BIGINT), CAST(0 AS BIGINT), 'Alice', 50, 'developer'),
				('d1', CAST(100 AS BIGINT), CAST(0 AS BIGINT), 'John Flushed', 25, 'developer')
			) AS t(row_id, changed_at, deleted_at, name, age, tag)
		) TO '` + parquetPath + `' (FORMAT PARQUET)`,
	}
	for _, stmt := range stmts {
		_, err := db.Exec(stmt)
		require.NoError(t, err, "seed statement failed: %s", stmt)
	}
}

// TestDesignDocSQL_ExecutesWithLWWFilterSemantics executes the §5 sketch on a
// real DuckDB engine and asserts the #173/#178 filter-after-LWW semantics it
// documents. See seedDesignDocScenario for the per-row expectations.
func TestDesignDocSQL_ExecutesWithLWWFilterSemantics(t *testing.T) {
	docSQL := extractDesignDocSection5SQL(t)
	parquetPath := filepath.Join(t.TempDir(), "base.parquet")

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	seedDesignDocScenario(t, db, parquetPath)

	query := rewriteDocSQLForLocalExecution(t, docSQL, parquetPath)
	rows, err := db.Query(query)
	require.NoError(t, err, "the documented §5 SQL must execute as written (query=%s)", query)
	defer rows.Close()

	type resultRow struct {
		rowID, name, tag string
		age              int
		createdAt        int64
	}
	var got []resultRow
	for rows.Next() {
		var r resultRow
		require.NoError(t, rows.Scan(&r.rowID, &r.name, &r.age, &r.tag, &r.createdAt))
		got = append(got, r)
	}
	require.NoError(t, rows.Err())

	// ORDER BY created_at DESC: s3 (100, from parquet changed_at) precedes
	// d1 (90, from entity_main.ltbase_created_at).
	require.Equal(t, []resultRow{
		{rowID: "s3", name: "John Base", age: 40, tag: "developer", createdAt: 100},
		{rowID: "d1", name: "John Dirty", age: 30, tag: "developer", createdAt: 90},
	}, got,
		"§5 must keep filter-after-LWW: s1 (newer non-matching version) and s2 "+
			"(tombstone winner) must not resurface; d1 must carry hot-tier values")
}
