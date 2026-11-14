package federated

import (
	"database/sql"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/stretchr/testify/require"
)

// These tests are the #214 executable-documentation guard for the §5 "SQL
// Execution Template" sketch in docs/federated-query/design.md. Since #858 §5
// documents the one statement DuckDB runs per federated pass, the parquet
// leg's (sqlgen.ParquetOnlyQueryTemplateDuckDB), and it must stay executable
// against the runtime source contract:
//
//   - the S3 projection uses the runtime parquet columns from
//     buildS3Projection (ltbase_created_at AS created_at, changed_at AS
//     ver_ts, deleted_at AS deleted_ts) — created_at and ver_ts come from
//     different parquet columns because they are different quantities (#460);
//   - both scan sites render the runtime's guarded scan source (#256);
//   - the documented query preserves the #173/#178 filter-after-LWW
//     semantics that PR #211 encoded.
//
// design_doc_parquet_only_sql_test.go checks that it reads no PostgreSQL and
// runs it on an in-memory DuckDB over seedDesignDocScenario, with the dirty
// set staged by the runtime's own StageDirtySet.

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

// extractDocS3ProjectionItems returns the individual SELECT items of the §5
// s3_source CTE, with SQL comments stripped. The doc's source_tier_priority
// literal is kept: the runtime template appends it outside SchemaProjection,
// so the caller decides how to treat it.
func extractDocS3ProjectionItems(t *testing.T, sqlText string) []string {
	t.Helper()

	// Anchored on the CTE-level FROM (line-leading), not on `FROM read_parquet`:
	// since #256 the scan source is a subquery that contains its own
	// `FROM read_parquet`, which would swallow the guard expression into the
	// projection items.
	block := regexp.MustCompile(`(?s)s3_source AS \(\s*SELECT\b(.*?)\n\s*FROM `).
		FindStringSubmatch(sqlText)
	require.NotNil(t, block, "§5 must contain an s3_source CTE with a FROM clause")

	var cleaned []string
	for _, line := range strings.Split(block[1], "\n") {
		if idx := strings.Index(line, "--"); idx >= 0 {
			line = line[:idx]
		}
		cleaned = append(cleaned, line)
	}

	var items []string
	for _, item := range strings.Split(strings.Join(cleaned, "\n"), ",") {
		if item = strings.TrimSpace(item); item != "" {
			items = append(items, item)
		}
	}
	return items
}

// TestDesignDocSQL_S3ProjectionAliasesMatchRuntime pins the §5 S3 source
// projection to the runtime projection itself: it derives the expected
// SELECT items from sqlgen.BuildSchemaProjection for the schema the doc
// example uses (name/age main-bound, tag pure-EAV), so a change to
// buildS3Projection fails this guard instead of leaving §5 silently stale.
func TestDesignDocSQL_S3ProjectionAliasesMatchRuntime(t *testing.T) {
	sqlText := extractDesignDocSection5SQL(t)

	items := extractDocS3ProjectionItems(t, sqlText)
	require.Contains(t, items, "1 AS source_tier_priority",
		"§5 s3_source must emit source_tier_priority: the ranked ORDER BY consumes it (#211)")

	docProjection := make([]string, 0, len(items)-1)
	for _, item := range items {
		if item != "1 AS source_tier_priority" {
			docProjection = append(docProjection, item)
		}
	}

	cache := forma.SchemaAttributeCache{
		"name": {AttributeID: 1, ValueType: forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_01")}},
		"age": {AttributeID: 2, ValueType: forma.ValueTypeInteger,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("integer_01")}},
		"tag": {AttributeID: 205, ValueType: forma.ValueTypeText},
	}
	sp, err := sqlgen.BuildSchemaProjection(1, cache)
	require.NoError(t, err)

	require.ElementsMatch(t, strings.Split(sp.S3SourceSelect, ", "), docProjection,
		"§5 s3_source projection must match the runtime SchemaProjection.S3SourceSelect "+
			"for the documented name/age/tag schema (attribute order aside)")
}

// TestDesignDocSQL_ScanSourceMatchesRuntimeGuard pins §5's parquet scan text
// to the runtime builder itself, at both scan sites. The #256 row_id guard is
// what keeps a stamp-trusted rogue object from vanishing under union_by_name,
// so a doc that still showed the bare read_parquet form would document a
// silent-loss path the engine no longer has — and the guard is only correct if
// BOTH sites carry it (the semijoin decides which rows qualify at all).
func TestDesignDocSQL_ScanSourceMatchesRuntimeGuard(t *testing.T) {
	sqlText := extractDesignDocSection5SQL(t)

	// The doc's path placeholder stands where the renderer splices its path
	// list, so the rest of the source text must match byte for byte.
	want := sqlgen.BuildParquetScanSource("$S3_PATHS", nil, nil)
	require.Equal(t, 2, strings.Count(sqlText, want),
		"§5 must render sqlgen.BuildParquetScanSource at both scan sites (s3_source FROM and the pushdown semijoin); got:\n%s", want)
}

// seedDesignDocScenario writes the parquet file §5's read_parquet scans. The
// data encodes the #173/#178 scenarios; the documented logical filter is
// (age > 18 AND name LIKE 'John%' AND tag = 'developer'):
//
//   - s1: old parquet version matches, newer one fails the filter — the newer
//     version must win LWW and the row must NOT surface (#173).
//   - s2: old live version matches, newer version is a tombstone — the
//     tombstone must win dedup and then be dropped.
//   - s3: single matching parquet version, flushed and out of the dirty set —
//     it must surface.
//   - s4: single non-matching parquet version — the semijoin excludes it.
//   - d1: stale matching parquet version of a row in the dirty set — the
//     anti-join must drop it, and the hot leg serves the row.
//
// Every parquet row carries an ltbase_created_at distinct from its changed_at,
// so a leg that aliased the version stamp into the created_at slot (#460)
// would report the wrong value.
//
// rowID maps each scenario name to the row_id the seed writes: the anti-join
// compares row_id as a UUID against the staged set.
func seedDesignDocScenario(t *testing.T, db *sql.DB, parquetPath string, rowID func(string) string) {
	t.Helper()
	id := rowID

	stmts := []string{
		`COPY (
			SELECT * FROM (VALUES
				('` + id("s1") + `', CAST(50 AS BIGINT), CAST(100 AS BIGINT), CAST(0 AS BIGINT), 'John Stale', 25, 'developer'),
				('` + id("s1") + `', CAST(50 AS BIGINT), CAST(200 AS BIGINT), CAST(0 AS BIGINT), 'John Newer', 15, 'developer'),
				('` + id("s2") + `', CAST(50 AS BIGINT), CAST(100 AS BIGINT), CAST(0 AS BIGINT), 'John Alive', 30, 'developer'),
				('` + id("s2") + `', CAST(50 AS BIGINT), CAST(200 AS BIGINT), CAST(200 AS BIGINT), 'John Gone', 30, 'developer'),
				('` + id("s3") + `', CAST(80 AS BIGINT), CAST(100 AS BIGINT), CAST(0 AS BIGINT), 'John Base', 40, 'developer'),
				('` + id("s4") + `', CAST(50 AS BIGINT), CAST(100 AS BIGINT), CAST(0 AS BIGINT), 'Alice', 50, 'developer'),
				('` + id("d1") + `', CAST(90 AS BIGINT), CAST(100 AS BIGINT), CAST(0 AS BIGINT), 'John Flushed', 25, 'developer')
			) AS t(row_id, ltbase_created_at, changed_at, deleted_at, name, age, tag)
		) TO '` + parquetPath + `' (FORMAT PARQUET)`,
	}
	for _, stmt := range stmts {
		_, err := db.Exec(stmt)
		require.NoError(t, err, "seed statement failed: %s", stmt)
	}
}
