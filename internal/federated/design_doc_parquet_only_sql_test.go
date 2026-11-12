package federated

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/lychee-technology/forma/internal/sqlgen/sqlgentest"
	"github.com/stretchr/testify/require"
)

// The parquet-only twin of the §5 sketch tests (#854). design.md §6.1.1
// documents the parquet leg of the pgx pass, sqlgen.ParquetOnlyQueryTemplateDuckDB,
// as an executable sketch. These tests keep it honest the way the §5 tests
// keep §5 honest: it reads no PostgreSQL, reads the dirty set from the staged
// table the runtime names, scans through the runtime scan source and
// projection, and, run over §5's seed with the dirty set staged by the
// runtime's own StageDirtySet, answers §5's parquet rows and nothing else.

// extractDesignDocParquetOnlySQL returns the fenced SQL block that follows
// the "parquet leg's statement" paragraph of design.md §6.1.1.
func extractDesignDocParquetOnlySQL(t *testing.T) string {
	t.Helper()

	raw, err := os.ReadFile(filepath.Clean(designDocRelPath))
	require.NoError(t, err, "design.md must be readable from internal/federated")
	doc := string(raw)

	anchor := strings.Index(doc, "**The parquet leg's statement")
	require.GreaterOrEqual(t, anchor, 0, "design.md §6.1.1 must document the parquet leg's statement")

	fence := regexp.MustCompile("(?is)```sql\\s*\n(.*?)```")
	m := fence.FindStringSubmatch(doc[anchor:])
	require.NotNil(t, m, "the parquet leg's statement must be a fenced SQL block")
	return m[1]
}

// designDocTwinRowID gives each §5 scenario row a UUID that still shows its
// name: the letter becomes a hex digit (s1 ends in 51, d1 in d1).
func designDocTwinRowID(name string) string {
	tail := strings.NewReplacer("s", "5").Replace(name)
	return "00000000-0000-0000-0000-" + strings.Repeat("0", 12-len(tail)) + tail
}

func TestDesignDocParquetOnlySQL_ReadsNoPostgres(t *testing.T) {
	sqlText := extractDesignDocParquetOnlySQL(t)

	require.Empty(t, sqlgentest.FindPostgresTableRefs(sqlText), "the parquet leg reads no attached catalog")
	for _, absent := range []string{"pg_source", "change_log", "$PG", "flushed_at", "$FLUSH_GRACE_CUTOFF_MS"} {
		require.NotContains(t, sqlText, absent)
	}
	require.Contains(t, sqlText, "NOT IN (SELECT row_id FROM temp.main."+sqlgen.StagedDirtyIDsTable+")",
		"the anti-join reads the table StageDirtySet writes")
}

// The sketch's scan source and projection are the runtime's, as §5's are.
func TestDesignDocParquetOnlySQL_ScanAndProjectionMatchRuntime(t *testing.T) {
	sqlText := extractDesignDocParquetOnlySQL(t)

	want := sqlgen.BuildParquetScanSource("$S3_PATHS", nil, nil)
	require.Equal(t, 2, strings.Count(sqlText, want),
		"the sketch must render sqlgen.BuildParquetScanSource at both scan sites")

	require.ElementsMatch(t, extractDocS3ProjectionItems(t, extractDesignDocSection5SQL(t)),
		extractDocS3ProjectionItems(t, sqlText),
		"the parquet leg projects what §5's s3_source projects, which TestDesignDocSQL_S3ProjectionAliasesMatchRuntime pins to the runtime")
}

// Every ORDER BY term of the sketch spells out NULLS LAST, as the runtime's do.
func TestDesignDocParquetOnlySQL_OrderIsNullsLast(t *testing.T) {
	sqlText := extractDesignDocParquetOnlySQL(t)

	m := regexp.MustCompile(`(?m)^ORDER BY (.*)$`).FindStringSubmatch(sqlText)
	require.NotNil(t, m, "the sketch must order its rows")
	for _, term := range strings.Split(m[1], ",") {
		require.True(t, strings.HasSuffix(strings.TrimSpace(term), "NULLS LAST"), "ORDER BY term %q", term)
	}
}

// runDesignDocParquetOnlySQL runs the sketch over §5's seed, with the dirty
// set (the change_log rows with flushed_at = 0: d1 and d2) staged through
// StageDirtySet on the connection the statement runs on.
func runDesignDocParquetOnlySQL(t *testing.T, windowLimit, windowOffset int) []designDocTwinRow {
	t.Helper()
	ctx := context.Background()
	parquetPath := filepath.Join(t.TempDir(), "base.parquet")

	db, err := sql.Open("duckdb", strictLambdaDSN)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	seedDesignDocScenario(t, db, parquetPath, designDocTwinRowID)

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	require.NoError(t, StageDirtySet(ctx, conn, []uuid.UUID{
		uuid.MustParse(designDocTwinRowID("d1")), uuid.MustParse(designDocTwinRowID("d2")),
	}))

	query := strings.NewReplacer(
		"$S3_PATHS", "['"+parquetPath+"']",
		"$WINDOW_LIMIT", fmt.Sprint(windowLimit),
		"$WINDOW_OFFSET", fmt.Sprint(windowOffset),
	).Replace(extractDesignDocParquetOnlySQL(t))
	require.NotContains(t, query, "$", "all placeholders must be substituted before execution")

	rows, err := conn.QueryContext(ctx, query)
	require.NoError(t, err, "the parquet leg's sketch must execute as written (query=%s)", query)
	defer func() { require.NoError(t, rows.Close()) }()
	var got []designDocTwinRow
	for rows.Next() {
		var r designDocTwinRow
		require.NoError(t, rows.Scan(&r.rowID, &r.name, &r.age, &r.tag, &r.createdAt, &r.total))
		got = append(got, r)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, DropStagedDirtySet(ctx, conn))
	return got
}

type designDocTwinRow struct {
	rowID, name, tag string
	age              int
	createdAt, total int64
}

// §5 answers d1 from PostgreSQL and s3 from parquet. The parquet leg answers
// s3 alone: d1's flushed copy is dropped by the staged set, and Go serves its
// hot version. s1, s2 and s4 stay out for §5's reasons (#173/#178).
func TestDesignDocParquetOnlySQL_ExecutesWithLWWFilterSemantics(t *testing.T) {
	want := designDocTwinRow{rowID: designDocTwinRowID("s3"), name: "John Base", age: 40, tag: "developer", createdAt: 80, total: 1}

	require.Equal(t, []designDocTwinRow{want}, runDesignDocParquetOnlySQL(t, 100, 0),
		"the parquet leg must keep filter-after-LWW and drop every dirty row's parquet copies")

	// A window past the last match returns no row, and so no count: the #181
	// case, which the merge resolves with its recount.
	require.Empty(t, runDesignDocParquetOnlySQL(t, 100, 1))
}
