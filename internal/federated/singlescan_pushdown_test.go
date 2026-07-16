package federated

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
)

// #195 evidence harness: compares s3_source predicate-pushdown formulations
// on identical parquet data. Each form is wrapped in the same
// ranked/visible pipeline as AdvancedQueryTemplateDuckDB, with the hot tier
// and dirty set elided — the #195 rewrite only touches how s3_source
// narrows the dedup input, and dirty exclusion removes whole row_id
// partitions identically under every form. The post-lww-only form applies
// no pushdown at all, so it is trivially correct (pure filter-after-LWW)
// and serves as the ground truth the other forms must agree with.

type scanForm struct {
	name     string
	s3Source func(pathsLiteral, predicate string) string
}

func scanForms() []scanForm {
	return []scanForm{
		{"semijoin-current", semijoinS3Source},
		{"qualify-single-scan", qualifyS3Source},
		{"post-lww-only", postLWWOnlyS3Source},
	}
}

// semijoinS3Source mirrors the production template (PR #191): the parquet
// glob is scanned twice — once for the row_id semijoin, once for the read.
func semijoinS3Source(paths, predicate string) string {
	return fmt.Sprintf(`
  SELECT *, 1 AS source_tier_priority
  FROM read_parquet(%[1]s, union_by_name=true)
  WHERE row_id IN (
    SELECT row_id FROM read_parquet(%[1]s, union_by_name=true)
    WHERE (%[2]s)
  )`, paths, predicate)
}

// qualifyS3Source is the #195 candidate: one scan, any-version match via a
// window over the row_id partition.
func qualifyS3Source(paths, predicate string) string {
	return fmt.Sprintf(`
  SELECT *, 1 AS source_tier_priority
  FROM read_parquet(%s, union_by_name=true)
  QUALIFY MAX(CASE WHEN (%s) THEN 1 ELSE 0 END) OVER (PARTITION BY row_id) = 1`,
		paths, predicate)
}

// postLWWOnlyS3Source is the no-pushdown control: every version enters
// dedup, the predicate applies only in visible.
func postLWWOnlyS3Source(paths, _ string) string {
	return fmt.Sprintf(`
  SELECT *, 1 AS source_tier_priority
  FROM read_parquet(%s, union_by_name=true)`, paths)
}

// scanFormQuery wraps one s3_source formulation in the template's
// ranked/visible pipeline and outer-select shape: COUNT(*) OVER() forces
// full pipeline evaluation, ORDER BY + LIMIT mirror production page reads.
func scanFormQuery(s3Source, predicate string, limit int) string {
	return fmt.Sprintf(`WITH
s3_source AS (%s),
ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY row_id
      ORDER BY ver_ts DESC, source_tier_priority DESC, deleted_ts DESC, row_id ASC
    ) AS rn
  FROM s3_source
),
visible AS (
  SELECT *
  FROM ranked
  WHERE rn = 1
    AND (deleted_ts IS NULL OR deleted_ts = 0)
    AND (%s)
)
SELECT row_id, symbol, trade_type, COUNT(*) OVER() AS total_records
FROM visible
ORDER BY ver_ts DESC, row_id ASC
LIMIT %d`, s3Source, predicate, limit)
}

// seedScanFormParquet writes rows*3 versioned records across nFiles parquet
// shards under dir and returns the DuckDB path-list literal. Row classes by
// row-number residue mod 100 (version v ∈ {0,1,2}, ver_ts = (v+1)*100):
//
//	residue 1 — stable match: symbol SYM00001 in every version → visible;
//	residue 2 — resurrection trap (#173): v0/v1 match, v2 flips to FLIPPED
//	            → must NOT be visible under symbol = 'SYM00001';
//	residue 3 — tombstone trap: every version matches, v2 is a tombstone
//	            → must NOT be visible;
//	others    — noise symbols SYMnnnnn (n = residue), never match.
//
// trade_type cycles i%7 per physical row, so versions of one row differ —
// the broad predicate exercises version-varying values on every class.
func seedScanFormParquet(t testing.TB, db *sql.DB, dir string, rows, nFiles int) string {
	t.Helper()
	seedSQL := fmt.Sprintf(`CREATE OR REPLACE TABLE scanform_seed AS
SELECT
  'row-' || LPAD((i %% %[1]d)::VARCHAR, 9, '0') AS row_id,
  ((i // %[1]d) + 1) * 100 AS ver_ts,
  100::BIGINT AS created_at,
  CASE WHEN (i %% %[1]d) %% 100 = 3 AND (i // %[1]d) = 2
       THEN 300::BIGINT ELSE 0::BIGINT END AS deleted_ts,
  CASE
    WHEN (i %% %[1]d) %% 100 IN (1, 3) THEN 'SYM00001'
    WHEN (i %% %[1]d) %% 100 = 2 THEN
      CASE WHEN (i // %[1]d) = 2 THEN 'FLIPPED' ELSE 'SYM00001' END
    ELSE 'SYM' || LPAD(((i %% %[1]d) %% 100)::VARCHAR, 5, '0')
  END AS symbol,
  (i %% 7)::INTEGER AS trade_type
FROM range(3 * %[1]d) t(i)`, rows)
	if _, err := db.Exec(seedSQL); err != nil {
		t.Fatalf("failed to seed scanform table (%d rows): %v", rows, err)
	}
	for k := 0; k < nFiles; k++ {
		file := filepath.Join(dir, fmt.Sprintf("part-%02d.parquet", k))
		copySQL := fmt.Sprintf(
			`COPY (SELECT * FROM scanform_seed WHERE hash(row_id) %% %d = %d)
TO '%s' (FORMAT PARQUET)`, nFiles, k, file)
		if _, err := db.Exec(copySQL); err != nil {
			t.Fatalf("failed to write parquet shard %d: %v", k, err)
		}
	}
	return "['" + filepath.Join(dir, "*.parquet") + "']"
}

func openScanFormDB(t testing.TB) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("failed to open in-memory duckdb: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("failed to close duckdb: %v", err)
		}
	})
	return db
}

type scanFormRow struct {
	RowID     string
	Symbol    string
	TradeType int
	Total     int64
}

func queryScanForm(t testing.TB, db *sql.DB, query string, args ...any) []scanFormRow {
	t.Helper()
	rows, err := db.Query(query, args...)
	if err != nil {
		t.Fatalf("scan-form query failed: %v (query=%s)", err, query)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("failed to close scan-form rows: %v", err)
		}
	}()
	var out []scanFormRow
	for rows.Next() {
		var r scanFormRow
		if err := rows.Scan(&r.RowID, &r.Symbol, &r.TradeType, &r.Total); err != nil {
			t.Fatalf("failed to scan scan-form row: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("scan-form row iteration failed: %v", err)
	}
	return out
}

func rowIDSet(rows []scanFormRow) map[string]bool {
	ids := make(map[string]bool, len(rows))
	for _, r := range rows {
		ids[r.RowID] = true
	}
	return ids
}

// TestScanForms_AgreeOnVisibleSet is the #195 equivalence guard: every
// formulation must produce the identical visible set, with post-lww-only as
// the trivially-correct reference, and the #173 traps pinned explicitly.
func TestScanForms_AgreeOnVisibleSet(t *testing.T) {
	db := openScanFormDB(t)
	paths := seedScanFormParquet(t, db, t.TempDir(), 30000, 8)

	predicates := []struct{ name, expr string }{
		{"selective", `symbol = 'SYM00001'`},
		{"broad", `trade_type = 0`},
	}
	for _, pred := range predicates {
		t.Run(pred.name, func(t *testing.T) {
			var reference []scanFormRow
			for _, form := range scanForms() {
				got := queryScanForm(t, db,
					scanFormQuery(form.s3Source(paths, pred.expr), pred.expr, 1000000))
				if reference == nil {
					reference = got
					continue
				}
				require.Equal(t, reference, got,
					"form %s must return the identical visible set", form.name)
			}
			if pred.name == "selective" {
				// 30000 logical rows, residue mod 100 == 1 → rows 1, 101, …,
				// 29901: exactly 300 stable-match rows; residue 2/3 all trapped.
				require.Len(t, reference, 300)
				ids := rowIDSet(reference)
				require.True(t, ids["row-000000101"], "stable-match row must surface")
				require.False(t, ids["row-000000102"],
					"resurrection trap: newest version fails the filter (#173)")
				require.False(t, ids["row-000000103"],
					"tombstone trap: deleted LWW winner must drop")
			}
		})
	}
}

// TestScanForms_QualifyBindsPreparedArgs pins that a parameter placeholder
// inside the QUALIFY window CASE binds correctly — the production template
// renders the logical clause with ? placeholders in exactly this position
// (once in s3_source, once in visible; args are appended twice, in order).
func TestScanForms_QualifyBindsPreparedArgs(t *testing.T) {
	db := openScanFormDB(t)
	paths := seedScanFormParquet(t, db, t.TempDir(), 5000, 4)

	literal := queryScanForm(t, db,
		scanFormQuery(qualifyS3Source(paths, `symbol = 'SYM00001'`), `symbol = 'SYM00001'`, 1000000))
	prepared := queryScanForm(t, db,
		scanFormQuery(qualifyS3Source(paths, `symbol = ?`), `symbol = ?`, 1000000),
		"SYM00001", "SYM00001")
	require.Equal(t, literal, prepared,
		"? placeholders inside QUALIFY must bind identically to literals")
}

// BenchmarkScanForms measures each formulation on local parquet shards.
// Scale via FORMA_SCANFORM_ROWS (logical rows; ×3 physical versions).
// Local NVMe understates the second scan's S3 GET amplification — the
// medium-live harness run (RustFS-backed) covers that dimension.
func BenchmarkScanForms(b *testing.B) {
	rows := 200000
	if env := os.Getenv("FORMA_SCANFORM_ROWS"); env != "" {
		parsed, err := strconv.Atoi(env)
		if err != nil {
			b.Fatalf("invalid FORMA_SCANFORM_ROWS %q: %v", env, err)
		}
		rows = parsed
	}
	db := openScanFormDB(b)
	paths := seedScanFormParquet(b, db, b.TempDir(), rows, 8)

	predicates := []struct{ name, expr string }{
		{"selective", `symbol = 'SYM00001'`},
		{"broad", `trade_type = 0`},
	}
	for _, pred := range predicates {
		for _, form := range scanForms() {
			b.Run(pred.name+"/"+form.name, func(b *testing.B) {
				query := scanFormQuery(form.s3Source(paths, pred.expr), pred.expr, 20)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_ = queryScanForm(b, db, query)
				}
			})
		}
	}
}
