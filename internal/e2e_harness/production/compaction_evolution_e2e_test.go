//go:build e2e

package production

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/compaction"
	"github.com/lychee-technology/forma/internal/manifest"
)

// #257: mixed-generation compaction equivalence. Base parquet is written
// under v1, delta under v2 — one evolution step carrying all three mutation
// kinds (old_col removed, score retyped integer→numeric, new_col added) — and
// the REAL compactor merges them. The merge SQL's union_by_name (#189,
// internal/compaction/merge_sql.go) must materialize the widened column
// union; every assertion here characterizes behavior already on main.

const evoV1Props = `{
    "name": { "type": "string" },
    "value": { "type": "number" },
    "old_col": { "type": "string" },
    "score": { "type": "integer" }
  }`

const evoV1Attrs = `{
  "name": { "attributeID": 1, "valueType": "text", "column_binding": { "col_name": "text_01" } },
  "value": { "attributeID": 2, "valueType": "numeric" },
  "old_col": { "attributeID": 3, "valueType": "text" },
  "score": { "attributeID": 4, "valueType": "integer" }
}
`

const evoV2Props = `{
    "name": { "type": "string" },
    "value": { "type": "number" },
    "score": { "type": "number" },
    "new_col": { "type": "integer" }
  }`

const evoV2Attrs = `{
  "name": { "attributeID": 1, "valueType": "text", "column_binding": { "col_name": "text_01" } },
  "value": { "attributeID": 2, "valueType": "numeric" },
  "score": { "attributeID": 4, "valueType": "numeric" },
  "new_col": { "attributeID": 6, "valueType": "integer" }
}
`

// buildEvoV1Profile seeds v1 rows: old_col + integer score (score =
// ordinal*10). Every v1 row carries old_col so the base parquet holds a
// non-NULL value for the rows the v2 generation later updates — that is what
// makes the merged winner's old_col-is-NULL assertion prove ROW-level LWW
// rather than pass vacuously.
func buildEvoV1Profile() AttrProfile {
	return buildEvolutionProfile(func(ordinal int) map[string]any {
		return map[string]any{
			"old_col": fmt.Sprintf("old-%04d", ordinal),
			"score":   float64(ordinal * 10),
		}
	})
}

// buildEvoV2Profile seeds v2 rows: new_col + fractional score, so any silent
// DOUBLE→BIGINT coercion in the merge corrupts a visible value.
func buildEvoV2Profile() AttrProfile {
	return buildEvolutionProfile(func(ordinal int) map[string]any {
		return map[string]any{
			"new_col": float64(ordinal * 10),
			"score":   float64(ordinal*10) + 0.5,
		}
	})
}

// buildEvolutionEquivalenceQueries is the before/after snapshot set: an
// unsorted page, a sort on a generation-stable attribute, a score filter
// spanning both generations (v1 BIGINT rows and v2 DOUBLE rows in one
// numeric domain, with the boundary row score=20 included), and a new_col
// filter that only v2-generation rows can match (v1 rows are NULL).
func buildEvolutionEquivalenceQueries(schema SchemaRef) []Query {
	return []Query{
		{Schema: schema, Limit: 100},
		{Schema: schema, Sorts: []Sort{{Attr: "value"}}, Limit: 100},
		{Schema: schema, Filters: []Filter{{Attr: "score", Op: "gte", Value: "20"}}, Limit: 100},
		{Schema: schema, Filters: []Filter{{Attr: "new_col", Op: "gte", Value: "0"}}, Limit: 100},
	}
}

// evolutionSeed carries the mixed-generation state the assertions need.
type evolutionSeed struct {
	baseKey   string   // v1-shaped base parquet
	deltaKey  string   // v2-shaped delta parquet
	creates   []*Event // v1 base creates, ordinals 0-4
	updates   []*Event // v2 winners over creates[3], creates[4]
	deleted   *Event   // tombstone for creates[0]
	v2Creates []*Event // v2 delta creates, ordinals 5-8
}

// seedMixedGenerationTiers builds the #257 fixture: 5 v1 rows exported as
// base via init, evolve to v2, then — all under v2 — 2 updates + 1 delete
// against v1 base rows plus 4 new rows flushed as ONE delta (dirty ratio
// 3/5 = 60% > the 5% rewrite trigger), and 3 hot rows left unflushed.
//
// #294 (fixed): the update path tolerates the dropped old_col's attrID and
// preserves its EAV rows untouched, so the two update targets are updated
// directly — no stale-EAV cleanup migration. The preserved rows stay in
// Postgres but the v2 flush projection never exports old_col, so the merged
// winner's old_col-is-NULL assertion below is unchanged: base (v1) carries
// the non-NULL value, the winning delta row does not.
func seedMixedGenerationTiers(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, v2Dir string) *evolutionSeed {
	t.Helper()
	s := &evolutionSeed{}
	s.creates = seedGeneration(ctx, t, env, schema, 5, buildEvoV1Profile())
	s.baseKey = runInitBase(ctx, t, env, schema)
	if err := env.EvolveSchema(ctx, v2Dir); err != nil {
		t.Fatalf("evolve schema to v2: %v", err)
	}
	s.updates = []*Event{
		UpdateEvent(schema, s.creates[3].RowID, map[string]any{"score": 1000.5, "new_col": float64(400)}),
		UpdateEvent(schema, s.creates[4].RowID, map[string]any{"score": 2000.5, "new_col": float64(500)}),
	}
	s.deleted = DeleteEvent(schema, s.creates[0].RowID)
	if err := env.ApplyEvents(ctx, s.updates[0], s.updates[1], s.deleted); err != nil {
		t.Fatalf("apply v2 updates/delete to v1 base rows: %v", err)
	}
	s.v2Creates = seedGeneration(ctx, t, env, schema, 4, buildEvoV2Profile())
	s.deltaKey = requireSoleParquet(t, "flush", mustFlush(ctx, t, env).NewObjects)
	seedGeneration(ctx, t, env, schema, 3, buildEvoV2Profile()) // hot tier
	return s
}

// buildParquetS3Path renders one parquet object key as the s3:// URI DuckDB
// reads.
func buildParquetS3Path(env *Env, key string) string {
	return fmt.Sprintf("s3://%s/%s", env.Cluster.Bucket, strings.TrimPrefix(key, "/"))
}

// requireBaseOldCol asserts the base parquet physically stores the given
// old_col value for a row — the positive control that makes the merged
// winner's old_col-NULL assertion prove row-level replacement instead of
// passing vacuously on an already-NULL source.
func requireBaseOldCol(ctx context.Context, t *testing.T, env *Env, path string, rowID uuid.UUID, want string) {
	t.Helper()
	var oldCol sql.NullString
	if err := env.Duck.DB.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT MAX(old_col) FROM read_parquet('%s') WHERE CAST(row_id AS VARCHAR) = ?`, path),
		rowID.String()).Scan(&oldCol); err != nil {
		t.Fatalf("scan base parquet old_col for row %s: %v", rowID, err)
	}
	if !oldCol.Valid || oldCol.String != want {
		t.Fatalf("base parquet old_col for row %s = %q (valid=%t), want %q (row-level-LWW positive-control precondition)",
			rowID, oldCol.String, oldCol.Valid, want)
	}
}

// scanMergedEvoRow reads one row's evolved attributes out of the merged base.
func scanMergedEvoRow(ctx context.Context, t *testing.T, env *Env, path string, rowID uuid.UUID) (n int, score sql.NullFloat64, oldCol sql.NullString, newCol sql.NullInt64) {
	t.Helper()
	if err := env.Duck.DB.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT COUNT(*), MAX(score), MAX(old_col), MAX(new_col)
		 FROM read_parquet('%s') WHERE CAST(row_id AS VARCHAR) = ?`, path),
		rowID.String()).Scan(&n, &score, &oldCol, &newCol); err != nil {
		t.Fatalf("scan merged base row %s: %v", rowID, err)
	}
	return n, score, oldCol, newCol
}

// assertMergedBaseUnion pins #257 criterion (b): the merged base's physical
// schema is the monotonic column union with widened types, and its rows are
// exactly the LWW winners — cross-generation folds are ROW-level (a v2
// winner carries old_col NULL; values do not column-merge).
func assertMergedBaseUnion(ctx context.Context, t *testing.T, env *Env, key string, seed *evolutionSeed) {
	t.Helper()
	requireParquetCols(t, "merged base", describeParquetCols(ctx, t, env, key), map[string]string{
		"name":    "VARCHAR",
		"value":   "DOUBLE",
		"old_col": "VARCHAR", // v1 legacy column survives the union
		"new_col": "BIGINT",  // v2 addition present
		"score":   "DOUBLE",  // BIGINT widened to the delta's DOUBLE
	})

	path := buildParquetS3Path(env, key)
	var total, tombstones, nullDeleted int
	if err := env.Duck.DB.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT COUNT(*),
		        COUNT(*) FILTER (WHERE deleted_at > 0),
		        COUNT(*) FILTER (WHERE deleted_at IS NULL)
		 FROM read_parquet('%s')`, path)).Scan(&total, &tombstones, &nullDeleted); err != nil {
		t.Fatalf("scan merged base %s: %v", key, err)
	}
	if total != 8 { // 5 base − 1 deleted + 4 v2 creates
		t.Errorf("merged base holds %d rows, want 8 LWW winners", total)
	}
	if tombstones != 0 || nullDeleted != 0 {
		t.Errorf("merged base holds %d tombstones, %d NULL deleted_at, want 0/0 (dropped and normalized)", tombstones, nullDeleted)
	}

	// v2 winners over v1 rows: fractional score, new_col set, old_col NULL.
	// The base copies carry non-NULL old_col (requireBaseOldCol positive
	// control), so NULL here proves the winner replaced the WHOLE row — a
	// column merge would leak the base values through.
	for i, up := range seed.updates {
		n, score, oldCol, newCol := scanMergedEvoRow(ctx, t, env, path, up.RowID)
		wantScore := up.Attrs["score"].(float64)
		wantNew := int64(up.Attrs["new_col"].(float64))
		if n != 1 || !score.Valid || score.Float64 != wantScore {
			t.Errorf("updated row %d: n=%d score=%v(valid=%t), want 1 row with v2 winner score %v", i, n, score.Float64, score.Valid, wantScore)
		}
		if !newCol.Valid || newCol.Int64 != wantNew {
			t.Errorf("updated row %d: new_col=%v(valid=%t), want %d", i, newCol.Int64, newCol.Valid, wantNew)
		}
		if oldCol.Valid {
			t.Errorf("updated row %d: old_col=%q, want NULL (row-level LWW: the v2 winner replaces the whole row)", i, oldCol.String)
		}
	}

	// Untouched v1 rows keep old_col and their integer-valued score (stored DOUBLE).
	for _, ordinal := range []int{1, 2} {
		row := seed.creates[ordinal]
		n, score, oldCol, newCol := scanMergedEvoRow(ctx, t, env, path, row.RowID)
		if n != 1 || !score.Valid || score.Float64 != float64(ordinal*10) {
			t.Errorf("untouched v1 row %d: n=%d score=%v(valid=%t), want 1 row with score %d", ordinal, n, score.Float64, score.Valid, ordinal*10)
		}
		if !oldCol.Valid || oldCol.String != fmt.Sprintf("old-%04d", ordinal) {
			t.Errorf("untouched v1 row %d: old_col=%q(valid=%t), want %q preserved", ordinal, oldCol.String, oldCol.Valid, fmt.Sprintf("old-%04d", ordinal))
		}
		if newCol.Valid {
			t.Errorf("untouched v1 row %d: new_col=%d, want NULL (attribute never written)", ordinal, newCol.Int64)
		}
	}

	// The deleted v1 row is physically absent.
	if n, _, _, _ := scanMergedEvoRow(ctx, t, env, path, seed.deleted.RowID); n != 0 {
		t.Errorf("deleted row survives the merged base (%d rows), want physically gone", n)
	}
}

// assertMixedGenRewriteSurfaces pins the manifest and hot-tier surfaces after
// the mixed-generation rewrite: exactly one base entry (the merged file),
// zero delta entries, monotonic manifest version, no duplicate entries,
// manifest ⊆ S3 inventory with only retired sources unlisted (#461), and an
// untouched hot tier.
func assertMixedGenRewriteSurfaces(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, mBefore *manifest.Manifest, hotBefore int64) {
	t.Helper()
	mAfter := loadSchemaManifest(ctx, t, env, schema)
	if got := countTier(mAfter, "delta"); got != 0 {
		t.Errorf("delta entries after rewrite = %d, want 0", got)
	}
	if got := countTier(mAfter, "base"); got != 1 {
		t.Errorf("base entries after rewrite = %d, want exactly the merged file", got)
	}
	if mAfter.Version <= mBefore.Version {
		t.Errorf("manifest version %d -> %d, want monotonic advance", mBefore.Version, mAfter.Version)
	}
	assertNoDuplicateManifestEntries(t, mAfter)
	everListed := make(map[string]bool, len(mBefore.Files))
	for _, f := range mBefore.Files {
		everListed[f.Path] = true
	}
	assertManifestMatchesInventory(ctx, t, env, schema, everListed)

	hotAfter, err := env.countUnflushed(ctx)
	if err != nil {
		t.Fatalf("count hot rows after rewrite: %v", err)
	}
	if hotAfter != hotBefore {
		t.Errorf("hot change_log rows %d -> %d across rewrite, want untouched", hotBefore, hotAfter)
	}
}

// TestCompactionMixedGenerationEquivalence covers #257: the real compactor
// over a v1 base + v2 delta (removed old_col, retyped score, added new_col)
// must produce bit-for-bit identical federated results, a union-shaped merged
// base (criterion b, assertMergedBaseUnion), and a still-evolvable schema
// (criterion c, verifyPostCompactionEvolution).
func TestCompactionMixedGenerationEquivalence(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	v1 := writeSimpleSchemaDir(t, evoV1Props, evoV1Attrs)
	v2 := writeSimpleSchemaDir(t, evoV2Props, evoV2Attrs)
	env := NewEnv(t, cluster, WithSchemaDir(v1))
	simple := DefaultSchemaFixtures()[0]

	seed := seedMixedGenerationTiers(ctx, t, env, simple, v2)

	// Generation-shape preconditions: without physically divergent parquet
	// shapes the equivalence pass proves nothing about cross-generation merge.
	baseCols := describeParquetCols(ctx, t, env, seed.baseKey)
	requireParquetCols(t, "base (v1)", baseCols, map[string]string{
		"name": "VARCHAR", "value": "DOUBLE", "old_col": "VARCHAR", "score": "BIGINT"})
	forbidParquetCols(t, "base (v1)", baseCols, "new_col")
	deltaCols := describeParquetCols(ctx, t, env, seed.deltaKey)
	requireParquetCols(t, "delta (v2)", deltaCols, map[string]string{
		"score": "DOUBLE", "new_col": "BIGINT"})
	forbidParquetCols(t, "delta (v2)", deltaCols, "old_col")

	// Positive control for the row-level LWW assertion: the update targets'
	// base copies physically carry old_col, so the merged winner showing NULL
	// (assertMergedBaseUnion) proves whole-row replacement — a column merge
	// would leak these values through.
	basePath := buildParquetS3Path(env, seed.baseKey)
	requireBaseOldCol(ctx, t, env, basePath, seed.creates[3].RowID, "old-0003")
	requireBaseOldCol(ctx, t, env, basePath, seed.creates[4].RowID, "old-0004")

	// Query-set discrimination preconditions: 11 visible entities (4 base
	// survivors + 4 delta creates + 3 hot); score>=20 excludes exactly the
	// untouched v1 row with score 10 and boundary-includes the one with score
	// 20; new_col>=0 excludes both untouched v1 rows.
	queries := buildEvolutionEquivalenceQueries(simple)
	full := env.AssertQueryMatches(ctx, queries[0])
	assertUsesDuckDB(t, full)
	if full != nil && full.Total != 11 {
		t.Fatalf("full scan total = %d, want 11 (4 base survivors + 4 delta + 3 hot)", full.Total)
	}
	if scored := env.AssertQueryMatches(ctx, queries[2]); scored != nil && scored.Total != 10 {
		t.Fatalf("score >= 20 total = %d, want 10 (only the v1 row with score 10 excluded)", scored.Total)
	}
	if newcol := env.AssertQueryMatches(ctx, queries[3]); newcol != nil && newcol.Total != 9 {
		t.Fatalf("new_col >= 0 total = %d, want 9 (2 updated + 4 delta + 3 hot; untouched v1 rows are NULL)", newcol.Total)
	}

	hotBefore, err := env.countUnflushed(ctx)
	if err != nil {
		t.Fatalf("count hot rows: %v", err)
	}
	if hotBefore == 0 {
		t.Fatal("seed produced no hot rows; the hot-tier-untouched assertion would be vacuous")
	}

	mBefore := loadSchemaManifest(ctx, t, env, simple)
	result := assertCompactionEquivalence(ctx, t, env, simple, queries,
		CompactionOverrides{}, "mixed-generation")
	if result.Outcome != compaction.RewriteApplied {
		t.Fatalf("outcome = %s (dirty ratio %.2f), want %s", result.Outcome, result.DirtyRatio, compaction.RewriteApplied)
	}
	if result.RowsIn != 12 { // 5 base + delta(2 updates + 1 tombstone + 4 creates)
		t.Errorf("RowsIn = %d, want 12", result.RowsIn)
	}
	if result.RowsOut != 8 { // 5 − 1 deleted + 4 created
		t.Errorf("RowsOut = %d, want 8", result.RowsOut)
	}
	if result.NewBaseKey == "" {
		t.Fatal("RewriteApplied result carries no NewBaseKey")
	}

	assertMixedGenRewriteSurfaces(ctx, t, env, simple, mBefore, hotBefore)
	assertMergedBaseUnion(ctx, t, env, result.NewBaseKey, seed)
	verifyPostCompactionEvolution(ctx, t, env, simple, seed)
}

// verifyPostCompactionEvolution pins #257 criterion (c): after the mixed-
// generation rewrite, a fresh v2 flush layers a delta on the union-shaped
// merged base and queries still resolve; a second compaction pass folds the
// union-typed base again (monotonic healing), keeping the widened types and
// the v1 legacy column data. The update targets a v2-created row (NOT a v1
// survivor) so both untouched v1 rows keep their old_col values through the
// second merge — the legacy column's DATA must survive repeated folds, not
// just the column.
func verifyPostCompactionEvolution(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, seed *evolutionSeed) {
	t.Helper()
	update := UpdateEvent(schema, seed.v2Creates[0].RowID, map[string]any{"score": 3000.5, "new_col": float64(600)})
	if err := env.ApplyEvents(ctx, update); err != nil {
		t.Fatalf("apply post-compaction v2 update: %v", err)
	}
	seedGeneration(ctx, t, env, schema, 1, buildEvoV2Profile()) // ordinal 12
	// This flush drains the update + new create + the 3 still-hot seed rows
	// into one fresh v2 delta over the merged base.
	mustFlush(ctx, t, env)

	queries := buildEvolutionEquivalenceQueries(schema)
	full := env.AssertQueryMatches(ctx, queries[0])
	assertUsesDuckDB(t, full)
	if full != nil && full.Total != 12 { // 8 merged + 3 ex-hot + 1 new create
		t.Fatalf("post-compaction full scan total = %d, want 12", full.Total)
	}
	for _, q := range queries[1:] {
		env.AssertQueryMatches(ctx, q)
	}

	second := assertCompactionEquivalence(ctx, t, env, schema, queries,
		CompactionOverrides{}, "second-merge")
	if second.Outcome != compaction.RewriteApplied {
		t.Fatalf("second pass outcome = %s (dirty ratio %.2f), want %s (union-typed base must be re-mergeable)", second.Outcome, second.DirtyRatio, compaction.RewriteApplied)
	}
	if second.RowsIn != 13 { // 8 merged base + delta(1 update + 1 create + 3 ex-hot)
		t.Errorf("second-merge RowsIn = %d, want 13", second.RowsIn)
	}
	if second.RowsOut != 12 {
		t.Errorf("second-merge RowsOut = %d, want 12", second.RowsOut)
	}

	// Monotonic healing: the second merge output keeps the union shape.
	requireParquetCols(t, "second merged base", describeParquetCols(ctx, t, env, second.NewBaseKey),
		map[string]string{"old_col": "VARCHAR", "new_col": "BIGINT", "score": "DOUBLE"})
	path := buildParquetS3Path(env, second.NewBaseKey)
	var oldColSurvivors int
	if err := env.Duck.DB.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT COUNT(*) FROM read_parquet('%s') WHERE old_col IS NOT NULL", path)).Scan(&oldColSurvivors); err != nil {
		t.Fatalf("scan second merged base %s: %v", second.NewBaseKey, err)
	}
	if oldColSurvivors != 2 {
		t.Errorf("second merged base has %d rows with old_col data, want 2 (untouched v1 rows must survive repeated folds)", oldColSurvivors)
	}
}
