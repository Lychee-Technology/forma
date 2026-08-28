//go:build e2e

package production

import (
	"context"
	"fmt"
	"testing"
)

// #189: federated queries over parquet files written under different schema
// generations. Every scenario writes the base tier under a v1 schema, evolves
// in place (Env.EvolveSchema = restart with new metadata), then writes delta
// and hot generations under v2 — so the manifest lists physically
// differently-shaped parquet files for one schema and the read must resolve
// the union.

// The core generation carries 3 attributes (name, value, label) and the
// score generations carry 4, matching the issue's stated cardinalities:
// added column = 3→4, removed column = 4→3.

const coreProps = `{
    "name": { "type": "string" },
    "value": { "type": "number" },
    "label": { "type": "string" }
  }`

const coreAttrs = `{
  "name": { "attributeID": 1, "valueType": "text", "column_binding": { "col_name": "text_01" } },
  "value": { "attributeID": 2, "valueType": "numeric" },
  "label": { "attributeID": 5, "valueType": "text" }
}
`

const scoreIntProps = `{
    "name": { "type": "string" },
    "value": { "type": "number" },
    "label": { "type": "string" },
    "score": { "type": "integer" }
  }`

const scoreIntAttrs = `{
  "name": { "attributeID": 1, "valueType": "text", "column_binding": { "col_name": "text_01" } },
  "value": { "attributeID": 2, "valueType": "numeric" },
  "label": { "attributeID": 5, "valueType": "text" },
  "score": { "attributeID": 3, "valueType": "integer" }
}
`

const scoreNumericProps = `{
    "name": { "type": "string" },
    "value": { "type": "number" },
    "label": { "type": "string" },
    "score": { "type": "number" }
  }`

const scoreNumericAttrs = `{
  "name": { "attributeID": 1, "valueType": "text", "column_binding": { "col_name": "text_01" } },
  "value": { "attributeID": 2, "valueType": "numeric" },
  "label": { "attributeID": 5, "valueType": "text" },
  "score": { "attributeID": 3, "valueType": "numeric" }
}
`

const pointsIntProps = `{
    "name": { "type": "string" },
    "value": { "type": "number" },
    "label": { "type": "string" },
    "points": { "type": "integer" }
  }`

const pointsIntAttrs = `{
  "name": { "attributeID": 1, "valueType": "text", "column_binding": { "col_name": "text_01" } },
  "value": { "attributeID": 2, "valueType": "numeric" },
  "label": { "attributeID": 5, "valueType": "text" },
  "points": { "attributeID": 3, "valueType": "integer" }
}
`

const oldColProps = `{
    "name": { "type": "string" },
    "value": { "type": "number" },
    "old_col": { "type": "string" }
  }`

const oldColAttrs = `{
  "name": { "attributeID": 1, "valueType": "text", "column_binding": { "col_name": "text_01" } },
  "value": { "attributeID": 2, "valueType": "numeric" },
  "old_col": { "attributeID": 3, "valueType": "text" }
}
`

const newColProps = `{
    "name": { "type": "string" },
    "value": { "type": "number" },
    "new_col": { "type": "integer" }
  }`

const newColAttrs = `{
  "name": { "attributeID": 1, "valueType": "text", "column_binding": { "col_name": "text_01" } },
  "value": { "attributeID": 2, "valueType": "numeric" },
  "new_col": { "attributeID": 4, "valueType": "integer" }
}
`

const promotionProps = `{
    "name": { "type": "string" },
    "value": { "type": "number" },
    "status": { "type": "string" }
  }`

const promotionV1Attrs = `{
  "name": { "attributeID": 1, "valueType": "text", "column_binding": { "col_name": "text_01" } },
  "value": { "attributeID": 2, "valueType": "numeric" },
  "status": { "attributeID": 3, "valueType": "text" }
}
`

const promotionV2Attrs = `{
  "name": { "attributeID": 1, "valueType": "text", "column_binding": { "col_name": "text_01" } },
  "value": { "attributeID": 2, "valueType": "numeric" },
  "status": { "attributeID": 3, "valueType": "text", "column_binding": { "col_name": "text_02" } }
}
`

// TestSchemaEvolutionEAVToBoundPromotion covers #189 scenario 4: `status` is
// EAV-only in v1 (base parquet carries it as an EAV-pivoted column) and
// main-column-bound in v2 (delta parquet exports it straight from text_02).
// Promotion with an unchanged valueType is read-transparent by construction —
// both storage modes export the same attribute-named, same-typed parquet
// column — and this test pins that contract, including the hot-tier LWW
// interplay: a v1-created row updated under v2 writes the bound column and
// must win over its base (EAV-derived) parquet version.
func TestSchemaEvolutionEAVToBoundPromotion(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	v1 := writeSimpleSchemaDir(t, promotionProps, promotionV1Attrs)
	v2 := writeSimpleSchemaDir(t, promotionProps, promotionV2Attrs)
	env := NewEnv(t, cluster, WithSchemaDir(v1))
	simple := DefaultSchemaFixtures()[0]

	profile := buildEvolutionProfile(func(ordinal int) map[string]any {
		return map[string]any{"status": fmt.Sprintf("st-%04d", ordinal)}
	})

	baseKey, deltaKey, baseEvents := seedEvolutionTiers(ctx, t, env, simple, v2, profile, profile)

	// Shape precondition asserts SAMENESS: transparency is the contract, so
	// both generations must physically carry status as attribute-named VARCHAR.
	wantCols := map[string]string{"name": "VARCHAR", "value": "DOUBLE", "status": "VARCHAR"}
	requireParquetCols(t, "base (v1 EAV pivot)", describeParquetCols(ctx, t, env, baseKey), wantCols)
	requireParquetCols(t, "delta (v2 bound)", describeParquetCols(ctx, t, env, deltaKey), wantCols)

	full := env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 20})
	assertUsesDuckDB(t, full)
	if full != nil && full.Total != 12 {
		t.Fatalf("full scan total = %d, want 12 (5 base + 4 delta + 3 hot)", full.Total)
	}

	// Filter and sort on the promoted attribute across all three generations.
	for _, status := range []string{"st-0002", "st-0007", "st-0010"} { // base / delta / hot rows
		res := env.AssertQueryMatches(ctx, Query{
			Schema:  simple,
			Filters: []Filter{{Attr: "status", Value: status}},
			Limit:   20,
		})
		if res != nil && res.Total != 1 {
			t.Fatalf("filter status=%s total = %d, want 1", status, res.Total)
		}
	}
	env.AssertQueryMatches(ctx, Query{
		Schema: simple,
		Sorts:  []Sort{{Attr: "status", Desc: true}},
		Limit:  20,
	})

	// LWW interplay: update a base-parquet row under v2. The write must land
	// in the bound main column, and the federated read must serve the hot
	// version over the row's base (EAV-derived) parquet version.
	victim := baseEvents[0]
	update := UpdateEvent(simple, victim.RowID, map[string]any{"status": "st-promoted"})
	if err := env.ApplyEvents(ctx, update); err != nil {
		t.Fatalf("apply v2 update to v1-created row: %v", err)
	}

	var text02 *string
	if err := env.Pool.QueryRow(ctx,
		"SELECT text_02 FROM entity_main WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2",
		simple.ID, victim.RowID).Scan(&text02); err != nil {
		t.Fatalf("read text_02 for promoted row: %v", err)
	}
	if text02 == nil || *text02 != "st-promoted" {
		t.Fatalf("v2 update wrote text_02 = %v, want %q (bound-column write path)", text02, "st-promoted")
	}

	promoted := env.AssertQueryMatches(ctx, Query{
		Schema:  simple,
		Filters: []Filter{{Attr: "status", Value: "st-promoted"}},
		Limit:   20,
	})
	assertUsesDuckDB(t, promoted)
	if promoted != nil {
		if promoted.Total != 1 {
			t.Fatalf("promoted-row filter total = %d, want 1", promoted.Total)
		}
		if promoted.Records[0].RowID != victim.RowID {
			t.Fatalf("promoted-row filter returned %s, want the updated base row %s",
				promoted.Records[0].RowID, victim.RowID)
		}
	}
}

// TestSchemaEvolutionAddedColumn covers #189 scenario 1 (v1=3 attributes →
// v2=4): v2 adds `score` (integer, EAV), so the v1 base parquet physically
// lacks a column the current projection names. Contract: the federated read
// resolves the union — all 12 rows return, v1-generation rows carry score
// NULL (excluded by score filters, sorted NULLS LAST).
func TestSchemaEvolutionAddedColumn(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	v1 := writeSimpleSchemaDir(t, coreProps, coreAttrs)
	v2 := writeSimpleSchemaDir(t, scoreIntProps, scoreIntAttrs)
	env := NewEnv(t, cluster, WithSchemaDir(v1))
	simple := DefaultSchemaFixtures()[0]

	baseKey, deltaKey, _ := seedEvolutionTiers(ctx, t, env, simple, v2,
		buildEvolutionProfile(buildLabeledExtras(nil)),
		buildEvolutionProfile(buildLabeledExtras(func(ordinal int) map[string]any {
			return map[string]any{"score": float64(ordinal * 10)}
		})))

	baseCols := describeParquetCols(ctx, t, env, baseKey)
	requireParquetCols(t, "base (v1)", baseCols,
		map[string]string{"name": "VARCHAR", "value": "DOUBLE", "label": "VARCHAR"})
	forbidParquetCols(t, "base (v1)", baseCols, "score")
	requireParquetCols(t, "delta (v2)", describeParquetCols(ctx, t, env, deltaKey),
		map[string]string{"score": "DOUBLE", "label": "VARCHAR"})

	full := env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 20})
	assertUsesDuckDB(t, full)
	if full != nil && full.Total != 12 {
		t.Fatalf("full scan total = %d, want 12 (5 v1 base + 4 delta + 3 hot)", full.Total)
	}

	// SQL NULL semantics: a score filter excludes every v1-generation row.
	// Delta ordinals 5-8 and hot 9-11 all have score >= 50.
	filtered := env.AssertQueryMatches(ctx, Query{
		Schema:  simple,
		Filters: []Filter{{Attr: "score", Op: "gte", Value: "50"}},
		Limit:   20,
	})
	if filtered != nil && filtered.Total != 7 {
		t.Fatalf("score >= 50 total = %d, want 7 (all v2 rows, no v1 row)", filtered.Total)
	}

	// Sorting on the new column: v1 rows (score NULL) land NULLS LAST.
	env.AssertQueryMatches(ctx, Query{
		Schema: simple,
		Sorts:  []Sort{{Attr: "score"}},
		Limit:  20,
	})
}

// TestSchemaEvolutionRemovedColumn covers #189 scenario 2 (v1=4 attributes →
// v2=3): v2 drops `score`, so the v1 base parquet physically carries a
// column the current projection no longer references. Both halves are
// characterization (green on current main): the lone-base read succeeds
// because the explicit projection ignores extra columns, and the mixed
// base+delta read succeeds because DuckDB's projection pushdown never
// requests the dropped column from any file — the generations only diverge
// on a column nobody reads.
func TestSchemaEvolutionRemovedColumn(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	v1 := writeSimpleSchemaDir(t, scoreIntProps, scoreIntAttrs)
	v2 := writeSimpleSchemaDir(t, coreProps, coreAttrs)
	env := NewEnv(t, cluster, WithSchemaDir(v1))
	simple := DefaultSchemaFixtures()[0]

	scoreProfile := buildEvolutionProfile(buildLabeledExtras(func(ordinal int) map[string]any {
		return map[string]any{"score": float64(ordinal * 10)}
	}))
	seedGeneration(ctx, t, env, simple, 5, scoreProfile)
	baseKey := runInitBase(ctx, t, env, simple)
	requireParquetCols(t, "base (v1)", describeParquetCols(ctx, t, env, baseKey),
		map[string]string{"score": "DOUBLE", "label": "VARCHAR"})

	if err := env.EvolveSchema(ctx, v2); err != nil {
		t.Fatalf("evolve schema to v2: %v", err)
	}

	// Green sub-contract on current main: a lone old-generation file under
	// the v2 projection succeeds — score is simply never selected.
	lone := env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 20})
	assertUsesDuckDB(t, lone)
	if lone != nil && lone.Total != 5 {
		t.Fatalf("lone-base scan under v2 projection total = %d, want 5", lone.Total)
	}

	plain := buildEvolutionProfile(buildLabeledExtras(nil))
	seedGeneration(ctx, t, env, simple, 4, plain)
	deltaKey := requireSoleParquet(t, "flush", mustFlush(ctx, t, env).NewObjects)
	seedGeneration(ctx, t, env, simple, 3, plain)
	forbidParquetCols(t, "delta (v2)", describeParquetCols(ctx, t, env, deltaKey), "score")

	full := env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 20})
	assertUsesDuckDB(t, full)
	if full != nil && full.Total != 12 {
		t.Fatalf("full scan total = %d, want 12 (5 v1 base + 4 delta + 3 hot)", full.Total)
	}

	// Filter and sort on a surviving attribute across both generations.
	filtered := env.AssertQueryMatches(ctx, Query{
		Schema:  simple,
		Filters: []Filter{{Attr: "value", Op: "gte", Value: "5"}},
		Limit:   20,
	})
	if filtered != nil && filtered.Total != 7 {
		t.Fatalf("value >= 5 total = %d, want 7 (ordinals 5-11)", filtered.Total)
	}
	env.AssertQueryMatches(ctx, Query{
		Schema: simple,
		Sorts:  []Sort{{Attr: "value", Desc: true}},
		Limit:  20,
	})
}

// TestSchemaEvolutionChangedType covers #189 scenario 3: `score` keeps its
// attributeID but its valueType changes integer→numeric. Since #384 both
// generations export at EAV storage width DOUBLE (pre-#384 the v1 base
// stored the declared integer width), so the union is trivially DOUBLE —
// matching the oracle's numeric-family float64 normalization; filters and
// sorts see one coherent numeric domain across generations.
//
// Red on current main with SILENTLY WRONG DATA, not a loud failure: without
// union_by_name DuckDB coerces every file to the first file's schema, so the
// delta's DOUBLE values are cast to the base's narrower integer width —
// fractional scores
// are corrupted in place and only the oracle catches it (attr mismatches,
// totals identical). That failure mode is the strongest reason the fix must
// widen the union rather than pin first-file-wins.
func TestSchemaEvolutionChangedType(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	v1 := writeSimpleSchemaDir(t, scoreIntProps, scoreIntAttrs)
	v2 := writeSimpleSchemaDir(t, scoreNumericProps, scoreNumericAttrs)
	env := NewEnv(t, cluster, WithSchemaDir(v1))
	simple := DefaultSchemaFixtures()[0]

	baseKey, deltaKey, _ := seedEvolutionTiers(ctx, t, env, simple, v2,
		buildEvolutionProfile(buildLabeledExtras(func(ordinal int) map[string]any {
			return map[string]any{"score": float64(ordinal * 10)}
		})),
		buildEvolutionProfile(buildLabeledExtras(func(ordinal int) map[string]any {
			return map[string]any{"score": float64(ordinal*10) + 0.5}
		})))

	requireParquetCols(t, "base (v1 integer)", describeParquetCols(ctx, t, env, baseKey),
		map[string]string{"score": "DOUBLE"})
	requireParquetCols(t, "delta (v2 numeric)", describeParquetCols(ctx, t, env, deltaKey),
		map[string]string{"score": "DOUBLE"})

	full := env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 20})
	assertUsesDuckDB(t, full)
	if full != nil && full.Total != 12 {
		t.Fatalf("full scan total = %d, want 12 (5 v1 base + 4 delta + 3 hot)", full.Total)
	}

	// One numeric domain across generations: the threshold catches v1
	// integer-generation rows (20,30,40) and every v2 DOUBLE row (50.5..110.5).
	filtered := env.AssertQueryMatches(ctx, Query{
		Schema:  simple,
		Filters: []Filter{{Attr: "score", Op: "gte", Value: "20"}},
		Limit:   20,
	})
	if filtered != nil && filtered.Total != 10 {
		t.Fatalf("score >= 20 total = %d, want 10 (3 v1 + 7 v2)", filtered.Total)
	}
	env.AssertQueryMatches(ctx, Query{
		Schema: simple,
		Sorts:  []Sort{{Attr: "score"}},
		Limit:  20,
	})
}

// TestSchemaEvolutionMixedGenerations is the #189 scenario 5 capstone: one
// evolution step both removes a column (old_col) and adds one (new_col), so
// the base and delta parquet shapes diverge in both directions while all
// three tiers are populated. The read must resolve the schema union: old
// rows return with new_col NULL, old_col never surfaces, and filters/sorts
// on new_col see only the v2 generation.
func TestSchemaEvolutionMixedGenerations(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	v1 := writeSimpleSchemaDir(t, oldColProps, oldColAttrs)
	v2 := writeSimpleSchemaDir(t, newColProps, newColAttrs)
	env := NewEnv(t, cluster, WithSchemaDir(v1))
	simple := DefaultSchemaFixtures()[0]

	baseKey, deltaKey, _ := seedEvolutionTiers(ctx, t, env, simple, v2,
		buildEvolutionProfile(func(ordinal int) map[string]any {
			return map[string]any{"old_col": fmt.Sprintf("old-%04d", ordinal)}
		}),
		buildEvolutionProfile(func(ordinal int) map[string]any {
			return map[string]any{"new_col": float64(ordinal * 10)}
		}))

	baseCols := describeParquetCols(ctx, t, env, baseKey)
	requireParquetCols(t, "base (v1)", baseCols, map[string]string{"old_col": "VARCHAR"})
	forbidParquetCols(t, "base (v1)", baseCols, "new_col")
	deltaCols := describeParquetCols(ctx, t, env, deltaKey)
	requireParquetCols(t, "delta (v2)", deltaCols, map[string]string{"new_col": "DOUBLE"})
	forbidParquetCols(t, "delta (v2)", deltaCols, "old_col")

	full := env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 20})
	assertUsesDuckDB(t, full)
	if full != nil && full.Total != 12 {
		t.Fatalf("full scan total = %d, want 12 (5 v1 base + 4 delta + 3 hot)", full.Total)
	}

	filtered := env.AssertQueryMatches(ctx, Query{
		Schema:  simple,
		Filters: []Filter{{Attr: "new_col", Op: "gte", Value: "50"}},
		Limit:   20,
	})
	if filtered != nil && filtered.Total != 7 {
		t.Fatalf("new_col >= 50 total = %d, want 7 (all v2 rows, no v1 row)", filtered.Total)
	}
	env.AssertQueryMatches(ctx, Query{
		Schema: simple,
		Sorts:  []Sort{{Attr: "new_col"}},
		Limit:  20,
	})
}

// TestSchemaEvolutionRenamedColumn pins the rename contract the #189 task
// line requires ("renamed columns"): a logical rename (same attributeID,
// same valueType, new name `score`→`points`) behaves as remove+add over
// parquet generations. Parquet columns are physical-NAME-keyed and
// union_by_name unifies by name only, so the old generation's values stay
// under the old column — which the v2 projection never selects — and old
// rows read NULL under the new name. Values do NOT migrate.
//
// Tier asymmetry, documented not pinned: hot-tier EAV storage is
// attr_id-keyed, so a v1-written row still HOT at evolution time would
// surface its old value under the new name from pg_source. This recipe keeps
// every v1 row parquet-resident, the generation boundary the issue is about.
func TestSchemaEvolutionRenamedColumn(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	v1 := writeSimpleSchemaDir(t, scoreIntProps, scoreIntAttrs)
	v2 := writeSimpleSchemaDir(t, pointsIntProps, pointsIntAttrs)
	env := NewEnv(t, cluster, WithSchemaDir(v1))
	simple := DefaultSchemaFixtures()[0]

	baseKey, deltaKey, _ := seedEvolutionTiers(ctx, t, env, simple, v2,
		buildEvolutionProfile(buildLabeledExtras(func(ordinal int) map[string]any {
			return map[string]any{"score": float64(ordinal * 10)}
		})),
		buildEvolutionProfile(buildLabeledExtras(func(ordinal int) map[string]any {
			return map[string]any{"points": float64(ordinal * 10)}
		})))

	baseCols := describeParquetCols(ctx, t, env, baseKey)
	requireParquetCols(t, "base (v1)", baseCols, map[string]string{"score": "DOUBLE"})
	forbidParquetCols(t, "base (v1)", baseCols, "points")
	deltaCols := describeParquetCols(ctx, t, env, deltaKey)
	requireParquetCols(t, "delta (v2)", deltaCols, map[string]string{"points": "DOUBLE"})
	forbidParquetCols(t, "delta (v2)", deltaCols, "score")

	full := env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 20})
	assertUsesDuckDB(t, full)
	if full != nil && full.Total != 12 {
		t.Fatalf("full scan total = %d, want 12 (5 v1 base + 4 delta + 3 hot)", full.Total)
	}

	// Remove+add semantics: v1 rows are NULL under the new name even though
	// their attributeID matches — a points filter excludes all of them.
	filtered := env.AssertQueryMatches(ctx, Query{
		Schema:  simple,
		Filters: []Filter{{Attr: "points", Op: "gte", Value: "0"}},
		Limit:   20,
	})
	if filtered != nil && filtered.Total != 7 {
		t.Fatalf("points >= 0 total = %d, want 7 (renamed values must NOT migrate to old rows)", filtered.Total)
	}
	env.AssertQueryMatches(ctx, Query{
		Schema: simple,
		Sorts:  []Sort{{Attr: "points"}},
		Limit:  20,
	})
}
