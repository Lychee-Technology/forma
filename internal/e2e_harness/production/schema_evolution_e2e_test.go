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

	profile := evolutionProfile(func(ordinal int) map[string]any {
		return map[string]any{"status": fmt.Sprintf("st-%04d", ordinal)}
	})

	// Base generation under v1 (ordinals 0-4), then evolve, then delta
	// (5-8) and hot (9-11) generations under v2.
	baseEvents := seedGeneration(ctx, t, env, simple, 5, profile)
	baseKey := runInitBase(ctx, t, env, simple)

	if err := env.EvolveSchema(ctx, v2); err != nil {
		t.Fatalf("evolve schema to v2: %v", err)
	}

	seedGeneration(ctx, t, env, simple, 4, profile)
	deltaKey := soleParquetOf(t, "flush", mustFlush(ctx, t, env).NewObjects)
	seedGeneration(ctx, t, env, simple, 3, profile)

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
