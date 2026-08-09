//go:build e2e

package production

import (
	"context"
	"errors"
	"testing"

	forma "github.com/lychee-technology/forma"
	fedengine "github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/model"
)

// TestKeysetUnderProductionRoutingDefaults pins #354 in the configuration a
// real deployment runs. forma defaults Routing.Strategy to hybrid
// (config_duckdb.go), whose "small result set" rule sends Limit < 1000 to the
// Postgres-only path — and keyset pagination is by definition a small-Limit
// shape. Pre-fix the cursor was dropped there and the caller received an
// unfiltered first page, so pagination never advanced.
func TestKeysetUnderProductionRoutingDefaults(t *testing.T) {
	cluster := SharedCluster(t)
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	t.Run("hybrid_small_limit_applies_cursor", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		env := NewEnv(t, cluster, WithRoutingStrategy(forma.RoutingStrategyHybrid))

		a := CreateEvent(wide, map[string]any{"title": "ks-route-a", "count": float64(100)})
		b := CreateEvent(wide, map[string]any{"title": "ks-route-b", "count": float64(400)})
		c := CreateEvent(wide, map[string]any{"title": "ks-route-c", "count": float64(600)})
		if err := env.ApplyEvents(ctx, a, b, c); err != nil {
			t.Fatalf("apply creates: %v", err)
		}
		mustFlush(ctx, t, env)

		page, err := env.Query(ctx, Query{
			Schema: wide,
			Sorts:  []Sort{{Attr: "count"}},
			Keyset: &model.KeysetCursor{
				Columns: []model.KeysetColumn{
					{Attribute: "count", Direction: forma.SortOrderAsc},
					{Attribute: "row_id", Direction: forma.SortOrderAsc}, // required tiebreak (#183)
				},
				Values: []any{float64(500), keysetZeroRowID},
				Mode:   model.KeysetCursorModeAfter,
			},
			Limit: 10, // < 1000: the hybrid rule that routes postgres-only
		})
		if err != nil {
			t.Fatalf("keyset query under hybrid routing: %v", err)
		}
		if len(page.Records) != 1 || page.Records[0].RowID != c.RowID {
			t.Fatalf("keyset page = %v, want exactly [%s]: the cursor was dropped by postgres-only routing (#354)",
				pageRowIDs(page), c.RowID)
		}
		if !page.Plan.Routing.UseDuckDB {
			t.Fatalf("a cursor query must be rerouted onto the federated path; got UseDuckDB=false, reason=%q",
				page.Plan.Routing.Reason)
		}
	})

	t.Run("prefer_hot_fails_closed", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		env := NewEnv(t, cluster, WithRoutingStrategy(forma.RoutingStrategyHybrid))

		a := CreateEvent(wide, map[string]any{"title": "ks-hot-a", "count": float64(100)})
		if err := env.ApplyEvents(ctx, a); err != nil {
			t.Fatalf("apply creates: %v", err)
		}

		_, err := env.Query(ctx, Query{
			Schema:    wide,
			Sorts:     []Sort{{Attr: "count"}},
			PreferHot: true,
			Keyset: &model.KeysetCursor{
				Columns: []model.KeysetColumn{
					{Attribute: "count", Direction: forma.SortOrderAsc},
					{Attribute: "row_id", Direction: forma.SortOrderAsc},
				},
				Values: []any{float64(50), keysetZeroRowID},
				Mode:   model.KeysetCursorModeAfter,
			},
			Limit: 10,
		})
		if err == nil {
			t.Fatal("hot-only + keyset cursor must fail closed: the postgres-only path applies no cursor predicate (#354)")
		}
		if !errors.Is(err, fedengine.ErrKeysetUnsupportedOnPostgres) {
			t.Fatalf("error must classify as ErrKeysetUnsupportedOnPostgres, got: %v", err)
		}
	})
}
