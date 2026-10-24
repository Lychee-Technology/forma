//go:build e2e

package production

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/lychee-technology/forma/internal/model"
)

// TestTextUUIDOperatorsAcrossRoutes pins #676: not_equals and the range
// operators on a text or uuid attribute answer the same rows whether the
// attribute is column-bound (title on text_01, ref on uuid_01) or stored in
// eav_data (note, token), on the Postgres route, the DuckDB route over hot
// rows (where the entity_main pushdown filters pg_source first) and the
// DuckDB route over warm delta parquet. Before #676 the bound forms were
// refused on the DuckDB route with `unsupported operator`. Expected rows come
// from Go's byte order over the seeded values. The text values are lowercase
// ASCII, which PostgreSQL's linguistic collation orders by bytes too, so the
// Postgres route can share the expectation; the uuids span the top bit,
// which a signed comparison of the 128-bit image would misorder.
func TestTextUUIDOperatorsAcrossRoutes(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	// The DuckDB route reads parquet, so a tier must exist before the probe
	// rows are hot. The filler holds only ratio, which no probe names.
	filler := CreateEvent(wide, map[string]any{"ratio": 0.5})
	mustApplyEvents(ctx, t, env, "seed cold filler", filler)
	if _, err := env.RunInit(ctx, wide); err != nil {
		t.Fatalf("run init: %v", err)
	}
	env.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1", wide.ID)

	type seeded struct {
		ev         *Event // its RowID is assigned when the event is applied
		text, uuid string
	}
	var rows []seeded
	var events []*Event
	for _, seed := range []struct{ text, uuid string }{
		{"apple", "00000000-0000-4000-8000-000000000001"},
		{"banana", "7fffffff-ffff-4fff-bfff-ffffffffffff"},
		{"bananas", "80000000-0000-4000-8000-000000000000"},
		{"cherry", "ffffffff-ffff-4fff-bfff-fffffffffffe"},
	} {
		ev := CreateEvent(wide, map[string]any{"title": seed.text, "note": seed.text, "ref": seed.uuid, "token": seed.uuid})
		events = append(events, ev)
		rows = append(rows, seeded{ev: ev, text: seed.text, uuid: seed.uuid})
	}
	mustApplyEvents(ctx, t, env, "seed text/uuid rows", events...)

	probe := func(label string, base Query, wantDuck bool) {
		t.Helper()
		ops := map[string]string{"equals": "=", "not_equals": "!=", "lt": "<", "lte": "<=", "gt": ">", "gte": ">="}
		for _, p := range []struct {
			attrs    []string
			operands []string
			stored   func(seeded) string
		}{
			{[]string{"title", "note"}, []string{"banana", "b", "bananas", "zebra"}, func(r seeded) string { return r.text }},
			{[]string{"ref", "token"}, []string{"7FFFFFFF-FFFF-4FFF-BFFF-FFFFFFFFFFFF", "80000000-0000-4000-8000-000000000000"},
				func(r seeded) string { return r.uuid }},
		} {
			for _, attr := range p.attrs {
				for _, operand := range p.operands {
					for op, sqlOp := range ops {
						var want []uuid.UUID
						for _, r := range rows {
							if operandHolds(sqlOp, strings.Compare(p.stored(r), strings.ToLower(operand))) {
								want = append(want, r.ev.RowID)
							}
						}
						assertOperandProbe(ctx, t, env, label, base, wantDuck, Filter{Attr: attr, Op: op, Value: operand}, want)
					}
				}
			}
		}
	}

	all := []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold}
	probe("hot-pg", Query{Schema: wide, PreferHot: true, PreferredTiers: all, Limit: 100}, false)
	probe("hot-duck", Query{Schema: wide, PreferredTiers: all, Limit: 100}, true)

	// Flush the probe rows to warm delta parquet; they leave the dirty set.
	mustFlush(ctx, t, env)
	probe("warm-duck", Query{Schema: wide, PreferredTiers: all, Limit: 100}, true)
}
