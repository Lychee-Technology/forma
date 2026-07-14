//go:build e2e

package production

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/model"
)

// Shared fixtures and plan/row assertion helpers for the tier-hint contract
// suites (#184); scenarios live in tier_hints_e2e_test.go.

// seedColdOnly creates n rows and moves them wholesale to base parquet
// (cdc-init + onboarding change_log cleanup), returning their row IDs.
func seedColdOnly(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, n int) []uuid.UUID {
	t.Helper()
	creates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: n})
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply creates: %v", err)
	}
	if _, err := env.RunInit(ctx, wide); err != nil {
		t.Fatalf("run init: %v", err)
	}
	env.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1", wide.ID)
	ids := make([]uuid.UUID, 0, n)
	for _, ev := range creates {
		ids = append(ids, ev.RowID)
	}
	return ids
}

// assertTitleFilterCount runs a title-equals filter under the given tiers and
// requires an exact match count.
func assertTitleFilterCount(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, tiers []model.DataTier, name, title string, want int) {
	t.Helper()
	result, err := env.Query(ctx, Query{
		Schema:         wide,
		PreferredTiers: tiers,
		Filters:        []Filter{{Attr: "title", Value: title}},
		Limit:          10,
	})
	if err != nil {
		t.Fatalf("%s: query: %v", name, err)
	}
	if len(result.Records) != want {
		t.Errorf("%s: title=%q returned %d rows, want %d", name, title, len(result.Records), want)
	}
}

// assertRowIDSet requires the records to be exactly the union of the given
// row-id groups.
func assertRowIDSet(t *testing.T, name string, records []*model.PersistentRecord, groups ...[]uuid.UUID) {
	t.Helper()
	want := make(map[uuid.UUID]bool)
	for _, g := range groups {
		for _, id := range g {
			want[id] = true
		}
	}
	got := make(map[uuid.UUID]bool, len(records))
	for _, rec := range records {
		got[rec.RowID] = true
	}
	for id := range want {
		if !got[id] {
			t.Errorf("%s: missing row %s", name, id)
		}
	}
	for id := range got {
		if !want[id] {
			t.Errorf("%s: unexpected row %s", name, id)
		}
	}
}

func tierComboName(tiers []model.DataTier) string {
	if len(tiers) == 0 {
		return "default-all"
	}
	parts := make([]string, 0, len(tiers))
	for _, tier := range tiers {
		parts = append(parts, string(tier))
	}
	return strings.Join(parts, "+")
}

// duckdbPlanSQL returns the rendered SQL of the last duckdb source in the
// plan, or "" when the plan recorded no duckdb source.
func duckdbPlanSQL(plan *model.ExecutionPlan) string {
	if plan == nil {
		return ""
	}
	sqlText := ""
	for _, src := range plan.Sources {
		if src.Engine == "duckdb" && src.SQL != "" {
			sqlText = src.SQL
		}
	}
	return sqlText
}

// duckdbSourceTier returns the Tier label of the last duckdb source, or ""
// when none is recorded.
func duckdbSourceTier(plan *model.ExecutionPlan) string {
	if plan == nil {
		return ""
	}
	tier := ""
	for _, src := range plan.Sources {
		if src.Engine == "duckdb" {
			tier = string(src.Tier)
		}
	}
	return tier
}

func planHasEngine(plan *model.ExecutionPlan, engine string) bool {
	if plan == nil {
		return false
	}
	for _, src := range plan.Sources {
		if src.Engine == engine {
			return true
		}
	}
	return false
}

func planHasReason(plan *model.ExecutionPlan, reason string) bool {
	if plan == nil {
		return false
	}
	for _, src := range plan.Sources {
		if src.Reason == reason {
			return true
		}
	}
	return false
}

func planNotesContain(plan *model.ExecutionPlan, substr string) bool {
	if plan == nil {
		return false
	}
	for _, note := range plan.Notes {
		if strings.Contains(note, substr) {
			return true
		}
	}
	return false
}
