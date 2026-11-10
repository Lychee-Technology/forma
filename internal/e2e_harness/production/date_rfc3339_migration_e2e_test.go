//go:build e2e

package production

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/compaction"
	"github.com/lychee-technology/forma/internal/rfc3339migrate"
	"github.com/lychee-technology/forma/internal/widthaudit"
)

// This file is the #842 migration acceptance. A deployment upgrading from
// the iso8601 encoding holds whole-second images (YYYY-MM-DDThh:mm:ssZ) in
// PostgreSQL, and parquet copies exported from them on the warm and cold
// tiers. The migration rewrites the PostgreSQL images to the rfc3339 shape
// without stamping change_log, on the argument that a parquet copy holds the
// instant, not the image. After it, rows the new release writes at
// millisecond precision sit beside the migrated ones, and the PostgreSQL
// route (text comparison and sort) and the DuckDB route over hot, delta and
// base must answer every probe and sort alike. Without the rewrite,
// "…05Z" sorts after "…05.250Z" as text, so the PostgreSQL route would
// misorder them.

// migrationGroups are the rows written before the migration, by tier at the
// time of the rewrite (cold: compacted to base, warm: in delta, hot:
// unflushed), and the rows the new release writes after it. Every instant
// is distinct per attribute, so the sorts are total.
var migrationGroups = map[string][]canonicalRow{
	"cold": {
		{name: "m-cold-a", seenAt: "2024-01-02T03:04:05Z", bornOn: "2024-01-10"},
		{name: "m-cold-b", seenAt: "1000-01-01T00:00:00Z", bornOn: "0999-12-31"},
	},
	"warm": {
		{name: "m-warm-a", seenAt: "2024-01-02T03:04:06Z", bornOn: "2024-09-30"},
		{name: "m-warm-b", seenAt: "9999-12-31T23:59:59Z"},
	},
	"hot": {
		{name: "m-hot-a", seenAt: "2024-10-01T00:00:00Z", bornOn: "2024-12-31"},
		{name: "m-hot-b", seenAt: "0000-01-01T00:00:00Z"},
	},
	"new": {
		{name: "m-new-250", seenAt: "2024-01-02T03:04:05.250Z", bornOn: "2024-01-10T00:00:00.001Z"},
		{name: "m-new-001", seenAt: "2024-01-02T03:04:05.001Z"},
		{name: "m-new-999", seenAt: "2024-01-02T03:04:05.999Z"},
	},
}

// seedMigrationGroup writes specs through the API; with legacy it then
// plants the iso8601 image of each instant, as the old release stored it.
func seedMigrationGroup(ctx context.Context, t *testing.T, env *Env, specs []canonicalRow, legacy bool) []*canonicalRow {
	t.Helper()
	simple := DefaultSchemaFixtures()[0]
	rows := make([]*canonicalRow, 0, len(specs))
	events := make([]*Event, 0, len(specs))
	ids := make([]uuid.UUID, 0, len(specs))
	for _, spec := range specs {
		row := spec
		attrs := map[string]any{"name": row.name}
		if row.seenAt != "" {
			attrs["seenAt"] = row.seenAt
			row.seenMs = mustInstantMillis(t, row.seenAt)
		}
		if row.bornOn != "" {
			attrs["bornOn"] = row.bornOn
			row.bornMs = mustInstantMillis(t, row.bornOn)
		}
		row.ev = CreateEvent(simple, attrs)
		rows = append(rows, &row)
		events = append(events, row.ev)
	}
	mustApplyEvents(ctx, t, env, "migration creates", events...)
	// The row ids are assigned when the events are applied.
	for _, ev := range events {
		ids = append(ids, ev.RowID)
	}
	if legacy {
		env.ExecSQL(ctx, `UPDATE `+env.Tables.EntityMain+` SET
			text_02 = CASE WHEN text_02 LIKE '%.000Z' THEN left(text_02, 19) || 'Z' ELSE text_02 END,
			text_03 = CASE WHEN text_03 LIKE '%.000Z' THEN left(text_03, 19) || 'Z' ELSE text_03 END
			WHERE ltbase_schema_id = $1 AND ltbase_row_id = ANY($2::uuid[])`, simple.ID, ids)
	}
	return rows
}

// TestRFC3339MigrationKeepsEveryTierConsistent: legacy rows on every tier,
// the migration, then new millisecond rows; both routes answer by instant,
// and the storage-domain census and the migration's own count find nothing.
func TestRFC3339MigrationKeepsEveryTierConsistent(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t), WithSchemaDir(writeSimpleSchemaDir(t, rfc3339Props, rfc3339Attrs)))
	simple := DefaultSchemaFixtures()[0]

	var rows []*canonicalRow
	rows = append(rows, seedMigrationGroup(ctx, t, env, migrationGroups["cold"], true)...)
	mustFlush(ctx, t, env)
	result, err := env.RunCompactionWith(ctx, simple, CompactionOverrides{TargetBaseSizeBytes: 1})
	if err != nil || result.Outcome != compaction.PromotionApplied {
		t.Fatalf("promote the cold rows: outcome %v, %v", result.Outcome, err)
	}
	rows = append(rows, seedMigrationGroup(ctx, t, env, migrationGroups["warm"], true)...)
	mustFlush(ctx, t, env)
	rows = append(rows, seedMigrationGroup(ctx, t, env, migrationGroups["hot"], true)...)

	targets := []rfc3339migrate.Target{
		{SchemaName: simple.Name, SchemaID: simple.ID, AttrID: 2, AttrName: "seenAt", Column: forma.MainColumnText02},
		{SchemaName: simple.Name, SchemaID: simple.ID, AttrID: 3, AttrName: "bornOn", Column: forma.MainColumnText03},
	}
	var planted int64
	for _, target := range targets {
		n, err := rfc3339migrate.CountLegacy(ctx, env.Pool, env.Tables.EntityMain, target)
		if err != nil {
			t.Fatalf("count %s: %v", target, err)
		}
		planted += n
		if _, err := rfc3339migrate.Rewrite(ctx, env.Pool, env.Tables.EntityMain, target, 2); err != nil {
			t.Fatalf("migrate %s: %v", target, err)
		}
		if n, err := rfc3339migrate.CountLegacy(ctx, env.Pool, env.Tables.EntityMain, target); err != nil || n != 0 {
			t.Fatalf("%s: %d whole-second image(s) left, %v", target, n, err)
		}
	}
	if planted != 10 {
		t.Fatalf("planted %d whole-second images, want 10", planted)
	}
	rows = append(rows, seedMigrationGroup(ctx, t, env, migrationGroups["new"], false)...)

	assertCanonicalImages(ctx, t, env, rows)
	probes := canonicalProbes(t, rows)
	pg := Query{Schema: simple, PreferHot: true, Limit: 100}
	duck := Query{Schema: simple, Limit: 100}
	runWidthProbes(ctx, t, env, "migrated-pg", pg, false, probes)
	assertCanonicalSorts(ctx, t, env, "migrated-pg", pg, false, rows)
	runWidthProbes(ctx, t, env, "migrated-duck", duck, true, probes)
	assertCanonicalSorts(ctx, t, env, "migrated-duck", duck, true, rows)

	var columnTargets []widthaudit.Target
	for _, target := range widthaudit.DomainTargets(env.Metadata) {
		if target.SchemaID == simple.ID && target.Column != "" {
			columnTargets = append(columnTargets, target)
		}
	}
	if len(columnTargets) != 2 {
		t.Fatalf("census targets = %+v, want seenAt and bornOn", columnTargets)
	}
	findings, err := widthaudit.DomainCensus(ctx, env.Pool,
		widthaudit.Tables{EntityMain: env.Tables.EntityMain, ChangeLog: env.Tables.ChangeLog}, columnTargets)
	if err != nil || len(findings) != 0 {
		t.Fatalf("census after the migration = %+v, %v; want nothing", findings, err)
	}
}
