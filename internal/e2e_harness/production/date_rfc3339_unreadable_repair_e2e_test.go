//go:build e2e

package production

import (
	"context"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/compaction"
	"github.com/lychee-technology/forma/internal/rfc3339migrate"
	"github.com/lychee-technology/forma/internal/transform"
)

// This file pins the offline repair docs/rfc3339-storage-contract.md gives
// for an image the read refuses (#842). The whole-second-shaped
// 2023-02-29T00:00:00Z names no day: migrate-rfc3339 leaves it, it still
// blocks the cutover, and no release can rewrite it through the API in that
// window. Its parquet copy reads as an absent attribute (TRY_CAST), so the
// repair must re-export the row, and the documented SQL does it by advancing
// the row's version and stamping change_log slot 0. The SQL is read from the
// doc itself, so the two cannot drift.

const (
	unreadableRepairDoc     = "../../../docs/rfc3339-storage-contract.md"
	unreadableRepairImage   = "2023-02-29T00:00:00Z"
	unreadableRepairChoice  = "2023-03-01T00:00:00.000Z"
	unreadableRepairHeading = "### Repairing an image the read refuses"
)

// unreadableRepairStatements answers the doc's repair statements for one
// row of env, without BEGIN/COMMIT, so the test runs them in its own
// transaction and checks the UPDATE's row count as the doc tells operators.
func unreadableRepairStatements(t *testing.T, env *Env, schemaID int16, rowID uuid.UUID) []string {
	t.Helper()
	raw, err := os.ReadFile(unreadableRepairDoc)
	if err != nil {
		t.Fatalf("read the contract doc: %v", err)
	}
	doc := string(raw)
	start := strings.Index(doc, unreadableRepairHeading)
	if start < 0 {
		t.Fatalf("the contract doc must keep %q", unreadableRepairHeading)
	}
	m := regexp.MustCompile("(?s)```sql\\s*\n(.*?)```").FindStringSubmatch(doc[start:])
	if m == nil {
		t.Fatalf("%q must hold a fenced SQL block", unreadableRepairHeading)
	}
	body := strings.NewReplacer(
		"<schema id>", strconv.Itoa(int(schemaID)),
		"<row id>", rowID.String(),
		"entity_main", env.Tables.EntityMain,
		"change_log", env.Tables.ChangeLog,
	).Replace(m[1])
	var lines []string
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "--") {
			lines = append(lines, line)
		}
	}
	var statements []string
	for _, stmt := range strings.Split(strings.Join(lines, "\n"), ";") {
		stmt = strings.TrimSpace(stmt)
		switch strings.ToUpper(stmt) {
		case "", "BEGIN", "COMMIT":
			continue
		}
		statements = append(statements, stmt)
	}
	if len(statements) != 2 || !strings.HasPrefix(statements[0], "UPDATE") || !strings.HasPrefix(statements[1], "INSERT") {
		t.Fatalf("the documented repair = %q, want an UPDATE and an INSERT", statements)
	}
	for _, image := range []string{unreadableRepairImage, unreadableRepairChoice} {
		if !strings.Contains(statements[0], "'"+image+"'") {
			t.Fatalf("the documented UPDATE does not name %s: %s", image, statements[0])
		}
	}
	return statements
}

// unreadableSeenAt reads seenAt of row on the DuckDB route (preferHot false)
// or the PostgreSQL route; ok is false when the attribute is absent.
func unreadableSeenAt(ctx context.Context, t *testing.T, env *Env, label string, preferHot bool, row uuid.UUID) (ms int64, ok bool) {
	t.Helper()
	simple := DefaultSchemaFixtures()[0]
	res := mustQuery(ctx, t, env, Query{Schema: simple, PreferHot: preferHot, Limit: 100})
	if res.Plan.Routing.UseDuckDB == preferHot {
		t.Fatalf("%s: routing %+v, want DuckDB %t", label, res.Plan.Routing, !preferHot)
	}
	transformer := transform.NewPersistentRecordTransformer(env.Registry)
	for _, rec := range res.Records {
		if rec.RowID != row {
			continue
		}
		attrs, err := transformer.FromPersistentRecord(ctx, rec)
		if err != nil {
			t.Fatalf("%s: decode row %s: %v", label, rec.RowID, err)
		}
		v, present := attrs["seenAt"].(time.Time)
		return v.UnixMilli(), present
	}
	t.Fatalf("%s: row %s missing", label, row)
	return 0, false
}

// TestRFC3339UnreadableImageOfflineRepair: a row on the cold tier holding
// the whole-second image of no day, the migration, then the documented
// offline repair; both routes then read the chosen instant on every tier,
// and nothing blocks the cutover.
func TestRFC3339UnreadableImageOfflineRepair(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t), WithSchemaDir(writeSimpleSchemaDir(t, rfc3339Props, rfc3339Attrs)))
	simple := DefaultSchemaFixtures()[0]
	target := rfc3339migrate.Target{SchemaName: simple.Name, SchemaID: simple.ID, AttrID: 2, AttrName: "seenAt", Column: forma.MainColumnText02}

	rows := seedMigrationGroup(ctx, t, env, []canonicalRow{
		{name: "u-bad", seenAt: "2023-03-01T00:00:00Z"},
		{name: "u-good", seenAt: "2024-01-02T03:04:05Z"},
	}, true)
	bad := rows[0].ev.RowID
	env.ExecSQL(ctx, "UPDATE "+env.Tables.EntityMain+" SET text_02 = $3 WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2",
		simple.ID, bad, unreadableRepairImage)
	mustFlush(ctx, t, env)
	result, err := env.RunCompactionWith(ctx, simple, CompactionOverrides{TargetBaseSizeBytes: 1})
	if err != nil || result.Outcome != compaction.PromotionApplied {
		t.Fatalf("promote to base: outcome %v, %v", result.Outcome, err)
	}

	// Before the repair the OLTP read refuses the row and its parquet copy
	// holds no value; the migration leaves the image and still counts it.
	if _, err := env.EntityManager().Get(ctx, &forma.QueryRequest{SchemaName: simple.Name, RowID: &bad}); err == nil {
		t.Fatalf("get of %s succeeded, want the read to refuse it", unreadableRepairImage)
	}
	if _, ok := unreadableSeenAt(ctx, t, env, "cold-duck before", false, bad); ok {
		t.Fatalf("the parquet copy of %s reads a value, want absent", unreadableRepairImage)
	}
	if _, err := rfc3339migrate.Rewrite(ctx, env.Pool, env.Tables.EntityMain, target, rfc3339migrate.BatchSize); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if n, err := rfc3339migrate.CountLegacy(ctx, env.Pool, env.Tables.EntityMain, target); err != nil || n != 1 {
		t.Fatalf("after the rewrite %d whole-second image(s) remain, %v; want the one naming no day", n, err)
	}
	census := rfc3339Census(ctx, t, env, simple.ID)
	if len(census) != 1 || census[0].RowID != bad || census[0].StoredValue != strconv.Quote(unreadableRepairImage) || census[0].NonCanonical != "" {
		t.Fatalf("census before the repair = %+v, want only %s on row %s as an image the read refuses", census, unreadableRepairImage, bad)
	}

	tx, err := env.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin the repair: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for i, stmt := range unreadableRepairStatements(t, env, simple.ID, bad) {
		tag, err := tx.Exec(ctx, stmt)
		if err != nil {
			t.Fatalf("repair statement %d: %v\n%s", i, err, stmt)
		}
		if tag.RowsAffected() != 1 {
			t.Fatalf("repair statement %d changed %d row(s), want 1\n%s", i, tag.RowsAffected(), stmt)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit the repair: %v", err)
	}

	if n, err := rfc3339migrate.CountLegacy(ctx, env.Pool, env.Tables.EntityMain, target); err != nil || n != 0 {
		t.Fatalf("after the repair %d whole-second image(s) remain, %v", n, err)
	}
	if census := rfc3339Census(ctx, t, env, simple.ID); len(census) != 0 {
		t.Fatalf("census after the repair = %+v, want nothing", census)
	}
	want := mustInstantMillis(t, unreadableRepairChoice)
	check := func(label string) {
		t.Helper()
		got, err := env.EntityManager().Get(ctx, &forma.QueryRequest{SchemaName: simple.Name, RowID: &bad})
		if err != nil {
			t.Fatalf("%s: get: %v", label, err)
		}
		if v, ok := got.Attributes["seenAt"].(time.Time); !ok || v.UnixMilli() != want {
			t.Fatalf("%s: get reads seenAt %v, want %s", label, got.Attributes["seenAt"], unreadableRepairChoice)
		}
		for _, preferHot := range []bool{true, false} {
			if ms, ok := unreadableSeenAt(ctx, t, env, label, preferHot, bad); !ok || ms != want {
				t.Fatalf("%s (preferHot %t): seenAt %d (present %t), want %d", label, preferHot, ms, ok, want)
			}
		}
	}
	// The slot-0 entry routes the row to PostgreSQL at once; the flush then
	// exports a copy whose ver_ts beats the stale one, and compaction keeps it.
	check("hot after the repair")
	mustFlush(ctx, t, env)
	check("warm after the repair")
	result, err = env.RunCompactionWith(ctx, simple, CompactionOverrides{TargetBaseSizeBytes: 1})
	if err != nil || result.Outcome != compaction.PromotionApplied {
		t.Fatalf("promote the repaired row: outcome %v, %v", result.Outcome, err)
	}
	check("cold after the repair")
}
