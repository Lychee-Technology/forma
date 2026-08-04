//go:build e2e

package production

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/manifest"
	"github.com/lychee-technology/forma/internal/reconcile"
)

// initBaseSeedCreates/initBaseSeedBatch shape the seeded cdc-init run into
// several base files, so promotion must prove coverage over a multi-file set
// with disjoint row ranges instead of a single trivial file.
const (
	initBaseSeedCreates = 12
	initBaseSeedBatch   = 5
	initBaseSeedFiles   = 3 // ceil(12/5): 5 + 5 + 2
)

// TestManifestReconcile_PromotesStrippedInitBase reproduces "complete export,
// failed publish" (#292): a real cdc-init run's base entries are stripped
// from the manifest while its objects stay in S3, so the exported base tier
// is invisible to readers and its files look like init-shaped orphans. With
// --repair, reconcile must prove the set complete and splice it back as the
// base tier, reconstructing entries equivalent to init's own — after which a
// second run finds nothing left to do.
func TestManifestReconcile_PromotesStrippedInitBase(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	schema := DefaultSchemaFixtures()[0] // e2e_simple

	original := seedStrippedInitBase(t, ctx, env, schema)

	t.Run("repair_promotes_whole_set", func(t *testing.T) {
		testPromoteRestoresBaseTier(t, ctx, env, schema, original)
	})
	t.Run("second_run_is_clean", func(t *testing.T) {
		testPromoteSecondRunClean(t, ctx, env, schema, len(original))
	})
}

// TestManifestReconcile_RefusesPartialInitPromotion covers the coverage
// identity's refusal side: after the manifest is stripped, one MORE live row
// is created, so the orphan set no longer covers every live entity_main row.
// Promoting a partial export would drop that row from the base tier's
// canonical inventory, so reconcile must refuse, leave the base tier empty,
// touch no object, and report residual discrepancies (the tool's exit 2).
func TestManifestReconcile_RefusesPartialInitPromotion(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	schema := DefaultSchemaFixtures()[0] // e2e_simple

	original := seedStrippedInitBase(t, ctx, env, schema)

	// One live row the init export cannot possibly contain. It is NOT
	// flushed: liveness reads entity_main directly, so a hot-only row already
	// breaks the coverage identity.
	late := env.GenerateScript(ScriptSpec{Schema: schema, Creates: 1})
	if err := env.ApplyEvents(ctx, late...); err != nil {
		t.Fatalf("apply late create: %v", err)
	}

	keysBefore, err := env.listS3Keys(ctx)
	if err != nil {
		t.Fatalf("list s3 keys before refusal run: %v", err)
	}
	mBefore, _ := loadManifestWithETag(t, ctx, env, schema)

	r, cleanup := newReconcileHarness(t, ctx, env, schema,
		reconcile.Options{Repair: true, MaxETagRetries: 3}, true)
	defer cleanup()

	report, err := r.Run(ctx)
	if err != nil {
		t.Fatalf("reconcile repair run: %v", err)
	}
	s := report.Schemas[0]

	wantFragment := fmt.Sprintf("covers %d of %d live rows", initBaseSeedCreates, initBaseSeedCreates+1)
	if !strings.Contains(s.InitPromotionRefusal, "live rows") {
		t.Fatalf("init promotion refusal = %q, want a live-row coverage refusal", s.InitPromotionRefusal)
	}
	if !strings.Contains(s.InitPromotionRefusal, wantFragment) {
		t.Errorf("refusal %q does not name the coverage shortfall %q", s.InitPromotionRefusal, wantFragment)
	}
	if len(s.PromotedBase) != 0 {
		t.Fatalf("refused run promoted %v, want nothing", s.PromotedBase)
	}
	if !report.HasResidualDiscrepancies() {
		t.Fatal("a refused promotion must leave residual discrepancies (tool exit 2)")
	}
	// Identity, not count: the refused set must be exactly the init exports,
	// still reported as orphans and still on the GC path.
	assertSameKeySet(t, "base orphans after refusal", s.BaseOrphans, entryPaths(original))

	mAfter, _ := loadManifestWithETag(t, ctx, env, schema)
	if got := baseTierEntries(mAfter); len(got) != 0 {
		t.Fatalf("refused run wrote %d base entries: %+v", len(got), got)
	}
	if mAfter.Version != mBefore.Version {
		t.Fatalf("refused run bumped manifest version %d -> %d", mBefore.Version, mAfter.Version)
	}
	assertKeysStillPresent(t, ctx, env, keysBefore, entryPaths(original))
}

// seedStrippedInitBase runs a REAL cdc-init and then strips every base entry
// it published from the manifest, leaving the exported objects untouched in
// S3 — the exact post-state of a failed manifest publish. It returns the
// entries init itself wrote, which serve as the per-field oracle for whatever
// promotion later reconstructs.
func seedStrippedInitBase(t *testing.T, ctx context.Context, env *Env, schema SchemaRef) []manifest.FileEntry {
	t.Helper()
	env.CDC.BatchSize = initBaseSeedBatch

	creates := env.GenerateScript(ScriptSpec{Schema: schema, Creates: initBaseSeedCreates})
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply init seed events: %v", err)
	}

	report, err := env.RunInit(ctx, schema)
	if err != nil {
		t.Fatalf("run init: %v", err)
	}
	// Pin the multi-file premise explicitly. It holds because the harness
	// leaves TargetFileSizeMB at 0, so BatchSize alone drives batching
	// (12 rows / batch 5 -> 5+5+2). A sizing change would otherwise collapse
	// this test to a single file and silently stop exercising disjoint-range
	// coverage over a multi-file set.
	if report.FilesCreated != initBaseSeedFiles {
		t.Fatalf("init created %d base files, want %d (batch size %d over %d rows)",
			report.FilesCreated, initBaseSeedFiles, initBaseSeedBatch, initBaseSeedCreates)
	}
	if report.RowsExported != int64(initBaseSeedCreates) {
		t.Fatalf("init exported %d rows, want %d", report.RowsExported, initBaseSeedCreates)
	}

	m, etag := loadManifestWithETag(t, ctx, env, schema)
	original := baseTierEntries(m)
	if len(original) == 0 {
		t.Fatal("init published no base manifest entries to strip")
	}
	if len(original) != report.FilesCreated {
		t.Fatalf("manifest holds %d base entries, init created %d files", len(original), report.FilesCreated)
	}

	var kept []manifest.FileEntry
	for _, f := range m.Files {
		if !strings.EqualFold(f.Tier, "base") {
			kept = append(kept, f)
		}
	}
	m.Files = kept
	saveManifestWithETag(t, ctx, env, schema, m, etag)

	stripped, _ := loadManifestWithETag(t, ctx, env, schema)
	if got := baseTierEntries(stripped); len(got) != 0 {
		t.Fatalf("strip left %d base entries behind: %+v", len(got), got)
	}
	return original
}

func testPromoteRestoresBaseTier(t *testing.T, ctx context.Context, env *Env, schema SchemaRef, original []manifest.FileEntry) {
	r, cleanup := newReconcileHarness(t, ctx, env, schema,
		reconcile.Options{Repair: true, MaxETagRetries: 3}, true)
	defer cleanup()

	report, err := r.Run(ctx)
	if err != nil {
		t.Fatalf("reconcile repair run: %v", err)
	}
	if len(report.Schemas) != 1 {
		t.Fatalf("expected 1 schema report, got %d", len(report.Schemas))
	}
	s := report.Schemas[0]
	if s.InitPromotionRefusal != "" {
		t.Fatalf("promotion refused: %s", s.InitPromotionRefusal)
	}
	assertSameKeySet(t, "promoted base", s.PromotedBase, entryPaths(original))
	if report.HasResidualDiscrepancies() {
		t.Fatalf("a fully promoted set leaves no residual discrepancies, got %+v", s)
	}

	mAfter, _ := loadManifestWithETag(t, ctx, env, schema)
	promoted := baseTierEntries(mAfter)
	if len(promoted) != len(original) {
		t.Fatalf("manifest holds %d base entries after promotion, want %d", len(promoted), len(original))
	}
	byPath := make(map[string]manifest.FileEntry, len(promoted))
	for _, e := range promoted {
		byPath[e.Path] = e
	}
	for i := range original {
		want := original[i]
		got, ok := byPath[want.Path]
		if !ok {
			t.Fatalf("promoted base tier %+v misses init entry %s", promoted, want.Path)
		}
		assertPromotedEntryMatches(t, got, want)
	}
}

// assertPromotedEntryMatches is the per-field oracle against cdc-init's own
// entry. Identity, row range, row count, byte size and the #256 column stamp
// must be reproduced EXACTLY — promotion recomputes them from the same
// parquet init wrote and lists the same object. Created* is the one field
// that legitimately differs: init stamps its wall-clock time.Now() at export,
// while promotion derives the range from the file's changed_at
// (ltbase_updated_at) values, which necessarily precede that stamp. So the
// promoted range is only required to be a real, positive range that does not
// exceed init's stamp.
func assertPromotedEntryMatches(t *testing.T, got, want manifest.FileEntry) {
	t.Helper()
	if !strings.EqualFold(got.Tier, "base") {
		t.Errorf("promoted entry %s has tier %q, want base", got.Path, got.Tier)
	}
	if got.RowIDMin != want.RowIDMin || got.RowIDMax != want.RowIDMax {
		t.Errorf("promoted %s row range [%s, %s], want [%s, %s]",
			got.Path, got.RowIDMin, got.RowIDMax, want.RowIDMin, want.RowIDMax)
	}
	if got.RowCount != want.RowCount {
		t.Errorf("promoted %s row count %d, want %d", got.Path, got.RowCount, want.RowCount)
	}
	if got.SizeBytes != want.SizeBytes {
		t.Errorf("promoted %s size %d bytes, want %d", got.Path, got.SizeBytes, want.SizeBytes)
	}
	// Both sides stamp columns best-effort (init's initStampColumns and
	// promotionColumns both degrade to nil on a failed probe), so a nil ==
	// nil comparison would pass vacuously. Require the pre-strip stamp to
	// exist before treating it as an oracle.
	if len(want.Columns) == 0 {
		t.Fatalf("pre-strip entry %s has no column stamp; the Columns oracle would be vacuous", want.Path)
	}
	if !reflect.DeepEqual(got.Columns, want.Columns) {
		t.Errorf("promoted %s column stamp %v, want %v", got.Path, got.Columns, want.Columns)
	}
	if got.CreatedMin <= 0 || got.CreatedMin > got.CreatedMax {
		t.Errorf("promoted %s created range [%d, %d] is not a positive range",
			got.Path, got.CreatedMin, got.CreatedMax)
	}
	if got.CreatedMax > want.CreatedMax {
		t.Errorf("promoted %s created max %d exceeds init's export stamp %d",
			got.Path, got.CreatedMax, want.CreatedMax)
	}
}

// testPromoteSecondRunClean proves the promoted set is now ordinary listed
// inventory: nothing is orphaned, nothing is promoted again, and the manifest
// is not rewritten.
func testPromoteSecondRunClean(t *testing.T, ctx context.Context, env *Env, schema SchemaRef, wantBase int) {
	mBefore, _ := loadManifestWithETag(t, ctx, env, schema)
	r, cleanup := newReconcileHarness(t, ctx, env, schema,
		reconcile.Options{Repair: true, MaxETagRetries: 3}, true)
	defer cleanup()

	report, err := r.Run(ctx)
	if err != nil {
		t.Fatalf("second reconcile repair run: %v", err)
	}
	s := report.Schemas[0]
	if len(s.PromotedBase) != 0 || s.InitPromotionRefusal != "" {
		t.Fatalf("second run promoted %v / refused %q, want a no-op", s.PromotedBase, s.InitPromotionRefusal)
	}
	if len(s.BaseOrphans) != 0 || len(s.Dangling) != 0 {
		t.Fatalf("second run still sees base orphans %v / dangling %v", s.BaseOrphans, s.Dangling)
	}
	if report.HasResidualDiscrepancies() {
		t.Fatalf("second run reports residual discrepancies: %+v", s)
	}

	mAfter, _ := loadManifestWithETag(t, ctx, env, schema)
	if mAfter.Version != mBefore.Version {
		t.Fatalf("second run bumped manifest version %d -> %d", mBefore.Version, mAfter.Version)
	}
	if got := baseTierEntries(mAfter); len(got) != wantBase {
		t.Fatalf("base tier holds %d entries after the second run, want %d", len(got), wantBase)
	}
	assertNoDuplicateManifestEntries(t, mAfter)
}

func baseTierEntries(m *manifest.Manifest) []manifest.FileEntry {
	var out []manifest.FileEntry
	for _, f := range m.Files {
		if strings.EqualFold(f.Tier, "base") {
			out = append(out, f)
		}
	}
	return out
}

func entryPaths(entries []manifest.FileEntry) []string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Path)
	}
	return out
}

func assertSameKeySet(t *testing.T, label string, got, want []string) {
	t.Helper()
	g := append([]string(nil), got...)
	w := append([]string(nil), want...)
	sort.Strings(g)
	sort.Strings(w)
	if !reflect.DeepEqual(g, w) {
		t.Fatalf("%s = %v, want %v", label, g, w)
	}
}

// assertKeysStillPresent proves a refusal deleted nothing: every key listed
// before the run is still listed, and the named keys are explicitly among
// them.
func assertKeysStillPresent(t *testing.T, ctx context.Context, env *Env, before, must []string) {
	t.Helper()
	after, err := env.listS3Keys(ctx)
	if err != nil {
		t.Fatalf("list s3 keys after refusal run: %v", err)
	}
	remaining := make(map[string]bool, len(after))
	for _, k := range after {
		remaining[k] = true
	}
	for _, k := range before {
		if !remaining[k] {
			t.Errorf("refused run deleted object %s", k)
		}
	}
	for _, k := range must {
		if !remaining[k] {
			t.Errorf("refused run deleted init export %s", k)
		}
	}
}
