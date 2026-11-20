//go:build e2e

package production

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// TestCDCFlushAndInit verifies the real CDC runner and the extracted
// cdc.RunInit against the per-test database and S3 prefix: dry-run leaves
// flushed_at and the publication untouched, a real flush writes delta parquet
// under <prefix>/<schemaID>/ and publishes it with the marks, and a re-init
// of the published schema publishes an init generation under a new lineage
// that lists its own base files only. The earlier delta objects stay in S3
// for garbage collection (#891): no V2 writer deletes an object.
func TestCDCFlushAndInit(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	simple := DefaultSchemaFixtures()[0] // e2e_simple

	var events []*Event
	for i := 0; i < 3; i++ {
		events = append(events, CreateEvent(simple, map[string]any{
			"name":  fmt.Sprintf("row-%d", i),
			"value": float64(i) + 0.5,
		}))
	}
	if err := env.ApplyEvents(ctx, events...); err != nil {
		t.Fatalf("apply events: %v", err)
	}

	dry, err := env.RunFlushDry(ctx)
	if err != nil {
		t.Fatalf("dry-run flush: %v", err)
	}
	if dry.UnflushedBefore != 3 || dry.UnflushedAfter != 3 {
		t.Errorf("dry-run unflushed before/after = %d/%d, want 3/3", dry.UnflushedBefore, dry.UnflushedAfter)
	}
	if len(dry.Manifests) != 0 {
		t.Errorf("dry-run produced manifests: %v", dry.Manifests)
	}

	flush, err := env.RunFlush(ctx)
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if flush.UnflushedBefore != 3 || flush.UnflushedAfter != 0 {
		t.Errorf("flush unflushed before/after = %d/%d, want 3/0", flush.UnflushedBefore, flush.UnflushedAfter)
	}
	deltaPrefix := fmt.Sprintf("%s/%d/", env.S3Prefix, simple.ID)
	if !anyKeyMatches(flush.NewObjects, deltaPrefix, ".parquet") {
		t.Errorf("flush produced no delta parquet under %s: %v", deltaPrefix, flush.NewObjects)
	}
	m := flush.Manifests[simple.ID]
	if m == nil {
		t.Fatal("flush did not create a manifest for e2e_simple")
	}
	if n := countTier(m, "delta"); n == 0 {
		t.Errorf("manifest has no delta entries: %+v", m.Files)
	}
	// #256: the flush writer stamps each delta entry from a DESCRIBE of the
	// object it just published.
	assertEntriesStampedToByteTruth(ctx, t, env, "cdc flush (delta)", TierEntries(m, "delta"))

	deltaKeys := make([]string, 0, countTier(m, "delta"))
	for _, entry := range TierEntries(m, "delta") {
		deltaKeys = append(deltaKeys, entry.Path)
	}

	initReport, err := env.RunInit(ctx, simple)
	if err != nil {
		t.Fatalf("re-init of the published schema: %v", err)
	}
	if initReport.RowsExported != 3 {
		t.Errorf("init rows exported = %d, want 3", initReport.RowsExported)
	}
	if initReport.FilesCreated == 0 {
		t.Error("init created no files")
	}
	if !anyKeyMatches(initReport.NewObjects, deltaPrefix, ".parquet") {
		t.Errorf("init produced no base parquet under %s: %v", deltaPrefix, initReport.NewObjects)
	}
	if initReport.Manifest == nil {
		t.Fatal("init did not publish a generation")
	}
	if initReport.Manifest.Kind != "init" || initReport.Manifest.Lineage == m.Lineage {
		t.Errorf("re-init published a %s generation in lineage %s, want an init generation in a new lineage (flush's was %s)",
			initReport.Manifest.Kind, initReport.Manifest.Lineage, m.Lineage)
	}
	if n := publishesBetween(m, initReport.Manifest); n != 1 {
		t.Errorf("re-init published %d generations after the flush's, want 1", n)
	}
	if n := countTier(initReport.Manifest, "base"); n == 0 {
		t.Errorf("manifest has no base entries after init: %+v", initReport.Manifest.Files)
	}
	// #256: the init writer stamps each base entry the same way.
	assertEntriesStampedToByteTruth(ctx, t, env, "cdc init (base)",
		TierEntries(initReport.Manifest, "base"))
	// The init generation lists its export alone; the flush's delta objects
	// are unlisted but stay until garbage collection.
	if got := countTier(initReport.Manifest, "delta"); got != 0 {
		t.Errorf("the init generation lists %d delta entries: %+v", got, initReport.Manifest.Files)
	}
	assertS3KeysPresence(ctx, t, env, "after re-init", deltaKeys, true)
}

// assertS3KeysPresence checks that every key exists (present=true) or is
// gone (present=false) in the Env's bucket.
func assertS3KeysPresence(ctx context.Context, t *testing.T, env *Env, when string, keys []string, present bool) {
	t.Helper()
	all, err := env.listS3Keys(ctx)
	if err != nil {
		t.Fatalf("list s3 keys %s: %v", when, err)
	}
	have := make(map[string]struct{}, len(all))
	for _, k := range all {
		have[k] = struct{}{}
	}
	for _, key := range keys {
		if _, ok := have[key]; ok != present {
			t.Errorf("%s: delta object %s present=%t, want %t", when, key, ok, present)
		}
	}
}

func anyKeyMatches(keys []string, prefix, suffix string) bool {
	for _, k := range keys {
		if strings.HasPrefix(k, prefix) && strings.HasSuffix(k, suffix) {
			return true
		}
	}
	return false
}

func countTier(m *Manifest, tier string) int {
	n := 0
	for _, f := range m.Files {
		if f.Tier == tier {
			n++
		}
	}
	return n
}
