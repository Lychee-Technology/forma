//go:build e2e

package production

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/manifest"
)

// TestCDCFlushAndInit verifies the real CDC runner and the extracted
// cdc.RunInit against the per-test database and S3 prefix: dry-run leaves
// flushed_at and the manifest untouched, a real flush writes delta parquet
// under <prefix>/<schemaID>/ and marks rows flushed, and init writes base
// files plus manifest base entries.
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
	assertEntriesStampedToByteTruth(ctx, t, env, "cdc flush (delta)", manifest.FilterByTier(m, "delta"))

	initReport, err := env.RunInit(ctx, simple)
	if err != nil {
		t.Fatalf("init: %v", err)
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
		t.Fatal("init did not update the manifest")
	}
	if n := countTier(initReport.Manifest, "base"); n == 0 {
		t.Errorf("manifest has no base entries after init: %+v", initReport.Manifest.Files)
	}
	// #256: the init writer stamps each base entry the same way, and the
	// delta stamps written above survive the manifest round-trip.
	assertEntriesStampedToByteTruth(ctx, t, env, "cdc init (base)",
		manifest.FilterByTier(initReport.Manifest, "base"))
	assertEntriesStampedToByteTruth(ctx, t, env, "cdc init (deltas preserved)",
		manifest.FilterByTier(initReport.Manifest, "delta"))
}

func anyKeyMatches(keys []string, prefix, suffix string) bool {
	for _, k := range keys {
		if strings.HasPrefix(k, prefix) && strings.HasSuffix(k, suffix) && !strings.Contains(k, "/_tmp/") {
			return true
		}
	}
	return false
}

func countTier(m *manifest.Manifest, tier string) int {
	n := 0
	for _, f := range m.Files {
		if f.Tier == tier {
			n++
		}
	}
	return n
}
