//go:build e2e

package production

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/manifest"
)

// TestCDCFlushAndInit verifies the real CDC runner and the extracted
// cdc.RunInit against the per-test database and S3 prefix: dry-run leaves
// flushed_at and the manifest untouched, a real flush writes delta parquet
// under <prefix>/<schemaID>/ and marks rows flushed, an init over that delta
// tier refuses (#371), and init with ReplaceDelta writes base files plus
// manifest base entries, empties the delta tier, and deletes its objects.
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

	// #371: a re-init over a populated delta tier refuses unless the run may
	// replace it — a base-only swap would leave the stale generations listed
	// under the new base. The refusal leaves the manifest and objects alone.
	if _, err := env.RunInit(ctx, simple); !errors.Is(err, cdc.ErrDeltaTierPresent) {
		t.Fatalf("init over delta tier: err = %v, want cdc.ErrDeltaTierPresent", err)
	}
	manifestsAfterRefusal, err := env.loadManifests(ctx)
	if err != nil {
		t.Fatalf("load manifests after refusal: %v", err)
	}
	if got := countTier(manifestsAfterRefusal[simple.ID], "delta"); got != countTier(m, "delta") {
		t.Fatalf("refused init changed the delta tier: %d entries, want %d", got, countTier(m, "delta"))
	}
	deltaKeys := make([]string, 0, countTier(m, "delta"))
	for _, entry := range manifest.FilterByTier(m, "delta") {
		key, ok := cdc.NormalizeObjectKey(env.Cluster.Bucket, entry.Path)
		if !ok {
			t.Fatalf("delta entry %q is not addressable in the test bucket", entry.Path)
		}
		deltaKeys = append(deltaKeys, key)
	}
	assertS3KeysPresence(ctx, t, env, "after refused init", deltaKeys, true)

	initReport, err := env.RunInitWith(ctx, simple, InitOverrides{ReplaceDelta: true})
	if err != nil {
		t.Fatalf("init with replace-delta: %v", err)
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
	// #256: the init writer stamps each base entry the same way.
	assertEntriesStampedToByteTruth(ctx, t, env, "cdc init (base)",
		manifest.FilterByTier(initReport.Manifest, "base"))
	// #371: the swap emptied the delta tier in the same manifest write, and
	// the purge that followed removed the objects themselves.
	if got := countTier(initReport.Manifest, "delta"); got != 0 {
		t.Errorf("manifest still lists %d delta entries after --replace-delta: %+v", got, initReport.Manifest.Files)
	}
	assertS3KeysPresence(ctx, t, env, "after replace-delta init", deltaKeys, false)
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
