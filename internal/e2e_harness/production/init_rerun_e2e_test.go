//go:build e2e

package production

import (
	"context"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/manifest"
)

// TestInitRerunIdempotency (#176 scenario 4): rerunning cdc-init with no
// data changes rewrites the same deterministic base keys (min/max row-id
// naming -> S3 overwrite, no new objects) and must not duplicate manifest
// entries. The federated result is unchanged.
func TestInitRerunIdempotency(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]

	env.CDC.BatchSize = 7 // 20 rows -> 3 base files, so duplication is x3-visible

	creates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 20})
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply creates: %v", err)
	}

	first, err := env.RunInit(ctx, wide)
	if err != nil {
		t.Fatalf("first init: %v", err)
	}
	firstPaths := buildBasePaths(first.Manifest)
	if len(firstPaths) != 3 {
		t.Fatalf("first init produced %d base entries, want 3", len(firstPaths))
	}

	second, err := env.RunInit(ctx, wide)
	if err != nil {
		t.Fatalf("second init: %v", err)
	}
	// No duplicate objects: identical batching -> identical keys -> overwrite.
	// (_tmp keys are copy-staging garbage, not duplicates, if one survives.)
	for _, k := range second.NewObjects {
		if !strings.Contains(k, "/_tmp/") {
			t.Errorf("rerun created new object %s, want overwrite of existing keys only", k)
		}
	}
	// No duplicate manifest entries: count RAW entries, not unique paths —
	// this is the assertion that goes red on append semantics (6 vs 3).
	rawEntries := manifest.FilterByTier(second.Manifest, "base")
	if len(rawEntries) != 3 {
		t.Fatalf("manifest holds %d base entries after rerun, want 3 (no duplicates)", len(rawEntries))
	}
	// Path identity with the first run: the rerun must reference the same
	// deterministic keys, not introduce new ones.
	for _, f := range rawEntries {
		if !firstPaths[f.Path] {
			t.Errorf("rerun introduced unexpected base path %s", f.Path)
		}
	}
	assertBaseRows(ctx, t, env, second.Manifest, 20)

	// Same federated results after the rerun (base-only source).
	env.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1", wide.ID)
	result := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 100})
	if result != nil && len(result.Records) != 20 {
		t.Fatalf("federated result has %d rows after rerun, want 20", len(result.Records))
	}
}

func buildBasePaths(m *manifest.Manifest) map[string]bool {
	paths := make(map[string]bool)
	for _, f := range manifest.FilterByTier(m, "base") {
		paths[f.Path] = true
	}
	return paths
}
