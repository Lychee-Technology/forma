//go:build e2e

package production

import (
	"context"
	"testing"
)

// TEMPORARY (#718 verification, reverted before review): fails on purpose so
// the E2E Smoke job's failure-gated upload of .artifacts/e2e/ can be observed
// on a real runner.
func TestTempForcedFailureForArtifactUpload(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	simple := DefaultSchemaFixtures()[0]

	script := env.GenerateScript(ScriptSpec{Schema: simple, Creates: 4, Updates: 2, Deletes: 1})
	if err := env.ApplyEvents(ctx, script...); err != nil {
		t.Fatalf("apply events: %v", err)
	}
	if _, err := env.RunFlush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if _, err := env.Query(ctx, Query{Schema: simple, Limit: 10}); err != nil {
		t.Fatalf("query: %v", err)
	}
	t.Fatal("forced failure: verifying the #718 artifact upload")
}
