//go:build e2e

package production

import (
	"context"
	"errors"
	"testing"
	"time"

	fedengine "github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/model"
)

var (
	allTiers     = []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold}
	parquetTiers = []model.DataTier{model.DataTierWarm, model.DataTierCold}
)

// TestNeverFlushedSchemaAnswersOnDuckDB pins #720: a schema with live rows
// and no parquet object yet resolves the fallback glob, which matches
// nothing, and a query that reaches DuckDB must still answer from the hot
// leg. The breaker opens on the first failure, so a read failure on any query
// shows up as a transition, and the next query is rejected.
func TestNeverFlushedSchemaAnswersOnDuckDB(t *testing.T) {
	ctx := context.Background()
	transitions := &breakerTransitions{}
	env := NewEnv(t, SharedCluster(t), WithBreaker(1, time.Minute), WithMetricEmitter(transitions))
	wide := DefaultSchemaFixtures()[1]

	if err := env.ApplyEvents(ctx, env.GenerateScript(ScriptSpec{Schema: wide, Creates: 4})...); err != nil {
		t.Fatalf("apply events: %v", err)
	}

	for i := 0; i < 3; i++ {
		res := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20, PreferredTiers: allTiers})
		if res != nil && !res.Plan.Routing.UseDuckDB {
			t.Fatalf("query %d did not route through DuckDB: %+v", i, res.Plan.Routing)
		}
	}

	// A hot-excluded query asks for flushed rows only, and there are none.
	res, err := env.Query(ctx, Query{Schema: wide, Limit: 20, PreferredTiers: parquetTiers})
	if err != nil {
		t.Fatalf("hot-excluded query on a never-flushed schema: %v", err)
	}
	if len(res.Records) != 0 || res.Total != 0 {
		t.Fatalf("hot-excluded query on a never-flushed schema returned %d rows (total %d), want none",
			len(res.Records), res.Total)
	}
	transitions.expect(t, "never-flushed queries")

	// After the first flush the same query reads the parquet tiers again
	// rather than a plan compiled for the empty scan set.
	mustFlush(ctx, t, env)
	if err := env.ApplyEvents(ctx, env.GenerateScript(ScriptSpec{Schema: wide, Creates: 2})...); err != nil {
		t.Fatalf("apply hot events: %v", err)
	}
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20, PreferredTiers: allTiers})
	transitions.expect(t, "first flush")
}

// TestLostManifestOverFlushedRowsStillFails pins the other side of #720: a
// confirmed-absent manifest and an empty fallback glob are also what a lost
// manifest looks like. When the schema has flushed rows, which the hot leg
// does not serve, the read must keep failing instead of answering from the
// unflushed rows alone.
func TestLostManifestOverFlushedRowsStillFails(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	wide := DefaultSchemaFixtures()[1]

	if err := env.ApplyEvents(ctx, env.GenerateScript(ScriptSpec{Schema: wide, Creates: 3})...); err != nil {
		t.Fatalf("apply events: %v", err)
	}
	flush := mustFlush(ctx, t, env)
	if len(flush.NewObjects) < 2 {
		t.Fatalf("precondition: first flush wrote %v, want a parquet object and the manifest", flush.NewObjects)
	}
	for _, key := range flush.NewObjects {
		deleteObject(ctx, t, env, key)
	}
	if err := env.ApplyEvents(ctx, env.GenerateScript(ScriptSpec{Schema: wide, Creates: 2})...); err != nil {
		t.Fatalf("apply hot events: %v", err)
	}

	res, err := env.Query(ctx, Query{Schema: wide, Limit: 20, PreferredTiers: allTiers})
	if err == nil {
		t.Fatalf("query answered %d rows with the flushed rows' manifest and objects gone; want a read failure",
			len(res.Records))
	}
	if !errors.Is(err, fedengine.ErrFederatedReadFailed) {
		t.Fatalf("lost manifest over flushed rows: got %v, want ErrFederatedReadFailed", err)
	}
}
