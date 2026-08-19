//go:build e2e

package production

import (
	"context"
	"testing"
)

// TestConcurrentFlushDifferentSchemas pins issue #186 scenario 3: the flush
// advisory lock is scoped per schema — pg_try_advisory_lock(schemaID,
// schemaID), internal/cdc/helpers.go — so a flush pass holding one schema's
// lock neither blocks nor is blocked by a concurrent pass flushing a
// different schema, and a locked schema is skipped (nil), not failed.
//
// Deterministic orchestration instead of a rerun-until-it-overlaps race:
//
//  1. Seed only schema A and start runner1, paused at its first CopyObject
//     (PausingS3). At the pause runner1 has enumerated its dirty schemas —
//     provably [A] alone, because B is not seeded yet — and sits inside A's
//     processSchema holding A's advisory lock.
//  2. Seed schema B and run runner2 to completion while runner1 is frozen.
//     runner2 enumerates both schemas: A's lock is held, so A is skipped
//     with no error and no side effects (same-schema exclusion on the real
//     production path); B flushes fully (per-schema scope: A's in-flight
//     flush is no contention for B).
//  3. Resume runner1; A completes. Both schemas converge to exactly one
//     final delta each, disjoint per-schema manifests, and oracle parity.
//
// The skip-while-locked proof depends on the runners using separate Postgres
// sessions: pg_try_advisory_lock is reentrant within a session, and each
// RunFlushWith builds its own Runner whose RunOnce opens its own sql.DB
// (internal/cdc/flusher.go setupPostgresConnection). If the harness ever
// shared one pool across runners, runner2 could re-acquire A's lock on the
// holding connection and this test would stop proving exclusion.
func TestConcurrentFlushDifferentSchemas(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	simple := DefaultSchemaFixtures()[0] // e2e_simple (20)
	second := DefaultSchemaFixtures()[2] // e2e_second (22)

	// Seed ONLY schema A before runner1 starts; B is seeded strictly after
	// the pause is reached, so runner1's enumeration cannot include it and
	// the single CopyObject the pause intercepts is A's.
	seedRows(ctx, t, env, simple, 3)

	pauser := NewPausingS3(env.Cluster.S3, S3OpCopy)
	runnerCtx, cancel := context.WithTimeout(ctx, pausedFlushTimeout)
	// The flush goroutine must not touch t; it reports through done (buffered
	// so it never leaks even when the test dies before reading it).
	done := make(chan flushOutcome, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		report, err := env.RunFlushWith(runnerCtx, FlushOverrides{S3: pauser})
		done <- flushOutcome{report: report, err: err}
	}()
	// Registered after NewEnv's cleanups, so this runs first: any early
	// t.Fatal releases the paused flush and waits for it to exit while the
	// Env's resources are still alive.
	t.Cleanup(func() {
		pauser.Resume()
		cancel()
		<-finished
	})

	select {
	case <-pauser.Reached():
	case out := <-done:
		t.Fatalf("runner1 finished before reaching the CopyObject pause: err=%v", out.err)
	}

	seedRows(ctx, t, env, second, 3)
	r2, err := env.RunFlushWith(ctx, FlushOverrides{})
	if err != nil {
		t.Fatalf("runner2 must succeed while runner1 holds schema %d's lock: %v", simple.ID, err)
	}
	// runner2 committed B completely while A's flush was mid-flight...
	assertSchemaFullyFlushed(ctx, t, env, r2.NewObjects, r2.Manifests, second, 3)
	// ...and left the locked schema A entirely alone: every A row still
	// dirty, no A final promoted.
	flushedA, dirtyA := fetchChangeLogRowIDs(ctx, t, env, simple)
	if len(flushedA) != 0 || len(dirtyA) != 3 {
		t.Fatalf("runner2 must not touch the locked schema %d: flushed=%v dirty=%v",
			simple.ID, flushedA, dirtyA)
	}
	if finals := filterFinalsForSchema(env, r2.NewObjects, simple); len(finals) != 0 {
		t.Errorf("runner2 must not promote finals for the locked schema %d, got %v", simple.ID, finals)
	}

	pauser.Resume()
	out := <-done
	if out.err != nil {
		t.Fatalf("runner1 flush after resume: %v", out.err)
	}

	// Joint end state from a fresh listing and manifest load — runner1's
	// report diff spans runner2's writes and cannot attribute objects.
	keys, err := env.listS3Keys(ctx)
	if err != nil {
		t.Fatalf("list final s3 keys: %v", err)
	}
	manifests, err := env.loadManifests(ctx)
	if err != nil {
		t.Fatalf("load final manifests: %v", err)
	}
	assertSchemaFullyFlushed(ctx, t, env, keys, manifests, simple, 3)
	assertSchemaFullyFlushed(ctx, t, env, keys, manifests, second, 3)
	env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 100})
	env.AssertQueryMatches(ctx, Query{Schema: second, Limit: 100})
}
