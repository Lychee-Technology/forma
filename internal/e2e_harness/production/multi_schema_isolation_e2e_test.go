//go:build e2e

package production

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// TestMultiSchemaFlushIsolation pins issue #186: a failure isolated to one
// schema must not corrupt (or block) a sibling schema in the same CDC flush
// pass. The production loop processes schemas sequentially and aggregates
// per-schema errors via errors.Join (internal/cdc/runner.go), so the healthy
// schema commits fully — rows marked, delta promoted, manifest updated —
// while the faulted schema's rows stay dirty and the aggregate error names
// only the faulted schema.
//
// Two failure vectors, one per pipeline side of mark-flushed:
//
//   - CopyFault (step 3, pre-mark): the faulted schema has zero side effects
//     (its tmp is self-healed in-band, #226) — all rows stay dirty for retry.
//   - ManifestSaveFault (step 7, post-mark): the faulted schema's rows are
//     already flushed and its final exists, but its manifest tracks nothing;
//     the #197 contract (error names the orphaned key, retry is a no-op, no
//     self-repair) must hold per-schema without disturbing the sibling.
//
// Missing schema metadata is not exercised here: as of #193 the flusher and
// init both pre-flight every schema's attribute cache and abort the whole run
// (ErrSchemaAttrCacheUnavailable) before any side effect, rather than falling
// back to a generic projection. That contract is pinned by unit tests in
// internal/cdc (TestProcessSchemas_AbortsWhenSchemaCacheUnavailable,
// TestProcessInitSchemas_AbortsWhenSchemaCacheUnavailable).
func TestMultiSchemaFlushIsolation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	t.Run("CopyFault", func(t *testing.T) {
		t.Parallel()
		testMultiSchemaCopyFault(ctx, t)
	})
	t.Run("ManifestSaveFault", func(t *testing.T) {
		t.Parallel()
		testMultiSchemaManifestSaveFault(ctx, t)
	})
}

// seedTwoSchemas seeds three creates in each isolation-pair schema and
// baselines both on the hot tier (PreferHot: nothing has flushed yet, so a
// federated read would hit an empty parquet glob).
func seedTwoSchemas(ctx context.Context, t *testing.T, env *Env) (simple, second SchemaRef) {
	t.Helper()
	simple = DefaultSchemaFixtures()[0] // e2e_simple (20)
	second = DefaultSchemaFixtures()[2] // e2e_second (22)
	seedRows(ctx, t, env, simple, 3)
	seedRows(ctx, t, env, second, 3)
	env.AssertQueryMatches(ctx, Query{Schema: simple, PreferHot: true, Limit: 100})
	env.AssertQueryMatches(ctx, Query{Schema: second, PreferHot: true, Limit: 100})
	return simple, second
}

// assertErrorNamesOnlySchema asserts the aggregate flush error attributes the
// failure to exactly the faulted schema (the errors.Join contract: per-schema
// wrapping is "schema <id>: ...").
func assertErrorNamesOnlySchema(t *testing.T, err error, faulted, healthy SchemaRef) {
	t.Helper()
	msg := err.Error()
	if !strings.Contains(msg, fmt.Sprintf("schema %d:", faulted.ID)) {
		t.Errorf("aggregate error must name schema %d, got: %v", faulted.ID, err)
	}
	if strings.Contains(msg, fmt.Sprintf("schema %d:", healthy.ID)) {
		t.Errorf("aggregate error must not implicate healthy schema %d, got: %v", healthy.ID, err)
	}
}

// testMultiSchemaCopyFault breaks the tmp->final promotion for one schema
// only (the fault matches the schema's S3 partition) and asserts the sibling
// schema's commit is complete and the failure is attributed correctly.
func testMultiSchemaCopyFault(ctx context.Context, t *testing.T) {
	env := NewEnv(t, SharedCluster(t))
	simple, second := seedTwoSchemas(ctx, t, env)

	faulty := &FaultInjectingS3{Inner: env.Cluster.S3,
		Fault: S3Fault{Op: S3OpCopy, KeyContains: buildSchemaKeyPrefix(env, second)}}
	report, err := env.RunFlushWith(ctx, FlushOverrides{S3: faulty})
	if err == nil {
		t.Fatal("flush with one schema's CopyObject failing must fail")
	}
	if faulty.Injected() == 0 {
		t.Fatal("fault never fired")
	}
	assertErrorNamesOnlySchema(t, err, second, simple)
	if !strings.Contains(err.Error(), "copy tmp to final") {
		t.Errorf("error must surface at the copy step, got: %v", err)
	}

	// The healthy schema commits fully; the faulted schema keeps every row
	// dirty with no final and no manifest entry.
	assertSchemaFullyFlushed(ctx, t, env, report.NewObjects, report.Manifests, simple, 3)
	assertSchemaUntouchedByFault(ctx, t, env, report, second, 3)
	if report.UnflushedAfter != 3 {
		t.Errorf("only the faulted schema's rows may stay dirty, unflushed = %d, want 3", report.UnflushedAfter)
	}

	// Oracle parity on both sides: the flushed schema through the federated
	// path, the faulted schema still hot-only (its glob has no finals).
	env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 100})
	env.AssertQueryMatches(ctx, Query{Schema: second, PreferHot: true, Limit: 100})
}

// testMultiSchemaManifestSaveFault breaks the manifest PutObject for one
// schema only. Under the #252 ordering the faulted schema's append fails
// BEFORE mark-flushed, so its rows stay dirty and the healed retry
// self-heals them into a fresh listed delta (the first copied final stays an
// unlisted orphan for reconcile --gc) — while the sibling schema's manifest
// and data are untouched by the failure.
func testMultiSchemaManifestSaveFault(ctx context.Context, t *testing.T) {
	env := NewEnv(t, SharedCluster(t))
	simple, second := seedTwoSchemas(ctx, t, env)

	manifestKey := fmt.Sprintf("%s/manifest/%d.json", env.S3Prefix, second.ID)
	faulty := &FaultInjectingS3{Inner: env.Cluster.S3,
		Fault: S3Fault{Op: S3OpPut, KeyContains: manifestKey}}
	report, err := env.RunFlushWith(ctx, FlushOverrides{S3: faulty})
	if err == nil {
		t.Fatal("flush with one schema's manifest save failing must fail")
	}
	if faulty.Injected() == 0 {
		t.Fatal("fault never fired")
	}
	assertErrorNamesOnlySchema(t, err, second, simple)

	// Healthy schema: complete commit including its manifest entry.
	assertSchemaFullyFlushed(ctx, t, env, report.NewObjects, report.Manifests, simple, 3)

	// Faulted schema: the failed append precedes mark-flushed (#252), so its
	// rows stay dirty; the copied final exists and the error names it for
	// observability, but the manifest tracks nothing.
	flushed, dirty := fetchChangeLogRowIDs(ctx, t, env, second)
	if len(flushed) != 0 || len(dirty) != 3 {
		t.Fatalf("schema %d rows must stay dirty when its manifest append fails: flushed=%v dirty=%v",
			second.ID, flushed, dirty)
	}
	finals := filterFinalsForSchema(env, report.NewObjects, second)
	if len(finals) != 1 {
		t.Fatalf("schema %d final must exist when its manifest save fails, got %v", second.ID, finals)
	}
	if !strings.Contains(err.Error(), "manifest update") || !strings.Contains(err.Error(), finals[0]) {
		t.Errorf("error must point at the copied final key %q, got: %v", finals[0], err)
	}
	assertManifestDeltaPaths(t, report.Manifests, second, nil)

	// Retry self-heals ONLY the faulted schema: the healthy schema has
	// nothing dirty and gains no objects; the faulted schema re-exports to a
	// fresh listed delta (#252 supersedes the #197 no-op-retry contract).
	retry, err := env.RunFlushWith(ctx, FlushOverrides{})
	if err != nil {
		t.Fatalf("clean retry flush: %v", err)
	}
	if retry.UnflushedBefore != 3 || retry.UnflushedAfter != 0 {
		t.Errorf("retry must drain only the faulted schema: unflushed before/after = %d/%d, want 3/0",
			retry.UnflushedBefore, retry.UnflushedAfter)
	}
	retryFinals := filterFinalsForSchema(env, retry.NewObjects, second)
	if len(retryFinals) != 1 {
		t.Fatalf("retry must promote exactly one new final for the faulted schema, got %v", retry.NewObjects)
	}
	if healthy := filterFinalsForSchema(env, retry.NewObjects, simple); len(healthy) != 0 {
		t.Errorf("healthy schema must gain no new objects on retry, got %v", healthy)
	}
	assertManifestDeltaPaths(t, retry.Manifests, second, retryFinals)

	// Both schemas fully visible after convergence.
	env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 100})
	env.AssertQueryMatches(ctx, Query{Schema: second, Limit: 100})
}

// TestMultiSchemaRetryRepairsOnlyFailed pins issue #186 scenario 2: after a
// pass where one schema failed at the copy step, a healed retry re-exports
// only the failed schema. The healthy schema has nothing dirty, so it is not
// re-exported, gains no new objects, and its manifest keeps exactly the
// first pass's entry.
func TestMultiSchemaRetryRepairsOnlyFailed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	simple, second := seedTwoSchemas(ctx, t, env)

	faulty := &FaultInjectingS3{Inner: env.Cluster.S3,
		Fault: S3Fault{Op: S3OpCopy, KeyContains: buildSchemaKeyPrefix(env, second)}}
	report, err := env.RunFlushWith(ctx, FlushOverrides{S3: faulty})
	if err == nil {
		t.Fatal("flush with one schema's CopyObject failing must fail")
	}
	if faulty.Injected() == 0 {
		t.Fatal("fault never fired")
	}
	// First pass: the healthy schema's single final (isolation details are
	// pinned by TestMultiSchemaFlushIsolation; here it anchors the retry).
	firstFinals := assertSchemaFullyFlushed(ctx, t, env, report.NewObjects, report.Manifests, simple, 3)

	// Healed retry: only the failed schema flushes.
	retry, err := env.RunFlushWith(ctx, FlushOverrides{})
	if err != nil {
		t.Fatalf("healed retry flush: %v", err)
	}
	if retry.UnflushedBefore != 3 || retry.UnflushedAfter != 0 {
		t.Errorf("retry unflushed %d -> %d, want 3 -> 0", retry.UnflushedBefore, retry.UnflushedAfter)
	}
	if healthyNew := filterFinalsForSchema(env, retry.NewObjects, simple); len(healthyNew) != 0 {
		t.Errorf("retry must not re-export the healthy schema, got new finals %v", healthyNew)
	}
	assertSchemaFullyFlushed(ctx, t, env, retry.NewObjects, retry.Manifests, second, 3)

	// The healthy schema's manifest still tracks exactly the first pass's
	// final — no duplicate entries, no re-export (its parquet inventory is
	// re-checked for duplicates too).
	assertManifestDeltaPaths(t, retry.Manifests, simple, firstFinals)
	assertNoRowExportedTwice(ctx, t, env, simple)

	env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 100})
	env.AssertQueryMatches(ctx, Query{Schema: second, Limit: 100})
}

// TestInitSchemaScopedIsolation pins the backfill side of issue #186's
// multi-schema story: cdc-init runs (and reruns) scoped to one schema via
// SchemaIDFilter touch nothing of a sibling schema — no objects under its
// partition, no manifest writes. Isolation is structural (per-schema key
// partitions and per-schema manifest files); this test documents the
// contract symmetrically with the flush-side isolation above. The rerun
// half of the backfill story is owned by #176 (TestInitRerunIdempotency,
// TestInitRerunAfterChangesReconcilesManifest, TestInitUnderConcurrentMutation).
func TestInitSchemaScopedIsolation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	simple := DefaultSchemaFixtures()[0]
	second := DefaultSchemaFixtures()[2]
	seedRows(ctx, t, env, simple, 5)
	seedRows(ctx, t, env, second, 5)

	assertInitTouchesOnly := func(report *InitReport, target, sibling SchemaRef) {
		t.Helper()
		siblingManifestKey := fmt.Sprintf("%s/manifest/%d.json", env.S3Prefix, sibling.ID)
		for _, k := range report.NewObjects {
			if strings.HasPrefix(k, buildSchemaKeyPrefix(env, sibling)) || k == siblingManifestKey {
				t.Errorf("init of schema %d touched sibling schema %d: %s", target.ID, sibling.ID, k)
			}
		}
	}

	first, err := env.RunInit(ctx, simple)
	if err != nil {
		t.Fatalf("init schema %d: %v", simple.ID, err)
	}
	assertInitTouchesOnly(first, simple, second)
	manifests, err := env.loadManifests(ctx)
	if err != nil {
		t.Fatalf("load manifests: %v", err)
	}
	if manifests[second.ID] != nil {
		t.Fatalf("schema %d must have no manifest before its own init, got %+v", second.ID, manifests[second.ID])
	}

	// #248: schema 20's surface already exists here, so schema 22's init is
	// held to the same overwrite-proof contract as the rerun below — the key
	// diff alone would miss an in-place re-export of the sibling.
	simpleBefore := captureSchemaS3State(t, ctx, env, simple)

	secondInit, err := env.RunInit(ctx, second)
	if err != nil {
		t.Fatalf("init schema %d: %v", second.ID, err)
	}
	assertInitTouchesOnly(secondInit, second, simple)
	assertSchemaS3StateUnchanged(t, "schema 22 init vs sibling 20",
		simpleBefore, captureSchemaS3State(t, ctx, env, simple))
	siblingPaths := buildBasePaths(secondInit.Manifest)
	if len(siblingPaths) == 0 {
		t.Fatal("schema 22 init must produce base entries (positive control)")
	}

	// #248: the NewObjects key diff is blind to overwrites that reuse the
	// deterministic min_max keys, and the base-path comparison below is blind
	// to a manifest rewritten with the same paths. Require exact stat/byte
	// identity of the sibling's whole S3 surface across the rerun.
	siblingBefore := captureSchemaS3State(t, ctx, env, second)

	// Rerun schema 20's init: the sibling's manifest and objects must be
	// byte-for-byte uninvolved (deterministic keys make the rerun itself a
	// pure overwrite, so any sibling-partition key here is a leak).
	rerun, err := env.RunInit(ctx, simple)
	if err != nil {
		t.Fatalf("rerun init schema %d: %v", simple.ID, err)
	}
	assertInitTouchesOnly(rerun, simple, second)
	assertSchemaS3StateUnchanged(t, "schema 20 rerun vs sibling 22",
		siblingBefore, captureSchemaS3State(t, ctx, env, second))
	manifests, err = env.loadManifests(ctx)
	if err != nil {
		t.Fatalf("reload manifests: %v", err)
	}
	rerunSibling := buildBasePaths(manifests[second.ID])
	if len(rerunSibling) != len(siblingPaths) {
		t.Fatalf("sibling base entries changed across the rerun: %v -> %v", siblingPaths, rerunSibling)
	}
	for p := range siblingPaths {
		if !rerunSibling[p] {
			t.Errorf("sibling base path %s lost across the rerun", p)
		}
	}

	// Onboarding contract: init does not clear change_log; clear it so the
	// base tier alone must answer, then oracle-check both schemas. The count
	// is only cold-tier evidence if the query actually routed to DuckDB —
	// a PG-only routing regression would still count 5 entity_main rows (#248).
	for _, ref := range []SchemaRef{simple, second} {
		env.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1", ref.ID)
	}
	for _, ref := range []SchemaRef{simple, second} {
		assertFederatedRowCount(ctx, t, env,
			fmt.Sprintf("schema %d base tier", ref.ID), Query{Schema: ref, Limit: 100}, 5)
	}
}
