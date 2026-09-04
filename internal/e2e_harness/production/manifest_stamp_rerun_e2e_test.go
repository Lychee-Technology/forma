//go:build e2e

package production

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/manifest"
)

// TestManifestStamp_InPlaceRewriteInvalidatesValidatorCache is the cache-keying
// half of #256.
//
// The pre-read validator caches every path it validates so a warm server pays
// no footer probe. That was justified by "parquet objects are write-once",
// which Forma's writers honour (flush and compaction always minted fresh
// UUIDv7 keys; init does too since #416) but which a rewrite under a listed
// key still violates: a pre-#416 init overwrote its deterministic
// {min}_{max}.parquet key in place, and an operator repair can republish bytes
// under an existing key, each re-stamping the entry under the same path. A
// path-keyed cache then serves the pre-rewrite column set for the rest of the
// process's life, with no eviction and no restart short of bouncing the server.
//
// The scenario keeps ONE engine alive across the rewrites — that is the whole
// point, and it is why no EvolveSchema appears after the warm-up (EvolveSchema
// rebuilds the engine and would discard the very cache under test).
//
// Two rewrites, because they probe opposite halves of the contract:
//
//  1. An init rerun over unchanged rows: a FRESH key (#416) carrying a
//     byte-identical schema and an IDENTICAL stamp. The stamp must not move on
//     an unchanged rerun — re-keying the cache must not cost a probe per
//     rerun beyond the new object's own, which is the cold-start win #256
//     exists for.
//  2. An out-of-band rewrite of the listed key that DROPS a column, with the
//     manifest re-stamped to match — the pre-#416 init shape, still reachable.
//     A stale union still claims the column, so #255 augments nothing and the
//     projection binds a column the object no longer has — a permanent
//     ErrFederatedReadFailed for this schema that no retry clears. Re-validating
//     on the new stamp restores the typed-NULL projection and the read answers.
//
// The last assertion deliberately skips the oracle: the column is physically
// gone from the only object that holds these rows, so NULL scores are the
// correct answer and the event-log oracle would still expect the written
// values. What is under test is that the read succeeds at all.
func TestManifestStamp_InPlaceRewriteInvalidatesValidatorCache(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	withScore := writeSimpleSchemaDir(t, scoreIntProps, scoreIntAttrs)
	env := NewEnv(t, cluster, WithSchemaDir(withScore))
	simple := DefaultSchemaFixtures()[0]

	seedGeneration(ctx, t, env, simple, 5, buildEvolutionProfile(buildLabeledExtras(
		func(ordinal int) map[string]any { return map[string]any{"score": float64(50 + ordinal*10)} })))
	baseKey := runInitBase(ctx, t, env, simple)
	requireParquetCols(t, "base (with score)", describeParquetCols(ctx, t, env, baseKey),
		map[string]string{"score": "DOUBLE"})

	// Warm-up: builds the engine that must survive both rewrites and leaves
	// the validator holding a column set that CLAIMS score.
	warm := env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 20})
	assertUsesDuckDB(t, warm)
	stampA := stampedEntryFor(ctx, t, env, simple, baseKey)
	if _, ok := stampA["score"]; !ok {
		t.Fatalf("precondition: the stamp does not claim score: %#v", stampA)
	}

	// Rewrite 1 — init rerun, nothing changed. Fresh write-once key (#416),
	// identical stamp.
	rerun, err := env.RunInit(ctx, simple)
	if err != nil {
		t.Fatalf("init rerun: %v", err)
	}
	env.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1", simple.ID)
	baseEntries := manifest.FilterByTier(rerun.Manifest, "base")
	if len(baseEntries) != 1 || baseEntries[0].Path == baseKey {
		t.Fatalf("init rerun did not replace %s with a fresh write-once key; base entries: %+v", baseKey, baseEntries)
	}
	baseKey = baseEntries[0].Path
	stampB := stampedEntryFor(ctx, t, env, simple, baseKey)
	if !maps.Equal(stampA, stampB) {
		t.Fatalf("an unchanged rerun moved the stamp; the cache would re-validate needlessly:\n a: %#v\n b: %#v", stampA, stampB)
	}
	env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 20})

	// Rewrite 2 — the column disappears and the manifest says so.
	rewriteBaseWithoutScore(ctx, t, env, simple, baseKey)
	forbidParquetCols(t, "base (score dropped)", describeParquetCols(ctx, t, env, baseKey), "score")
	stampC := stampedEntryFor(ctx, t, env, simple, baseKey)
	if maps.Equal(stampB, stampC) {
		t.Fatalf("the rewrite left the stamp unchanged, so there is no invalidation signal to test: %#v", stampC)
	}

	res, err := env.Query(ctx, Query{Schema: simple, Limit: 20})
	if err != nil {
		t.Fatalf("read after the in-place rewrite failed; the validator served the pre-rewrite column set "+
			"and the projection bound a column the object no longer has: %v", err)
	}
	if !res.Plan.Routing.UseDuckDB {
		t.Fatalf("read after the rewrite did not route to duckdb: %+v", res.Plan.Routing)
	}
	if len(res.Records) != 5 {
		t.Fatalf("read after the rewrite returned %d records, want 5", len(res.Records))
	}
}

// rewriteBaseWithoutScore republishes the schema's single base object without
// its score column and re-stamps the manifest entry to match, leaving the key
// and every other entry field alone — the physical state an init rerun
// produces when an attribute leaves the schema.
func rewriteBaseWithoutScore(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, baseKey string) {
	t.Helper()
	const staging = "base_without_score"
	stage := fmt.Sprintf("CREATE OR REPLACE TABLE %s AS SELECT * EXCLUDE (score) FROM read_parquet('s3://%s/%s')",
		staging, env.Cluster.Bucket, strings.TrimPrefix(baseKey, "/"))
	if _, err := env.Duck.DB.ExecContext(ctx, stage); err != nil {
		t.Fatalf("stage score-less rewrite of %s: %v", baseKey, err)
	}
	writeParquetViaDuck(ctx, t, env, "SELECT * FROM "+staging, baseKey)

	entries := manifest.FilterByTier(mustLoadManifest(ctx, t, env, schema), "base")
	for i := range entries {
		if strings.TrimPrefix(entries[i].Path, "/") == strings.TrimPrefix(baseKey, "/") {
			entries[i].Columns = describeParquetCols(ctx, t, env, baseKey)
		}
	}
	store := &manifest.S3Store{Client: env.Cluster.S3, Bucket: env.Cluster.Bucket}
	resolver := manifest.PathResolver{Prefix: env.CDC.ManifestPrefix, PathTemplate: env.CDC.ManifestTemplate}
	path, err := resolver.Resolve(schema.ID)
	if err != nil {
		t.Fatalf("resolve manifest path: %v", err)
	}
	if err := manifest.ReplaceTierFiles(ctx, store, path, schema.ID, "base", entries); err != nil {
		t.Fatalf("re-stamp base entries: %v", err)
	}
}

// mustLoadManifest returns the schema's current manifest.
func mustLoadManifest(ctx context.Context, t *testing.T, env *Env, schema SchemaRef) *manifest.Manifest {
	t.Helper()
	manifests, err := env.loadManifests(ctx)
	if err != nil {
		t.Fatalf("load manifests: %v", err)
	}
	m := manifests[schema.ID]
	if m == nil {
		t.Fatalf("schema %s has no manifest", schema.Name)
	}
	return m
}
