//go:build e2e

package production

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	fedengine "github.com/lychee-technology/forma/internal/federated"
)

// TestParquetCorruption_CorruptBytes covers #187 scenario 1 with real bad
// bytes: a mid-file 64-byte flip leaves footer and magic intact, so DuckDB
// accepts the file and fails on the mangled page data.
func TestParquetCorruption_CorruptBytes(t *testing.T) {
	runParquetFaultScenario(t, corruptMidFile)
}

// TestParquetCorruption_Truncated covers #187 scenario 4: dropping the
// trailing half takes the parquet footer and magic with it, so the reader
// fails while planning the scan.
func TestParquetCorruption_Truncated(t *testing.T) {
	runParquetFaultScenario(t, truncateHalf)
}

// runParquetFaultScenario is the shared #187 corruption shape: seed two
// tiers, mutate the only parquet's bytes in place, then assert the classified
// bounded failure (degraded off) and the oracle-complete Postgres-only
// fallback (degraded on). The object still exists, so the failure must stay
// ErrFederatedReadFailed — never manifest inconsistency.
func runParquetFaultScenario(t *testing.T, mutate func([]byte) []byte) {
	t.Helper()
	ctx := context.Background()
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	wide := DefaultSchemaFixtures()[1]

	seedTwoTiers(ctx, t, env, wide)
	keys := schemaParquetKeys(ctx, t, env, wide)
	if len(keys) != 1 {
		t.Fatalf("expected exactly one parquet after seedTwoTiers, got %v", keys)
	}

	// Precondition: with the object intact the query routes through DuckDB
	// and matches the oracle, so the mutation below is what breaks it.
	healthy := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20})
	if healthy != nil && !healthy.Plan.Routing.UseDuckDB {
		t.Fatalf("precondition: healthy query did not route to duckdb: %+v", healthy.Plan.Routing)
	}

	overwriteObjectBytes(ctx, t, env, keys[0], mutate)

	// Degraded OFF: a bounded, classified error — the corrupt parquet must
	// not crash the process, hang, or silently succeed.
	failCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, err := env.Query(failCtx, Query{Schema: wide, Limit: 20})
	if err == nil {
		t.Fatal("degraded mode off: expected error reading mutated parquet, got success")
	}
	if !errors.Is(err, fedengine.ErrFederatedReadFailed) {
		t.Fatalf("mutated parquet must classify as ErrFederatedReadFailed, got: %v", err)
	}
	if errors.Is(err, fedengine.ErrParquetSetInconsistent) {
		t.Errorf("object exists in storage; failure must not classify as manifest inconsistency: %v", err)
	}
	if !strings.Contains(err.Error(), "duckdb federated query") {
		t.Errorf("error must carry the federated wrap chain, got: %v", err)
	}

	// Degraded ON: oracle-complete Postgres-only fallback (flush never
	// deletes Postgres rows) with the fallback recorded on the plan.
	degraded := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20, AllowPartialDegradedMode: true})
	assertDegradedFallbackPlan(t, degraded)
}

// TestParquetCorruption_WrongSchemaFile covers #187 scenario 5: the manifest
// lists a parquet whose columns match nothing the schema projection selects.
// Since #189 the scan runs union_by_name (schema evolution tolerance), which
// would NULL-fill the missing system columns and silently drop the file's
// rows via the dirty anti-join — so the loud failure now comes from the
// pre-read system-column invariant validator
// (internal/federated/parquet_schema_validation.go): missing row_id/
// changed_at/deleted_at fails before the scan — classified as a read failure
// (the object exists), degradable as usual, same contract as before.
// Fabricating and manifest-registering the file is deliberate: the
// production exporter cannot produce one, and unlisted rogue objects are
// invisible to manifest-driven reads (#203's reconciliation scope).
func TestParquetCorruption_WrongSchemaFile(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	wide := DefaultSchemaFixtures()[1]

	seedTwoTiers(ctx, t, env, wide)
	healthy := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20})
	if healthy != nil && !healthy.Plan.Routing.UseDuckDB {
		t.Fatalf("precondition: healthy query did not route to duckdb: %+v", healthy.Plan.Routing)
	}

	wrongKey := schemaKeyPrefix(env, wide) + "wrong_schema_zzz.parquet"
	writeParquetViaDuck(ctx, t, env, "SELECT 1 AS wrong_col, 'x' AS other_col", wrongKey)
	if err := env.RegisterParquetInManifest(ctx, wide, wrongKey, "delta"); err != nil {
		t.Fatalf("register wrong-schema parquet: %v", err)
	}

	failCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, err := env.Query(failCtx, Query{Schema: wide, Limit: 20})
	if err == nil {
		t.Fatal("degraded mode off: wrong-schema parquet in the scan set silently succeeded (#187 scenario 5)")
	}
	if !errors.Is(err, fedengine.ErrFederatedReadFailed) {
		t.Fatalf("wrong-schema parquet must classify as ErrFederatedReadFailed, got: %v", err)
	}
	if errors.Is(err, fedengine.ErrParquetSetInconsistent) {
		t.Errorf("object exists in storage; must not classify as manifest inconsistency: %v", err)
	}

	degraded := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20, AllowPartialDegradedMode: true})
	assertDegradedFallbackPlan(t, degraded)
}

// TestParquetCorruption_WrongTypeFile covers the type half of #187 scenario
// 5 ("different column names/types"): a manifest-listed parquet whose column
// NAMES all match the real export but whose row_id/changed_at carry
// incompatible types (VARCHAR non-UUID / VARCHAR non-epoch). Since #189 the
// pre-read invariant validator rejects the mistyped system columns before
// the union_by_name scan could widen them (row_id UUID∪VARCHAR would unify
// to VARCHAR and only fail later at the UUID cast) — still a classified read
// failure, not a silent success.
func TestParquetCorruption_WrongTypeFile(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	wide := DefaultSchemaFixtures()[1]

	seedTwoTiers(ctx, t, env, wide)
	keys := schemaParquetKeys(ctx, t, env, wide)
	if len(keys) != 1 {
		t.Fatalf("expected exactly one parquet after seedTwoTiers, got %v", keys)
	}
	healthy := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20})
	if healthy != nil && !healthy.Plan.Routing.UseDuckDB {
		t.Fatalf("precondition: healthy query did not route to duckdb: %+v", healthy.Plan.Routing)
	}

	// Same column names as the real export, poisoned types for the two
	// columns every projection touches.
	wrongTypeKey := schemaKeyPrefix(env, wide) + "wrong_type_zzz.parquet"
	writeParquetViaDuck(ctx, t, env, fmt.Sprintf(
		"SELECT 'not-a-uuid' AS row_id, 'not-an-epoch' AS changed_at, * EXCLUDE (row_id, changed_at) FROM read_parquet('s3://%s/%s') LIMIT 1",
		env.Cluster.Bucket, keys[0]), wrongTypeKey)
	if err := env.RegisterParquetInManifest(ctx, wide, wrongTypeKey, "delta"); err != nil {
		t.Fatalf("register wrong-type parquet: %v", err)
	}

	failCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, err := env.Query(failCtx, Query{Schema: wide, Limit: 20})
	if err == nil {
		t.Fatal("degraded mode off: wrong-type parquet in the scan set silently succeeded (#187 scenario 5)")
	}
	if !errors.Is(err, fedengine.ErrFederatedReadFailed) {
		t.Fatalf("wrong-type parquet must classify as ErrFederatedReadFailed, got: %v", err)
	}
	if errors.Is(err, fedengine.ErrParquetSetInconsistent) {
		t.Errorf("object exists in storage; must not classify as manifest inconsistency: %v", err)
	}

	degraded := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20, AllowPartialDegradedMode: true})
	assertDegradedFallbackPlan(t, degraded)
}

// TestParquetCorruption_EmptyParquetFile covers #187 scenario 6: a valid
// 0-row parquet with the correct schema. The production exporter refuses
// 0-row exports (duckdb_exporter.go fails fast on empty batches), so the
// file is derived from a real export — WHERE 1=0 keeps the exact column
// shape. Contract: the empty file contributes nothing, the DuckDB path
// stays intact, and results from the other tiers are unaffected.
func TestParquetCorruption_EmptyParquetFile(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	wide := DefaultSchemaFixtures()[1]

	seedTwoTiers(ctx, t, env, wide)
	keys := schemaParquetKeys(ctx, t, env, wide)
	if len(keys) != 1 {
		t.Fatalf("expected exactly one parquet after seedTwoTiers, got %v", keys)
	}

	emptyKey := schemaKeyPrefix(env, wide) + "empty_zzz.parquet"
	writeParquetViaDuck(ctx, t, env,
		fmt.Sprintf("SELECT * FROM read_parquet('s3://%s/%s') WHERE 1=0", env.Cluster.Bucket, keys[0]),
		emptyKey)
	if err := env.RegisterParquetInManifest(ctx, wide, emptyKey, "delta"); err != nil {
		t.Fatalf("register empty parquet: %v", err)
	}

	result := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20})
	if result != nil && !result.Plan.Routing.UseDuckDB {
		t.Errorf("query with an empty parquet in the scan set must keep the DuckDB route: %+v", result.Plan.Routing)
	}
}

// TestParquetCorruption_WrongSchemaFile_GlobHint pins the #189-review P1
// bypass: an explicit S3ParquetPathTemplate hint wins over the manifest
// source (#184) and renders as a GLOB, so the pre-read validator must
// enumerate the glob's matches and validate each — an unexpanded glob would
// read the rogue file with union_by_name and its rows would vanish silently
// (NULL row_id drops out of the dirty anti-join), never surfacing an error.
func TestParquetCorruption_WrongSchemaFile_GlobHint(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	wide := DefaultSchemaFixtures()[1]

	seedTwoTiers(ctx, t, env, wide)

	// Positive control: the glob-hinted read is healthy and routes to DuckDB.
	healthy := env.AssertQueryMatches(ctx, Query{
		Schema: wide, Limit: 20, S3ParquetPathTemplate: env.ParquetGlob(),
	})
	if healthy != nil && !healthy.Plan.Routing.UseDuckDB {
		t.Fatalf("precondition: glob-hinted query did not route to duckdb: %+v", healthy.Plan.Routing)
	}

	// The rogue file is NOT manifest-registered: only the hinted glob can
	// reach it, which is exactly the bypass under test.
	wrongKey := schemaKeyPrefix(env, wide) + "wrong_schema_glob_zzz.parquet"
	writeParquetViaDuck(ctx, t, env, "SELECT 1 AS wrong_col, 'x' AS other_col", wrongKey)

	failCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, err := env.Query(failCtx, Query{
		Schema: wide, Limit: 20, S3ParquetPathTemplate: env.ParquetGlob(),
	})
	if err == nil {
		t.Fatal("wrong-schema parquet under a hinted glob silently succeeded (#189 review P1)")
	}
	if !errors.Is(err, fedengine.ErrFederatedReadFailed) {
		t.Fatalf("glob-hinted wrong-schema parquet must classify as ErrFederatedReadFailed, got: %v", err)
	}
	if errors.Is(err, fedengine.ErrParquetSetInconsistent) {
		t.Errorf("object exists in storage; must not classify as manifest inconsistency: %v", err)
	}

	degraded := env.AssertQueryMatches(ctx, Query{
		Schema: wide, Limit: 20, S3ParquetPathTemplate: env.ParquetGlob(),
		AllowPartialDegradedMode: true,
	})
	assertDegradedFallbackPlan(t, degraded)
}
