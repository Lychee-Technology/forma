//go:build e2e

package production

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	fedengine "github.com/lychee-technology/forma/internal/federated"
)

// TestManifestConsistency_MissingListedParquet pins #187 scenario 2 and
// #759's partial-match case as a contract: deleting an object the published
// generation lists (5 flushed rows vanish from storage while the manifest
// still lists the file) must classify as ErrPublicationInconsistent naming
// the schema, its generation and the missing key — never silently succeed
// with fewer rows, which is what the flat glob did before manifest-driven
// reads: the glob expanded to whatever objects survived, so cold-tier data
// loss was invisible to callers and operators alike.
func TestManifestConsistency_MissingListedParquet(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	wide := DefaultSchemaFixtures()[1]

	keys := seedMultiParquet(ctx, t, env, wide)

	// Precondition: with all objects present the query routes through DuckDB
	// and matches the oracle, so the deletion below is what breaks it.
	healthy := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20})
	if healthy != nil && !healthy.Plan.Routing.UseDuckDB {
		t.Fatalf("precondition: healthy query did not route to duckdb: %+v", healthy.Plan.Routing)
	}

	deleteObject(ctx, t, env, keys[1])

	failCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, err := env.Query(failCtx, Query{Schema: wide, Limit: 20})
	if err == nil {
		t.Fatal("query silently succeeded with a manifest-listed parquet missing from storage (#187 scenario 2)")
	}
	if !errors.Is(err, fedengine.ErrPublicationInconsistent) {
		t.Fatalf("missing listed parquet must classify as ErrPublicationInconsistent, got: %v", err)
	}
	var typed *fedengine.PublicationConsistencyError
	if !errors.As(err, &typed) {
		t.Fatalf("error chain must carry PublicationConsistencyError, got: %v", err)
	}
	if typed.SchemaID != wide.ID {
		t.Errorf("inconsistency names schema %d, want %d", typed.SchemaID, wide.ID)
	}
	if published := loadSchemaManifest(ctx, t, env, wide); typed.Epoch != published.Epoch || typed.Generation != published.Generation {
		t.Errorf("inconsistency names epoch %d generation %d, want the published epoch %d generation %d",
			typed.Epoch, typed.Generation, published.Epoch, published.Generation)
	}
	if len(typed.MissingKeys) != 1 || typed.MissingKeys[0] != keys[1] {
		t.Errorf("inconsistency names missing keys %v, want [%s]", typed.MissingKeys, keys[1])
	}

	// Non-degradable: AllowPartialDegradedMode exists to absorb transient
	// infrastructure faults, and a Postgres-only fallback here would return
	// exactly the silent-loss answer this classification exists to prevent.
	degradedCtx, cancelDegraded := context.WithTimeout(ctx, 30*time.Second)
	defer cancelDegraded()
	_, err = env.Query(degradedCtx, Query{Schema: wide, Limit: 20, AllowPartialDegradedMode: true})
	if err == nil {
		t.Fatal("degraded mode re-silenced the manifest inconsistency (#187 scenario 2)")
	}
	if !errors.Is(err, fedengine.ErrPublicationInconsistent) {
		t.Fatalf("degraded-mode error must keep the inconsistency classification, got: %v", err)
	}
}

// TestManifestConsistency_OneGoodOneBadFile pins #187 scenario 7 under the
// #251 verification and the #722 refusal: one valid and one page-corrupt
// parquet in the same scan set. The corrupt file's footer is intact, so the
// pre-read validator accepts it and the failure surfaces mid-scan;
// per-file verification then attributes it and caches it (TTL), and the read
// fails rather than answering from the readable remainder. Later queries
// refuse from the cache without a scan. Confirmed corruption is
// breaker-neutral (it is not engine sickness), so a permanently corrupt
// object never turns the DuckDB route away, and degraded mode answers
// oracle-complete from PostgreSQL.
//
// The breaker is armed at threshold 1 deliberately: a default Env builds no
// breaker at all (engine.go only constructs one when breakerFailures > 0,
// and every CircuitBreaker method is a nil-receiver no-op), so neutrality
// would be unfalsifiable. At threshold 1 a single RecordFailure opens the
// circuit, and every later query would fail with ErrDuckDBUnavailable
// instead of the corrupt-object failure. The cooldown outlasts the rest of
// the test, so an opened breaker cannot silently heal before the assertions
// run.
func TestManifestConsistency_OneGoodOneBadFile(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster, WithBreaker(1, time.Minute))
	wide := DefaultSchemaFixtures()[1]

	keys := seedMultiParquet(ctx, t, env, wide)

	healthy := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20})
	if healthy == nil || !healthy.Plan.Routing.UseDuckDB {
		t.Fatalf("precondition: healthy query did not route to duckdb")
	}

	overwriteObjectBytes(ctx, t, env, keys[1], corruptMidFile)

	// The first query pays the failed scan and the verification; the rest
	// refuse from the cached verdict. Well past any failure threshold, none
	// may come back as a breaker rejection.
	for i := 0; i < 10; i++ {
		failCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		_, err := env.Query(failCtx, Query{Schema: wide, Limit: 20})
		cancel()
		if err == nil {
			t.Fatalf("query %d: degraded off must fail over a corrupt object (#722), got an answer", i)
		}
		assertCorruptRefusal(t, err, keys[1])
	}

	degraded := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20, AllowPartialDegradedMode: true})
	assertDegradedFallbackPlan(t, degraded)
}

// TestManifestConsistency_OneGoodOneTruncatedFile is the footer-dead sibling
// of OneGoodOneBadFile: truncation kills the footer, so the failure surfaces
// at Query (bind) time rather than mid-stream. Same contract, other error
// branch.
//
// The mode ordering is deliberately inverted relative to OneGoodOneBadFile:
// here the FIRST post-corruption query — the only one that pays a real
// failed scan, before any cache entry exists — carries
// AllowPartialDegradedMode, so it pins that a fresh confirmation degrades to
// the oracle-complete PostgreSQL answer. The query after it, degraded off,
// pins that the confirmation was cached even though the fallback absorbed
// it. Between the two tests, both error branches get first-query coverage in
// both modes. The breaker is armed at threshold 1 for the reason documented
// on OneGoodOneBadFile.
func TestManifestConsistency_OneGoodOneTruncatedFile(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster, WithBreaker(1, time.Minute))
	wide := DefaultSchemaFixtures()[1]

	keys := seedMultiParquet(ctx, t, env, wide)

	healthy := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20})
	if healthy == nil || !healthy.Plan.Routing.UseDuckDB {
		t.Fatalf("precondition: healthy query did not route to duckdb")
	}

	overwriteObjectBytes(ctx, t, env, keys[1], truncateHalf)

	degraded := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20, AllowPartialDegradedMode: true})
	assertDegradedFallbackPlan(t, degraded)

	failCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	_, err := env.Query(failCtx, Query{Schema: wide, Limit: 20})
	if err == nil {
		t.Fatal("degraded off: the cached verdict must refuse the read (#722), got an answer")
	}
	assertCorruptRefusal(t, err, keys[1])
	if !strings.Contains(err.Error(), "still cached") {
		t.Errorf("the second query must refuse from the cached verdict, got: %v", err)
	}
}
