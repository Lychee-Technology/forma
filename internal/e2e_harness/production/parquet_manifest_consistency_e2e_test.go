//go:build e2e

package production

import (
	"context"
	"errors"
	"testing"
	"time"

	fedengine "github.com/lychee-technology/forma/internal/federated"
)

// TestManifestConsistency_MissingListedParquet pins #187 scenario 2 as a
// contract: deleting a manifest-listed parquet object (5 flushed rows vanish
// from storage while the manifest still lists the file) must classify as
// ErrParquetSetInconsistent naming the missing key — never silently succeed
// with fewer rows, which is what the flat glob did before manifest-driven
// reads: the glob expanded to whatever objects survived, so cold-tier data
// loss was invisible to callers and operators alike.
func TestManifestConsistency_MissingListedParquet(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster, WithDuckMaxConnections(1))
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
	if !errors.Is(err, fedengine.ErrParquetSetInconsistent) {
		t.Fatalf("missing listed parquet must classify as ErrParquetSetInconsistent, got: %v", err)
	}
	var typed *fedengine.ParquetSetInconsistentError
	if !errors.As(err, &typed) {
		t.Fatalf("error chain must carry ParquetSetInconsistentError, got: %v", err)
	}
	if typed.SchemaID != wide.ID {
		t.Errorf("inconsistency names schema %d, want %d", typed.SchemaID, wide.ID)
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
	if !errors.Is(err, fedengine.ErrParquetSetInconsistent) {
		t.Fatalf("degraded-mode error must keep the inconsistency classification, got: %v", err)
	}
}
