//go:build e2e

package production

import (
	"context"
	"testing"
)

// TestEnvIsolation provisions two parallel Envs on the shared cluster and
// verifies they get distinct databases, prefixes, and working metadata.
func TestEnvIsolation(t *testing.T) {
	cluster := SharedCluster(t)

	type envInfo struct {
		dbName, prefix string
	}
	results := make(chan envInfo, 2)

	for _, name := range []string{"A", "B"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			env := NewEnv(t, cluster)
			verifyEnvBasics(t, env)
			results <- envInfo{dbName: env.DBName, prefix: env.S3Prefix}
		})
	}

	t.Cleanup(func() {
		close(results)
		seenDB := map[string]bool{}
		seenPrefix := map[string]bool{}
		for info := range results {
			if seenDB[info.dbName] {
				t.Errorf("duplicate per-test database %s", info.dbName)
			}
			if seenPrefix[info.prefix] {
				t.Errorf("duplicate per-test s3 prefix %s", info.prefix)
			}
			seenDB[info.dbName] = true
			seenPrefix[info.prefix] = true
		}
	})
}

func verifyEnvBasics(t *testing.T, env *Env) {
	t.Helper()
	ctx := context.Background()

	var count int
	if err := env.Pool.QueryRow(ctx, "SELECT COUNT(*) FROM schema_registry").Scan(&count); err != nil {
		t.Fatalf("query schema_registry: %v", err)
	}
	if want := len(DefaultSchemaFixtures()); count != want {
		t.Fatalf("schema_registry rows = %d, want %d", count, want)
	}

	for _, ref := range DefaultSchemaFixtures() {
		id, cache, err := env.Registry.GetSchemaAttributeCacheByName(ref.Name)
		if err != nil {
			t.Fatalf("resolve schema %s: %v", ref.Name, err)
		}
		if id != ref.ID {
			t.Fatalf("schema %s resolved to id %d, want %d", ref.Name, id, ref.ID)
		}
		if len(cache) == 0 {
			t.Fatalf("schema %s has empty attribute cache", ref.Name)
		}
		if _, ok := env.Metadata.GetSchemaCacheByID(ref.ID); !ok {
			t.Fatalf("metadata cache missing schema id %d", ref.ID)
		}
	}

	// DuckDB client answers.
	var one int
	if err := env.Duck.DB.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
		t.Fatalf("duckdb query: %v", err)
	}
}
