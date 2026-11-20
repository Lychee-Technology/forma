//go:build e2e

package production

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/factory"
)

// factorySeedRows is the seeded row count; every phase reads the same set.
const factorySeedRows = 8

// TestFactoryWiring_ManifestDrivenReads is the ONLY coverage proving that
// factory.NewEntityManagerWithConfigContext actually reaches the engine with
// federated.WithPublicationReader over the Protocol V2 reader (#250, #896):
// the composition root's wiring is deliberately not unit-covered end to end,
// so if it were dropped, only this test fails. An engine without a reader
// fails every pass as misconfigured, so Phase 1 dies at once.
//
// Everything here goes through the PUBLIC surface — a config struct, the
// factory, and forma.EntityManager.Query — never Env.Engine()/Env.Query(),
// which assemble the engine themselves and would pass with the factory line
// removed. The factory pins each schema's epoch at construction, so the
// managers are built after provisioning wrote the records.
//
// The phases share one Env and run in order; each depends on the state the
// previous one left behind. The managers' deployments route every query
// through DuckDB (freshness-first), since a request no longer chooses its
// route or its parquet paths (#715).
func TestFactoryWiring_ManifestDrivenReads(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	simple := DefaultSchemaFixtures()[0]
	wide := DefaultSchemaFixtures()[1]

	// Phase 0 — seed and cold-ify: flush every row to parquet; the flush
	// marks every entry, so the hot tier contributes nothing. Any row the
	// later phases see can only have come from a parquet scan.
	creates := env.GenerateScript(ScriptSpec{Schema: simple, Creates: factorySeedRows})
	mustApplyEvents(ctx, t, env, "factory-path seed", creates...)
	mustFlush(ctx, t, env)
	env.RetireAllFlushed(ctx)
	// e2e_wide stays never published: its rows are hot only.
	hot := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 3})
	mustApplyEvents(ctx, t, env, "never-published seed", hot...)

	keys := schemaParquetKeys(ctx, t, env, simple)
	if len(keys) == 0 {
		t.Fatalf("seed produced no parquet objects for schema %s", simple.Name)
	}

	manager := newFactoryManager(ctx, t, env, false)
	degraded := newFactoryManager(ctx, t, env, true)

	t.Run("Phase1_ManifestListedRead", func(t *testing.T) {
		factoryPhaseManifestRead(ctx, t, env, manager, simple)
	})
	t.Run("Phase2_InconsistencyClassificationThroughPublicAPI", func(t *testing.T) {
		factoryPhaseInconsistency(ctx, t, env, manager, degraded, simple, keys[0])
	})
	t.Run("Phase3_NeverPublishedSchemaAnswersFromPostgres", func(t *testing.T) {
		factoryPhaseNeverPublished(ctx, t, manager, wide, len(hot))
	})
}

// factoryPhaseManifestRead is Phase 1: a federated query through the
// factory-built manager returns every flushed row.
//
// The garbage object PUT below is the manifest-vs-glob discriminator. It sits
// under the schema's current epoch, in the V2 key shape a flush writes, so a
// glob over the schema's prefix would expand onto it and the scan would fail
// on its unparsable bytes — the read path has no ignore_errors, so the
// failure would surface at scan time (characterized by
// TestManifestConsistency_OneGoodOneBadFile). The V2 reader never sees it: no
// generation lists it. A green Phase 1 therefore proves the scan set came from
// the published generation, not from a listing.
//
// CAVEAT for future maintainers: this assertion depends on the read path NOT
// tolerating unreadable objects. If parquet reads are ever softened (scan-level
// ignore_errors, or a validator that skips unreadable files), the garbage
// object stops failing a glob read and this discriminator silently weakens to
// a plain smoke test — replace it rather than let it rot.
func factoryPhaseManifestRead(ctx context.Context, t *testing.T, env *Env, manager forma.EntityManager, schema SchemaRef) {
	t.Helper()
	putGarbageObject(ctx, t, env, newTestParquetKey(ctx, t, env, schema, "delta"))

	res, err := factoryQuery(ctx, manager, schema)
	if err != nil {
		t.Fatalf("factory-path federated query: %v", err)
	}
	if len(res.Data) != factorySeedRows {
		t.Fatalf("factory-path query returned %d rows, want %d: the factory's publication reader did not reach the engine",
			len(res.Data), factorySeedRows)
	}
	assertFactoryUsedDuckDB(t, res)
}

// factoryPhaseInconsistency is Phase 2 (#250, #759): with an object the
// published generation lists missing from storage, the inconsistency must
// classify all the way out through the public EntityManager.Query error —
// sentinel, typed error, schema and missing key intact — matched through the
// public package, which is all an embedder building its manager via the
// factory can import.
//
// The listed object is a one-row copy of a seeded one under a fresh key,
// published and then deleted: state-equivalent to deleting a flushed object,
// but non-destructive, so the seeded objects stay readable.
func factoryPhaseInconsistency(ctx context.Context, t *testing.T, env *Env, manager, degradedManager forma.EntityManager, schema SchemaRef, seeded string) {
	t.Helper()
	ghostKey := newTestParquetKey(ctx, t, env, schema, "delta")
	writeParquetViaDuck(ctx, t, env, fmt.Sprintf(
		"SELECT * REPLACE (gen_random_uuid() AS row_id) FROM read_parquet('%s') LIMIT 1", env.ParquetURI(seeded)), ghostKey)
	if err := env.RegisterParquetInManifest(ctx, schema, ghostKey, "delta"); err != nil {
		t.Fatalf("list the ghost object in a generation: %v", err)
	}
	deleteObject(ctx, t, env, ghostKey)

	res, err := factoryQuery(ctx, manager, schema)
	if err == nil {
		t.Fatalf("query silently succeeded with %d rows while the generation lists a key storage does not have",
			len(res.Data))
	}
	assertFactoryInconsistency(t, err, schema, ghostKey)

	// Non-degradable: a deployment's AllowPartialDegradedMode absorbs
	// transient infrastructure faults, but a Postgres-only fallback here
	// would hide exactly the state the classification exists to report.
	degraded, err := factoryQuery(ctx, degradedManager, schema)
	if err == nil {
		t.Fatalf("degraded mode re-silenced the publication inconsistency (%d rows returned)", len(degraded.Data))
	}
	assertFactoryInconsistency(t, err, schema, ghostKey)
}

// factoryPhaseNeverPublished is Phase 3 (#720, #896): a schema whose record
// is UNPUBLISHED answers its live rows from PostgreSQL through the
// factory-built manager, with no DuckDB statement, although the deployment
// routes every query to DuckDB.
func factoryPhaseNeverPublished(ctx context.Context, t *testing.T, manager forma.EntityManager, schema SchemaRef, want int) {
	t.Helper()
	res, err := factoryQuery(ctx, manager, schema)
	if err != nil {
		t.Fatalf("query of a never-published schema: %v", err)
	}
	if len(res.Data) != want {
		t.Fatalf("never-published schema answered %d rows, want its %d live rows", len(res.Data), want)
	}
	if res.ExecutionPlan == nil {
		t.Fatal("the request asked for the execution plan but none was returned")
	}
	for _, src := range res.ExecutionPlan.Sources {
		if src.Engine == "duckdb" {
			t.Errorf("a never-published schema ran a DuckDB statement: %+v", src)
		}
	}
}

// newFactoryManager builds an EntityManager exactly the way a server does:
// a forma.Config plus the factory composition root, reading the published
// generations in the cluster bucket. degraded toggles the deployment's
// degraded fallback; the routing strategy sends every query through DuckDB.
func newFactoryManager(ctx context.Context, t *testing.T, env *Env, degraded bool) forma.EntityManager {
	t.Helper()
	return buildFactoryManager(ctx, t, env, factoryConfig(env, degraded), degraded)
}

// factoryConfig is the deployment configuration newFactoryManager builds
// with: the Env's registry, loaded at the Env's registry epochs.
func factoryConfig(env *Env, degraded bool) *forma.Config {
	cfg := forma.DefaultConfig(env.Registry)
	cfg.SchemaRegistryEpochs = env.RegistryEpochs
	cfg.Entity.SchemaDirectory = FixtureSchemasDir()
	cfg.Database.TableNames = forma.TableNames{
		SchemaRegistry: env.Tables.SchemaRegistry,
		EntityMain:     env.Tables.EntityMain,
		EAVData:        env.Tables.EAVData,
		ChangeLog:      env.Tables.ChangeLog,
	}

	duck := env.DuckCfg
	// Pins #245: DuckDB session S3 settings land on a single pooled connection,
	// so MaxConn > 1 against an in-memory database yields flaky 404s in
	// concurrent reads. Not a leak mitigation — the managers are closed below.
	duck.MaxConnections = 1
	duck.Routing.Strategy = forma.RoutingStrategyFreshnessFirst
	duck.AllowPartialDegradedMode = degraded
	duck.S3Bucket = env.Cluster.Bucket
	cfg.DuckDB = duck
	return cfg
}

// buildFactoryManager builds the manager of cfg with the factory and closes
// it when the test ends.
func buildFactoryManager(ctx context.Context, t *testing.T, env *Env, cfg *forma.Config, degraded bool) forma.EntityManager {
	t.Helper()
	manager, err := factory.NewEntityManagerWithConfigContext(ctx, cfg, env.Pool)
	if err != nil {
		t.Fatalf("build entity manager via factory (degraded=%t): %v", degraded, err)
	}
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Errorf("close factory-built manager (degraded=%t): %v", degraded, err)
		}
	})
	return manager
}

// factoryQuery issues the phases' common query through the public
// EntityManager surface; the manager's deployment decides the route.
func factoryQuery(ctx context.Context, manager forma.EntityManager, schema SchemaRef) (*forma.QueryResult, error) {
	res, err := manager.Query(ctx, &forma.QueryRequest{
		SchemaName:           schema.Name,
		Page:                 1,
		ItemsPerPage:         20,
		IncludeExecutionPlan: true,
	})
	if err != nil {
		return nil, fmt.Errorf("entity manager federated query (schema=%s): %w", schema.Name, err)
	}
	return res, nil
}

// assertFactoryUsedDuckDB pins that the answer came from the federated engine,
// not from a Postgres-only route that would make the parquet assertions vacuous.
func assertFactoryUsedDuckDB(t *testing.T, res *forma.QueryResult) {
	t.Helper()
	if res.ExecutionPlan == nil {
		t.Fatal("federated request asked for the execution plan but none was returned")
	}
	if !res.ExecutionPlan.Routing.UsedDuckDB {
		t.Fatalf("query did not route through DuckDB: %+v", res.ExecutionPlan.Routing)
	}
}

// assertFactoryInconsistency pins the full classification surface, through
// the public package: sentinel, typed error, offending schema, and the
// missing key by name.
func assertFactoryInconsistency(t *testing.T, err error, schema SchemaRef, missingKey string) {
	t.Helper()
	if !errors.Is(err, forma.ErrPublicationInconsistent) {
		t.Fatalf("error must classify as forma.ErrPublicationInconsistent, got: %v", err)
	}
	var typed *forma.PublicationConsistencyError
	if !errors.As(err, &typed) {
		t.Fatalf("error chain must carry *forma.PublicationConsistencyError, got: %v", err)
	}
	if typed.SchemaID != schema.ID {
		t.Errorf("inconsistency names schema %d, want %d", typed.SchemaID, schema.ID)
	}
	found := false
	for _, key := range typed.MissingKeys {
		if key == missingKey {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("inconsistency names missing keys %v, want it to contain %s", typed.MissingKeys, missingKey)
	}
}

// putGarbageObject writes an unreadable ".parquet" object that no generation
// lists: the bait a glob-based read would swallow.
func putGarbageObject(ctx context.Context, t *testing.T, env *Env, key string) {
	t.Helper()
	if _, err := env.Cluster.S3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(env.Cluster.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte("not a parquet file, not listed in any manifest")),
	}); err != nil {
		t.Fatalf("put unlisted garbage object %s: %v", key, err)
	}
}
