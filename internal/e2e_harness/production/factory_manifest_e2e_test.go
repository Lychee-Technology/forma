//go:build e2e

package production

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/factory"
	fedengine "github.com/lychee-technology/forma/internal/federated"
)

// factorySeedRows is the seeded row count; every phase reads the same set.
const factorySeedRows = 8

// TestFactoryWiring_ManifestDrivenReads is the ONLY coverage proving that
// factory.NewEntityManagerWithConfigContext actually reaches the engine with
// federated.WithParquetSource (#250): the composition root's one-line
// `engineOpts = append(engineOpts, federated.WithParquetSource(...))` is
// deliberately not unit-covered, so if it were dropped, only this test fails.
//
// Everything here goes through the PUBLIC surface — a config struct, the
// factory, and forma.EntityManager.Query — never Env.Engine()/Env.Query(),
// which assemble the engine themselves and would pass with the factory line
// removed.
//
// The four phases share one Env and run in order; each depends on the state
// the previous one left behind.
func TestFactoryWiring_ManifestDrivenReads(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	simple := DefaultSchemaFixtures()[0]

	// Phase 0 — seed and cold-ify: flush every row to parquet, then drop the
	// change_log entries so the hot tier contributes nothing. Any row the
	// later phases see can only have come from a parquet scan.
	creates := env.GenerateScript(ScriptSpec{Schema: simple, Creates: factorySeedRows})
	mustApplyEvents(ctx, t, env, "factory-path seed", creates...)
	mustFlush(ctx, t, env)
	env.ExecSQL(ctx, "DELETE FROM change_log")

	keys := schemaParquetKeys(ctx, t, env, simple)
	if len(keys) == 0 {
		t.Fatalf("seed produced no parquet objects for schema %s", simple.Name)
	}

	manager := newFactoryManager(ctx, t, env, true)
	ghostKey := fmt.Sprintf("%s/%d/zz_ghost_never_written.parquet", env.S3Prefix, simple.ID)

	t.Run("Phase1_ManifestListedReadWithoutHint", func(t *testing.T) {
		factoryPhaseManifestRead(ctx, t, env, manager, simple)
	})
	t.Run("Phase2_ExplicitHintBypassesInconsistentManifest", func(t *testing.T) {
		factoryPhaseHintWins(ctx, t, env, manager, simple, keys, ghostKey)
	})
	t.Run("Phase3_InconsistencyClassificationThroughPublicAPI", func(t *testing.T) {
		factoryPhaseInconsistency(ctx, t, manager, simple, ghostKey)
	})
	t.Run("Phase4_NegativeControlManifestOff", func(t *testing.T) {
		factoryPhaseManifestOff(ctx, t, env, simple, keys)
	})
}

// factoryPhaseManifestRead is Phase 1: a hint-less federated query through the
// factory-built manager returns every flushed row.
//
// The garbage object PUT below is the manifest-vs-glob discriminator. It sits
// inside the schema's data prefix, so the legacy glob
// (s3://bucket/{prefix}/{schemaID}/*.parquet) expands onto it and the scan
// fails on its unparsable bytes — the #189 pre-read validator treats an
// unreadable footer as inconclusive and defers, and the read path has no
// ignore_errors, so the failure surfaces at scan time (characterized by
// TestManifestConsistency_OneGoodOneBadFile). Manifest-driven reads never see
// it: it is not listed. A green Phase 1 therefore proves the scan set came
// from the manifest, not from a glob.
//
// CAVEAT for future maintainers: this assertion depends on the read path NOT
// tolerating unreadable objects. If parquet reads are ever softened (scan-level
// ignore_errors, or a validator that skips unreadable files), the garbage
// object stops failing a glob read and this discriminator silently weakens to
// a plain smoke test — replace it rather than let it rot.
func factoryPhaseManifestRead(ctx context.Context, t *testing.T, env *Env, manager forma.EntityManager, schema SchemaRef) {
	t.Helper()
	putGarbageObject(ctx, t, env, fmt.Sprintf("%s/%d/zz_unlisted.parquet", env.S3Prefix, schema.ID))

	res, err := factoryQuery(ctx, manager, schema, factoryQueryOpts{})
	if err != nil {
		t.Fatalf("factory-path federated query without hint: %v", err)
	}
	if len(res.Data) != factorySeedRows {
		t.Fatalf("factory-path query returned %d rows, want %d: the factory's parquet source did not reach the engine",
			len(res.Data), factorySeedRows)
	}
	assertFactoryUsedDuckDB(t, res)
}

// factoryPhaseHintWins is Phase 2 (#250 issue question 3): an explicit
// S3ParquetPathTemplate wins over the manifest-driven source, including when
// the manifest has since gone inconsistent. A ghost key — listed in the
// manifest but never written — makes the manifest path fail; the hint names
// the real objects, so the query must still succeed.
func factoryPhaseHintWins(ctx context.Context, t *testing.T, env *Env, manager forma.EntityManager, schema SchemaRef, keys []string, ghostKey string) {
	t.Helper()
	if err := env.RegisterParquetInManifest(ctx, schema, ghostKey, "delta"); err != nil {
		t.Fatalf("register ghost key in manifest: %v", err)
	}

	uris := make([]string, 0, len(keys))
	for _, key := range keys {
		uris = append(uris, fmt.Sprintf("s3://%s/%s", env.Cluster.Bucket, key))
	}
	res, err := factoryQuery(ctx, manager, schema, factoryQueryOpts{hint: strings.Join(uris, ",")})
	if err != nil {
		t.Fatalf("explicit parquet hint did not bypass the inconsistent manifest: %v", err)
	}
	if len(res.Data) != factorySeedRows {
		t.Fatalf("hinted query returned %d rows, want %d", len(res.Data), factorySeedRows)
	}
	assertFactoryUsedDuckDB(t, res)
}

// factoryPhaseInconsistency is Phase 3: with the ghost key still listed and no
// hint to bypass it, the manifest inconsistency must classify all the way out
// through the public EntityManager.Query error — sentinel, typed error, schema
// and missing key intact.
//
// The ghost key is state-equivalent to deleting keys[0] (manifest lists a key
// storage does not have) but non-destructive, so the phases after it still
// have their objects. Either vector exercises the same classification path.
//
// Together with Phase 1 this is what dies if federated.WithParquetSource is
// dropped from the factory: with no source the engine resolves an empty path
// set for a hint-less query, so the read never reaches the manifest-listed
// objects and the classification is unreachable (verified by mutation — the
// engine then fails the way Phase 4's manifest-off control does). Phases 2
// and 4 keep passing under that mutation, which is what makes this phase the
// discriminator rather than a generic smoke test.
func factoryPhaseInconsistency(ctx context.Context, t *testing.T, manager forma.EntityManager, schema SchemaRef, ghostKey string) {
	t.Helper()
	res, err := factoryQuery(ctx, manager, schema, factoryQueryOpts{})
	if err == nil {
		t.Fatalf("hint-less query silently succeeded with %d rows while the manifest lists a key storage does not have",
			len(res.Data))
	}
	assertFactoryInconsistency(t, err, schema, ghostKey)

	// Non-degradable: AllowPartialDegradedMode absorbs transient
	// infrastructure faults, but a Postgres-only fallback here would return
	// exactly the silent short answer the classification exists to prevent.
	degraded, err := factoryQuery(ctx, manager, schema, factoryQueryOpts{degraded: true})
	if err == nil {
		t.Fatalf("degraded mode re-silenced the manifest inconsistency (%d rows returned)", len(degraded.Data))
	}
	assertFactoryInconsistency(t, err, schema, ghostKey)
}

// factoryPhaseManifestOff is Phase 4, the negative control: a second manager
// built from the same config with the manifest read surface cleared gets no
// parquet source at all, so the inconsistent manifest of Phases 2-3 is
// invisible to it. Its outcome must NOT carry the inconsistency
// classification — that proves Phase 3's error came from the new factory
// wiring rather than from ambient environment damage.
//
// The whole surface is cleared, not just ManifestTemplate: ValidateManifestRead
// rejects a ManifestPrefix/S3DataPrefix set without a template, so "manifest
// off" is all-or-nothing by construction.
//
// Three assertions, in increasing strength:
//
//  1. The hint-less query fails with ErrNoParquetPaths (#299). With no source
//     and no hint nothing authors a path set, and since #299 that is its own
//     non-degradable classification rather than the degradable read failure a
//     `read_parquet(<no value>)` parser error used to produce.
//  2. That failure is not the Phase 3 classification, and degraded mode does
//     not absorb it.
//  3. The SAME manager, given an explicit hint, reads every seeded row. This
//     is what gives the control discriminating power: it proves the objects are
//     healthy and this manager's engine works, so Phase 3's error can only have
//     come from the manifest wiring — not from a broken environment that would
//     fail every query regardless. Before #299 the hint-less query died at
//     parse time and no positive assertion was possible here at all.
func factoryPhaseManifestOff(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, keys []string) {
	t.Helper()
	manager := newFactoryManager(ctx, t, env, false)

	_, err := factoryQuery(ctx, manager, schema, factoryQueryOpts{})
	if err == nil {
		t.Fatal("manifest-off, hint-less query succeeded: with no parquet source and no hint there is no path set to scan")
	}
	// Matched through the PUBLIC package, not internal/federated: an embedder
	// building its manager via factory.NewEntityManagerWithConfig* cannot import
	// an internal package, so a sentinel only reachable there would leave them
	// comparing error text. Asserting the public name here is what proves the
	// discriminator #299 promised is actually usable (review P2).
	if !errors.Is(err, forma.ErrNoParquetPaths) {
		t.Fatalf("manifest-off, hint-less query must classify as forma.ErrNoParquetPaths (#299), got: %v", err)
	}
	var noPaths *forma.NoParquetPathsError
	if !errors.As(err, &noPaths) {
		t.Fatalf("error must carry the public detail type, got: %v", err)
	}
	if noPaths.SchemaID != schema.ID {
		t.Fatalf("detail names schema %d, want %d", noPaths.SchemaID, schema.ID)
	}
	if noPaths.SourceConfigured {
		t.Fatal("manifest reads are off here, so no source was consulted; the remedy differs and the flag must say so")
	}
	if errors.Is(err, fedengine.ErrParquetSetInconsistent) {
		t.Fatalf("manifest-off manager reported a manifest inconsistency; Phase 3's classification is environment noise, not wiring: %v", err)
	}
	if errors.Is(err, fedengine.ErrFederatedReadFailed) {
		t.Fatalf("an unresolvable path set must not look like a transient read failure — that conflation is the #299 bug: %v", err)
	}

	// Non-degradable: a Postgres-only answer here would silently omit the cold
	// tier the request asked for.
	if _, derr := factoryQuery(ctx, manager, schema, factoryQueryOpts{degraded: true}); derr == nil {
		t.Fatal("degraded mode absorbed the empty path set; a misconfigured read surface must stay loud (#299)")
	} else if !errors.Is(derr, forma.ErrNoParquetPaths) {
		t.Fatalf("degraded-mode failure lost its classification: %v", derr)
	}

	// Positive control on the same manager: the objects are fine and the engine
	// reads them when told where to look.
	uris := make([]string, 0, len(keys))
	for _, key := range keys {
		uris = append(uris, fmt.Sprintf("s3://%s/%s", env.Cluster.Bucket, key))
	}
	res, err := factoryQuery(ctx, manager, schema, factoryQueryOpts{hint: strings.Join(uris, ",")})
	if err != nil {
		t.Fatalf("manifest-off manager could not read the seeded objects even with an explicit hint, so Phase 3 proves nothing about wiring: %v", err)
	}
	if len(res.Data) != factorySeedRows {
		t.Fatalf("hinted manifest-off query returned %d rows, want %d", len(res.Data), factorySeedRows)
	}
	assertFactoryUsedDuckDB(t, res)
}

// newFactoryManager builds an EntityManager exactly the way a server does:
// a forma.Config plus the factory composition root. manifestOn toggles the
// manifest read surface, which is the only difference between the two
// managers this test builds.
func newFactoryManager(ctx context.Context, t *testing.T, env *Env, manifestOn bool) forma.EntityManager {
	t.Helper()
	cfg := forma.DefaultConfig(env.Registry)
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
	if manifestOn {
		duck.S3Bucket = env.Cluster.Bucket
		duck.S3DataPrefix = env.S3Prefix
		duck.ManifestPrefix = env.CDC.ManifestPrefix
		duck.ManifestTemplate = env.CDC.ManifestTemplate
	}
	cfg.DuckDB = duck

	manager, err := factory.NewEntityManagerWithConfigContext(ctx, cfg, env.Pool)
	if err != nil {
		t.Fatalf("build entity manager via factory (manifest=%t): %v", manifestOn, err)
	}
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Errorf("close factory-built manager (manifest=%t): %v", manifestOn, err)
		}
	})
	return manager
}

// factoryQueryOpts carries the per-phase knobs of the shared query.
type factoryQueryOpts struct {
	hint     string
	degraded bool
}

// factoryQuery issues the phases' common federated query through the public
// EntityManager surface.
func factoryQuery(ctx context.Context, manager forma.EntityManager, schema SchemaRef, opts factoryQueryOpts) (*forma.QueryResult, error) {
	res, err := manager.Query(ctx, &forma.QueryRequest{
		SchemaName:   schema.Name,
		Page:         1,
		ItemsPerPage: 20,
		Federated: &forma.FederatedQueryRequest{
			Enabled:                  true,
			PreferredTiers:           []string{"hot", "warm", "cold"},
			S3ParquetPathTemplate:    opts.hint,
			AllowPartialDegradedMode: opts.degraded,
			IncludeExecutionPlan:     true,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("entity manager federated query (schema=%s, hinted=%t): %w",
			schema.Name, opts.hint != "", err)
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

// assertFactoryInconsistency pins the full classification surface: sentinel,
// typed error, offending schema, and the missing key by name.
func assertFactoryInconsistency(t *testing.T, err error, schema SchemaRef, missingKey string) {
	t.Helper()
	if !errors.Is(err, fedengine.ErrParquetSetInconsistent) {
		t.Fatalf("error must classify as ErrParquetSetInconsistent, got: %v", err)
	}
	var typed *fedengine.ParquetSetInconsistentError
	if !errors.As(err, &typed) {
		t.Fatalf("error chain must carry *ParquetSetInconsistentError, got: %v", err)
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

// putGarbageObject writes an unreadable ".parquet" object that no manifest
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
