//go:build e2e

package production

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"

	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/manifest"
	"github.com/lychee-technology/forma/internal/reconcile"
)

// newReconcileHarness wires a real reconcile.Reconciler the way the
// manifest-reconcile tool does: real S3 listing/deletion, raw object reads,
// etag manifest store, the flusher's advisory lock over a database/sql
// handle, registry enumeration, and (when withStats) a DuckDB stats engine
// reading parquet through httpfs. The returned cleanup closes both handles.
func newReconcileHarness(t *testing.T, ctx context.Context, env *Env, schema SchemaRef, opts reconcile.Options, withStats bool) (*reconcile.Reconciler, func()) {
	t.Helper()

	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		env.CDC.PGHost, env.CDC.PGPort, env.CDC.PGUser, env.CDC.PGPassword, env.CDC.PGDB, env.CDC.PGSSLMode)
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		t.Fatalf("open pg for reconcile: %v", err)
	}

	store := &reconcile.S3ObjectStore{Client: env.Cluster.S3, Bucket: env.Cluster.Bucket}
	manifestStore := &manifest.S3Store{Client: env.Cluster.S3, Bucket: env.Cluster.Bucket}
	resolver := manifest.PathResolver{Prefix: env.CDC.ManifestPrefix, PathTemplate: env.CDC.ManifestTemplate}
	r := &reconcile.Reconciler{
		Lister:  store,
		Deleter: store,
		// Raw byte reads, wired unconditionally the way cmd/tools'
		// newReconciler wires them: without Objects the #347 checksum scrub
		// answers a configuration error instead of scrubbing, and --repair
		// publishes its adopted and promoted entries unstamped.
		Objects:    env.Cluster.S3,
		Manifests:  &reconcile.ResolverManifestStore{Store: manifestStore, Resolver: resolver},
		GCStates:   &reconcile.ManifestGCStateStore{Store: manifestStore, Resolver: resolver},
		Locker:     &reconcile.PGAdvisoryLocker{DB: db},
		Schemas:    &reconcile.RegistrySchemaEnumerator{DB: db, Table: env.Tables.SchemaRegistry, SchemaIDFilter: int(schema.ID)},
		Now:        time.Now,
		Bucket:     env.Cluster.Bucket,
		DataPrefix: env.S3Prefix,
		Logger:     env.logger,
		Opts:       opts,
	}

	cleanup := func() { _ = db.Close() }
	if withStats {
		s3Key, s3Secret, s3Token := cdc.ResolveStaticS3Credentials(env.CDC)
		exporter, err := cdc.NewDuckExporter(ctx, env.CDC, s3Key, s3Secret, s3Token, env.logger)
		if err != nil {
			_ = db.Close()
			t.Fatalf("open stats duckdb: %v", err)
		}
		exporter.DB.SetMaxOpenConns(1)
		r.Stats = &reconcile.DuckStatsReader{DB: exporter.DB, Bucket: env.Cluster.Bucket}
		r.LiveRows = &reconcile.PGLiveRows{DB: db, Table: env.Tables.EntityMain}
		cleanup = func() {
			_ = exporter.DB.Close()
			_ = db.Close()
		}
	}
	return r, cleanup
}

func loadManifestWithETag(t *testing.T, ctx context.Context, env *Env, schema SchemaRef) (*manifest.Manifest, string) {
	t.Helper()
	store := &manifest.S3Store{Client: env.Cluster.S3, Bucket: env.Cluster.Bucket}
	resolver := manifest.PathResolver{Prefix: env.CDC.ManifestPrefix, PathTemplate: env.CDC.ManifestTemplate}
	path, err := resolver.Resolve(schema.ID)
	if err != nil {
		t.Fatalf("resolve manifest path: %v", err)
	}
	m, etag, err := manifest.Load(ctx, store, path)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	return m, etag
}

func saveManifestWithETag(t *testing.T, ctx context.Context, env *Env, schema SchemaRef, m *manifest.Manifest, etag string) {
	t.Helper()
	store := &manifest.S3Store{Client: env.Cluster.S3, Bucket: env.Cluster.Bucket}
	resolver := manifest.PathResolver{Prefix: env.CDC.ManifestPrefix, PathTemplate: env.CDC.ManifestTemplate}
	path, err := resolver.Resolve(schema.ID)
	if err != nil {
		t.Fatalf("resolve manifest path: %v", err)
	}
	if _, err := manifest.Save(ctx, store, path, m, etag); err != nil {
		t.Fatalf("save manifest: %v", err)
	}
}

func putJunkObject(t *testing.T, ctx context.Context, env *Env, key string) {
	t.Helper()
	_, err := env.Cluster.S3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(env.Cluster.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte("junk parquet placeholder")),
	})
	if err != nil {
		t.Fatalf("put junk object %s: %v", key, err)
	}
}

// TestManifestReconcile_RepairsDeltaOrphan drives the #197 recovery path
// end to end: a flushed delta whose manifest append was lost is invisible
// to federated reads; report mode names it without mutating anything, and
// --repair appends an entry whose metadata — recomputed from the parquet
// via DuckDB — matches what the flusher originally wrote, restoring query
// visibility. A second repair run proves idempotency.
func TestManifestReconcile_RepairsDeltaOrphan(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	schema := DefaultSchemaFixtures()[0] // e2e_simple

	original := seedStrippedDeltaOrphan(t, ctx, env, schema)

	t.Run("report_only_names_diff_and_mutates_nothing", func(t *testing.T) {
		testReconcileReportOnly(t, ctx, env, schema, original)
	})
	t.Run("repair_restores_entry_and_visibility", func(t *testing.T) {
		testReconcileRepairRestores(t, ctx, env, schema, original)
	})
	t.Run("second_repair_run_is_idempotent", func(t *testing.T) {
		testReconcileRepairIdempotent(t, ctx, env, schema)
	})
}

// seedStrippedDeltaOrphan flushes two batches and strips the second flush's
// delta entry from the manifest — the exact post-state of a #197 manifest
// append failure (file at its final key, rows marked flushed, manifest
// without the entry). Two batches keep the manifest non-empty: an empty
// manifest sends reads to the legacy glob fallback
// (manifest/query_source.go), which would see the orphan and defeat the
// visibility oracle. Returns the stripped entry.
func seedStrippedDeltaOrphan(t *testing.T, ctx context.Context, env *Env, schema SchemaRef) *manifest.FileEntry {
	t.Helper()
	seed := env.GenerateScript(ScriptSpec{Schema: schema, Creates: 4})
	if err := env.ApplyEvents(ctx, seed...); err != nil {
		t.Fatalf("apply seed events: %v", err)
	}
	mustFlush(ctx, t, env)
	mKept, _ := loadManifestWithETag(t, ctx, env, schema)
	keptPaths := make(map[string]bool, len(mKept.Files))
	for _, f := range mKept.Files {
		keptPaths[f.Path] = true
	}

	second := env.GenerateScript(ScriptSpec{Schema: schema, Creates: 3})
	if err := env.ApplyEvents(ctx, second...); err != nil {
		t.Fatalf("apply second batch: %v", err)
	}
	mustFlush(ctx, t, env)

	m, etag := loadManifestWithETag(t, ctx, env, schema)
	var original *manifest.FileEntry
	var kept []manifest.FileEntry
	for _, f := range m.Files {
		if f.Tier == "delta" && !keptPaths[f.Path] && original == nil {
			e := f
			original = &e
			continue
		}
		kept = append(kept, f)
	}
	if original == nil {
		t.Fatal("second flush produced no new delta manifest entry to strip")
	}
	m.Files = kept
	saveManifestWithETag(t, ctx, env, schema, m, etag)

	stripped, err := env.Query(ctx, Query{Schema: schema, Limit: 100})
	if err != nil {
		t.Fatalf("query after strip: %v", err)
	}
	if len(stripped.Records) != 4 {
		t.Fatalf("stripped manifest yields %d records, want 4 (second batch must be invisible)", len(stripped.Records))
	}
	return original
}

func testReconcileReportOnly(t *testing.T, ctx context.Context, env *Env, schema SchemaRef, original *manifest.FileEntry) {
	// Dangling probe: a manifest entry pointing at a key that never
	// existed. Removed again at the end so the ghost does not break later
	// read-path assertions.
	ghostKey := fmt.Sprintf("%s/%d/%s.parquet", env.S3Prefix, schema.ID, uuid.Must(uuid.NewV7()).String())
	if err := env.RegisterParquetInManifest(ctx, schema, ghostKey, "delta"); err != nil {
		t.Fatalf("register ghost manifest entry: %v", err)
	}

	mBefore, _ := loadManifestWithETag(t, ctx, env, schema)
	r, cleanup := newReconcileHarness(t, ctx, env, schema, reconcile.Options{}, false)
	defer cleanup()

	report, err := r.Run(ctx)
	if err != nil {
		t.Fatalf("reconcile report run: %v", err)
	}
	if len(report.Schemas) != 1 {
		t.Fatalf("expected 1 schema report, got %d", len(report.Schemas))
	}
	s := report.Schemas[0]
	if len(s.DeltaOrphans) != 1 || s.DeltaOrphans[0] != original.Path {
		t.Fatalf("delta orphans = %v, want [%s]", s.DeltaOrphans, original.Path)
	}
	if len(s.Dangling) != 1 || s.Dangling[0] != ghostKey {
		t.Fatalf("dangling = %v, want [%s]", s.Dangling, ghostKey)
	}
	if !report.HasResidualDiscrepancies() {
		t.Fatal("report run must flag residual discrepancies")
	}
	mAfter, etagAfter := loadManifestWithETag(t, ctx, env, schema)
	if mAfter.Version != mBefore.Version {
		t.Fatalf("report mode bumped manifest version %d -> %d", mBefore.Version, mAfter.Version)
	}

	// Remove the ghost entry again.
	var files []manifest.FileEntry
	for _, f := range mAfter.Files {
		if f.Path != ghostKey {
			files = append(files, f)
		}
	}
	mAfter.Files = files
	saveManifestWithETag(t, ctx, env, schema, mAfter, etagAfter)
}

func testReconcileRepairRestores(t *testing.T, ctx context.Context, env *Env, schema SchemaRef, original *manifest.FileEntry) {
	r, cleanup := newReconcileHarness(t, ctx, env, schema,
		reconcile.Options{Repair: true, MaxETagRetries: 3}, true)
	defer cleanup()

	report, err := r.Run(ctx)
	if err != nil {
		t.Fatalf("reconcile repair run: %v", err)
	}
	s := report.Schemas[0]
	if len(s.Repaired) != 1 || s.Repaired[0] != original.Path {
		t.Fatalf("repaired = %v, want [%s]", s.Repaired, original.Path)
	}

	mAfter, _ := loadManifestWithETag(t, ctx, env, schema)
	var repaired *manifest.FileEntry
	for _, f := range mAfter.Files {
		if f.Path == original.Path {
			e := f
			repaired = &e
			break
		}
	}
	if repaired == nil {
		t.Fatalf("repaired entry %s missing from manifest: %+v", original.Path, mAfter.Files)
	}
	assertRepairedEntryMatches(t, repaired, original)

	result, err := env.Query(ctx, Query{Schema: schema, Limit: 100})
	if err != nil {
		t.Fatalf("query after repair: %v", err)
	}
	if len(result.Records) != 7 {
		t.Fatalf("query after repair returned %d records, want 7 (both flush batches)", len(result.Records))
	}
	if !result.Plan.Routing.UseDuckDB {
		t.Errorf("post-repair query did not route through duckdb: %+v", result.Plan.Routing)
	}
}

// assertRepairedEntryMatches compares a repaired entry against the
// flusher-written original: identity metadata must match exactly; RowID
// bounds tolerate the flusher's same-millisecond first-seen tie-break
// (repair recomputes lexicographic MIN/MAX, which always bounds the
// flusher's picks over the same set); Created* semantics differ by design —
// the flusher stamps the flush timestamp, reconcile recomputes the
// parquet's changed_at range (compaction's convention for merged files),
// and row changed_at is always <= the flush timestamp.
func assertRepairedEntryMatches(t *testing.T, repaired, original *manifest.FileEntry) {
	t.Helper()
	if repaired.Tier != original.Tier ||
		repaired.RowCount != original.RowCount ||
		repaired.SizeBytes != original.SizeBytes {
		t.Fatalf("repaired entry diverges from original:\n got %+v\nwant %+v", *repaired, *original)
	}
	if repaired.RowIDMin == "" || repaired.RowIDMin > original.RowIDMin ||
		repaired.RowIDMax < original.RowIDMax {
		t.Fatalf("repaired RowID range [%s, %s] does not bound original [%s, %s]",
			repaired.RowIDMin, repaired.RowIDMax, original.RowIDMin, original.RowIDMax)
	}
	if repaired.CreatedMin <= 0 || repaired.CreatedMin > repaired.CreatedMax ||
		repaired.CreatedMax > original.CreatedMax {
		t.Fatalf("repaired Created range [%d, %d] outside (0, %d]",
			repaired.CreatedMin, repaired.CreatedMax, original.CreatedMax)
	}
}

func testReconcileRepairIdempotent(t *testing.T, ctx context.Context, env *Env, schema SchemaRef) {
	mBefore, _ := loadManifestWithETag(t, ctx, env, schema)
	r, cleanup := newReconcileHarness(t, ctx, env, schema,
		reconcile.Options{Repair: true, MaxETagRetries: 3}, true)
	defer cleanup()

	report, err := r.Run(ctx)
	if err != nil {
		t.Fatalf("second reconcile repair run: %v", err)
	}
	if len(report.Schemas[0].Repaired) != 0 {
		t.Fatalf("second run repaired %v, want nothing", report.Schemas[0].Repaired)
	}
	mAfter, _ := loadManifestWithETag(t, ctx, env, schema)
	if mAfter.Version != mBefore.Version {
		t.Fatalf("idempotent run bumped manifest version %d -> %d", mBefore.Version, mAfter.Version)
	}
	assertNoDuplicateManifestEntries(t, mAfter)
}

// TestManifestReconcile_GCRemovesRewriteLeftovers drives the #188 recovery
// path with the two-phase sighting contract: the first --gc run only
// records each leftover's first-unlisted sighting (persisted next to the
// manifest), and a later run deletes it once BOTH the observed-unlisted
// duration and the object age exceed the grace period. Init-shaped base
// orphans join the sweep since #290 (cdc-init holds the same per-schema
// advisory lock reconcile takes). Delta-shaped orphans and manifest-listed
// objects are never touched.
func TestManifestReconcile_GCRemovesRewriteLeftovers(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	schema := DefaultSchemaFixtures()[0] // e2e_simple

	seed := env.GenerateScript(ScriptSpec{Schema: schema, Creates: 3})
	if err := env.ApplyEvents(ctx, seed...); err != nil {
		t.Fatalf("apply seed events: %v", err)
	}
	mustFlush(ctx, t, env)

	staleBase := fmt.Sprintf("%s/%d/base-%s.parquet", env.S3Prefix, schema.ID, uuid.Must(uuid.NewV7()).String())
	staleTmp := fmt.Sprintf("%s/%d/_tmp/%s.parquet", env.S3Prefix, schema.ID, uuid.Must(uuid.NewV7()).String())
	deltaOrphan := fmt.Sprintf("%s/%d/%s.parquet", env.S3Prefix, schema.ID, uuid.Must(uuid.NewV7()).String())
	initShaped := fmt.Sprintf("%s/%d/%s_%s.parquet", env.S3Prefix, schema.ID,
		uuid.Must(uuid.NewV7()).String(), uuid.Must(uuid.NewV7()).String())
	putJunkObject(t, ctx, env, staleBase)
	putJunkObject(t, ctx, env, staleTmp)
	putJunkObject(t, ctx, env, deltaOrphan)
	putJunkObject(t, ctx, env, initShaped)

	gcOpts := reconcile.Options{GC: true, GCGrace: 15 * time.Minute}

	t.Run("within_grace_nothing_deleted", func(t *testing.T) {
		// This run doubles as the sighting-recording phase: it persists
		// each candidate's first-unlisted timestamp for the next run.
		r, cleanup := newReconcileHarness(t, ctx, env, schema, gcOpts, false)
		defer cleanup()

		report, err := r.Run(ctx)
		if err != nil {
			t.Fatalf("gc run within grace: %v", err)
		}
		s := report.Schemas[0]
		if len(s.Deleted) != 0 {
			t.Fatalf("gc within grace deleted %v, want nothing", s.Deleted)
		}
		if len(s.BaseOrphans) != 2 || len(s.TmpOrphans) != 1 || len(s.DeltaOrphans) != 1 {
			t.Fatalf("expected 2 base (merged+init) / 1 tmp / 1 delta orphans, got %+v", s)
		}
	})

	t.Run("past_grace_deletes_leftovers_only", func(t *testing.T) {
		mBefore, _ := loadManifestWithETag(t, ctx, env, schema)
		r, cleanup := newReconcileHarness(t, ctx, env, schema, gcOpts, false)
		defer cleanup()
		// Pretend the run happens an hour later instead of sleeping past
		// the grace period.
		r.Now = func() time.Time { return time.Now().Add(time.Hour) }

		report, err := r.Run(ctx)
		if err != nil {
			t.Fatalf("gc run past grace: %v", err)
		}
		s := report.Schemas[0]
		if len(s.Deleted) != 3 {
			t.Fatalf("gc deleted %v, want the merged-base, _tmp, and init-shaped leftovers", s.Deleted)
		}

		keys, err := env.listS3Keys(ctx)
		if err != nil {
			t.Fatalf("list s3 keys: %v", err)
		}
		remaining := make(map[string]bool, len(keys))
		for _, k := range keys {
			remaining[k] = true
		}
		if remaining[staleBase] || remaining[staleTmp] || remaining[initShaped] {
			t.Fatalf("gc left leftovers behind: base=%v tmp=%v init=%v",
				remaining[staleBase], remaining[staleTmp], remaining[initShaped])
		}
		if !remaining[deltaOrphan] {
			t.Fatal("gc deleted a delta-shaped orphan; delta orphans carry unique data and need --repair analysis")
		}
		for _, f := range mBefore.Files {
			if key, ok := normalizedManifestKey(env, f.Path); ok && !remaining[key] {
				t.Fatalf("gc deleted manifest-listed object %s", key)
			}
		}

		mAfter, _ := loadManifestWithETag(t, ctx, env, schema)
		if mAfter.Version != mBefore.Version {
			t.Fatalf("gc bumped manifest version %d -> %d", mBefore.Version, mAfter.Version)
		}
	})
}

// normalizedManifestKey reduces a manifest path to a bucket-relative key
// for listing comparison (mirrors the tool's normalization).
func normalizedManifestKey(env *Env, path string) (string, bool) {
	prefix := "s3://" + env.Cluster.Bucket + "/"
	if len(path) > len(prefix) && path[:len(prefix)] == prefix {
		return path[len(prefix):], true
	}
	if len(path) > 5 && path[:5] == "s3://" {
		return "", false
	}
	return path, true
}
