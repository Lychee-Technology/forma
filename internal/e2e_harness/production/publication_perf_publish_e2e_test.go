//go:build e2e

package production

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/compaction"
	"github.com/lychee-technology/forma/internal/reconcile"
)

const (
	writeWarmup        = 1
	writeSamples       = 20
	perfReconcileGrace = 400 * time.Millisecond
)

// perfReconcileOptions matches reconcile.Options validation (RetentionGrace >=
// ReaderDeadline + ClockMargin) while keeping repeated GC collection samples
// bounded in the opt-in perf harness.
func perfReconcileOptions(gc bool) reconcile.Options {
	return reconcile.Options{
		GC:             gc,
		RetentionGrace: 300 * time.Millisecond,
		ReaderDeadline: 200 * time.Millisecond,
		ClockMargin:    50 * time.Millisecond,
		OrphanAge:      100 * time.Millisecond,
	}
}

func seedPerfRows(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, prefix string, n, baseCount int) {
	t.Helper()
	ops := make([]forma.EntityOperation, 0, n)
	for i := 0; i < n; i++ {
		ops = append(ops, forma.EntityOperation{
			EntityIdentifier: forma.EntityIdentifier{SchemaName: schema.Name},
			Type:             forma.OperationCreate,
			Data: map[string]any{
				"title": fmt.Sprintf("%s-%03d", prefix, i),
				"count": float64(baseCount + i),
			},
		})
	}
	res, err := env.PostgresEntityManager().BatchCreate(ctx, &forma.BatchOperation{
		Operations: ops,
		Atomic:     true,
	})
	if err != nil {
		t.Fatalf("seedPerfRows %s (%d rows): %v", prefix, n, err)
	}
	if len(res.Failed) != 0 || len(res.Successful) != n {
		t.Fatalf("seedPerfRows %s: successful=%d failed=%d want=%d", prefix, len(res.Successful), len(res.Failed), n)
	}
}

type writeSeries struct {
	name      string
	durations []time.Duration
	counts    s3CountSnapshot
}

func newWriteSeries(name string) *writeSeries {
	return &writeSeries{
		name:      name,
		durations: make([]time.Duration, 0, writeSamples),
	}
}

func (s *writeSeries) record(t *testing.T, dur time.Duration, snap s3CountSnapshot, isWarmup bool) {
	t.Helper()
	assertCountAccounting(t, s.name, snap)
	if isWarmup {
		return
	}
	s.durations = append(s.durations, dur)
	s.counts.total += snap.total
	s.counts.getTotal += snap.getTotal
	s.counts.getManifest += snap.getManifest
	s.counts.getParquet += snap.getParquet
	s.counts.head += snap.head
	s.counts.list += snap.list
	s.counts.put += snap.put
	s.counts.copyOp += snap.copyOp
	s.counts.del += snap.del
	s.counts.other += snap.other
}

func (s *writeSeries) log(t *testing.T) {
	t.Helper()
	if len(s.durations) != writeSamples {
		t.Fatalf("%s: collected %d samples, want %d", s.name, len(s.durations), writeSamples)
	}
	sorted := append([]time.Duration(nil), s.durations...)
	slices.Sort(sorted)
	c := s.counts
	assertCountAccounting(t, s.name, c)
	t.Logf("PERF_WRITE name=%s n=%d p50_ms=%.2f p95_ms=%.2f p99_ms=%.2f s3_total=%d s3_put=%d s3_get=%d (manifest=%d parquet=%d) s3_head=%d s3_list=%d s3_copy=%d s3_del=%d s3_other=%d",
		s.name, len(sorted), durMs(percentile(sorted, 0.50)), durMs(percentile(sorted, 0.95)), durMs(percentile(sorted, 0.99)),
		c.total, c.put, c.getTotal, c.getManifest, c.getParquet, c.head, c.list, c.copyOp, c.del, c.other)
}

type publishBenchmarkSeries struct {
	initDryRun         *writeSeries
	initReal           *writeSeries
	flushDryRun        *writeSeries
	flushUnpublished   *writeSeries
	flushPublished     *writeSeries
	compactSkipVerify  *writeSeries
	compactVerify      *writeSeries
	reconcileDiagnose  *writeSeries
	reconcileGCRetire  *writeSeries
	reconcileGCCollect *writeSeries
}

func newPublishBenchmarkSeries() *publishBenchmarkSeries {
	return &publishBenchmarkSeries{
		initDryRun:         newWriteSeries("v2_cdc_init_250rows_dryrun"),
		initReal:           newWriteSeries("v2_cdc_init_250rows"),
		flushDryRun:        newWriteSeries("v2_cdc_flush_50rows_dryrun"),
		flushUnpublished:   newWriteSeries("v2_cdc_flush_unpublished_50rows"),
		flushPublished:     newWriteSeries("v2_cdc_flush_50rows"),
		compactSkipVerify:  newWriteSeries("v2_compactor_300rows_skip_verify"),
		compactVerify:      newWriteSeries("v2_compactor_300rows"),
		reconcileDiagnose:  newWriteSeries("v2_reconcile_diagnose"),
		reconcileGCRetire:  newWriteSeries("v2_reconcile_gc_retire"),
		reconcileGCCollect: newWriteSeries("v2_reconcile_gc_collect"),
	}
}

func (p *publishBenchmarkSeries) logAll(t *testing.T) {
	t.Helper()
	wantVerifyDelta := int64(2 * writeSamples)
	if got := p.compactVerify.counts.getParquet - p.compactSkipVerify.counts.getParquet; got != wantVerifyDelta {
		t.Fatalf("compactor checksum verification parquet GET delta = %d, want %d", got, wantVerifyDelta)
	}
	for _, s := range []*writeSeries{
		p.initDryRun, p.initReal,
		p.flushDryRun, p.flushUnpublished, p.flushPublished,
		p.compactSkipVerify, p.compactVerify,
		p.reconcileDiagnose, p.reconcileGCRetire, p.reconcileGCCollect,
	} {
		s.log(t)
	}
}

// measurePublishingWorkloads runs writeWarmup discarded iterations followed by
// writeSamples independently prepared iterations across isolated Envs,
// capturing P50/P95/P99 latency and complete endpoint-level S3 HTTP counts for
// every V2 publishing/maintenance workload and its in-tree comparison baseline.
func measurePublishingWorkloads(ctx context.Context, t *testing.T, cluster *Cluster, wide SchemaRef, counts *s3EndpointCounts) {
	t.Helper()
	series := newPublishBenchmarkSeries()
	totalRuns := writeWarmup + writeSamples
	for i := 0; i < totalRuns; i++ {
		isWarmup := i < writeWarmup
		measurePrimaryPublishLifecycle(ctx, t, cluster, wide, counts, series, isWarmup)
		measurePairedPublishBaselines(ctx, t, cluster, wide, counts, series, isWarmup)
	}
	series.logAll(t)
}

func measurePrimaryPublishLifecycle(
	ctx context.Context, t *testing.T, cluster *Cluster, wide SchemaRef,
	counts *s3EndpointCounts, series *publishBenchmarkSeries, isWarmup bool,
) {
	t.Helper()
	env := NewEnv(t, cluster, WithDuckThreads(4))
	defer env.teardown(ctx)

	seedPerfRows(ctx, t, env, wide, "perf-init", 250, 1000)

	// cdc-init dry-run baseline (PG+DuckDB setup + lock + row count/sizing).
	runTimedInit(ctx, t, env, wide, true, counts, series.initDryRun, isWarmup)
	// cdc-init real publish (250 rows -> base parquet + manifest + handoff).
	runTimedInit(ctx, t, env, wide, false, counts, series.initReal, isWarmup)

	// Seed 50 dirty rows on the PUBLISHED schema for steady-state cdc-flush.
	seedPerfRows(ctx, t, env, wide, "perf-delta", 50, 1500)
	runTimedFlush(ctx, t, env, true, 0, counts, series.flushDryRun, isWarmup)
	runTimedFlush(ctx, t, env, false, 1, counts, series.flushPublished, isWarmup)

	// Compactor rewrite (250 base + 50 delta -> 1 merged 300-row base) with
	// pre-merge SHA-256 verification ON (default Protocol V2 behavior).
	runTimedCompaction(ctx, t, env, wide, false, counts, series.compactVerify, isWarmup)

	// manifest-reconcile: lock-free diagnose, --gc retire, and --gc collect.
	measureReconcilePhases(ctx, t, env, wide, counts, series, isWarmup)
}

func measurePairedPublishBaselines(
	ctx context.Context, t *testing.T, cluster *Cluster, wide SchemaRef,
	counts *s3EndpointCounts, series *publishBenchmarkSeries, isWarmup bool,
) {
	t.Helper()
	env := NewEnv(t, cluster, WithDuckThreads(4))
	defer env.teardown(ctx)

	// First flush from UNPUBLISHED (50 dirty rows, 0 parent manifest GETs).
	seedPerfRows(ctx, t, env, wide, "perf-unpub", 50, 1000)
	runTimedFlush(ctx, t, env, false, 0, counts, series.flushUnpublished, isWarmup)

	// Bring schema to the identical 250-base + 50-delta (300-row) pre-compaction
	// state, then time compactor with --skip-input-checksum-verify.
	seedPerfRows(ctx, t, env, wide, "perf-pad", 200, 1050)
	if _, err := cdc.RunInit(ctx, perfInitOptions(env, wide, false)); err != nil {
		t.Fatalf("prep paired compaction cdc-init: %v", err)
	}
	seedPerfRows(ctx, t, env, wide, "perf-delta2", 50, 1500)
	prepRunner := cdc.NewRunner(env.logger)
	defer func() { _ = prepRunner.Close() }()
	if _, err := prepRunner.RunOnce(ctx, env.CDC, env.Cluster.S3, false, env.Registry); err != nil {
		t.Fatalf("prep paired compaction cdc-flush: %v", err)
	}
	runTimedCompaction(ctx, t, env, wide, true, counts, series.compactSkipVerify, isWarmup)
}

func perfInitOptions(env *Env, wide SchemaRef, dryRun bool) cdc.InitOptions {
	return cdc.InitOptions{
		Config:               env.CDC,
		S3Client:             env.Cluster.S3,
		SchemaRegistryTable:  env.Tables.SchemaRegistry,
		SchemaIDFilter:       int(wide.ID),
		DryRun:               dryRun,
		AutoEstimateRowBytes: true,
		Logger:               env.logger,
		SchemaRegistry:       env.Registry,
	}
}

func runTimedInit(
	ctx context.Context, t *testing.T, env *Env, wide SchemaRef, dryRun bool,
	counts *s3EndpointCounts, series *writeSeries, isWarmup bool,
) {
	t.Helper()
	counts.reset()
	t0 := time.Now()
	if _, err := cdc.RunInit(ctx, perfInitOptions(env, wide, dryRun)); err != nil {
		t.Fatalf("%s: %v", series.name, err)
	}
	dur := time.Since(t0)
	snap := counts.snapshot()
	if dryRun && snap.total != 0 {
		t.Fatalf("%s: dry-run cdc-init must issue 0 S3 HTTP requests, got %+v", series.name, snap)
	}
	if !dryRun && (snap.put != 2 || snap.getTotal != 0 || snap.list != 0 || snap.copyOp != 0 || snap.del != 0) {
		t.Fatalf("%s: cdc-init must issue 2 PUTs and 0 GET/LIST/COPY/DELETE at S3 endpoint, got %+v", series.name, snap)
	}
	series.record(t, dur, snap, isWarmup)
}

// runTimedFlush uses a fresh cdc.Runner per call so dry-run, unpublished, and
// published flush measurements all include identical runner initialization cost
// and match cdc-init / compactor's per-invocation setup.
func runTimedFlush(
	ctx context.Context, t *testing.T, env *Env, dryRun bool, wantManifestGets int64,
	counts *s3EndpointCounts, series *writeSeries, isWarmup bool,
) {
	t.Helper()
	runner := cdc.NewRunner(env.logger)
	defer func() { _ = runner.Close() }()
	counts.reset()
	t0 := time.Now()
	if _, err := runner.RunOnce(ctx, env.CDC, env.Cluster.S3, dryRun, env.Registry); err != nil {
		t.Fatalf("%s: %v", series.name, err)
	}
	dur := time.Since(t0)
	snap := counts.snapshot()
	if dryRun && snap.total != 0 {
		t.Fatalf("%s: dry-run cdc-flush must issue 0 S3 HTTP requests, got %+v", series.name, snap)
	}
	if !dryRun && (snap.put != 2 || snap.getManifest != wantManifestGets || snap.list != 0 || snap.copyOp != 0 || snap.del != 0) {
		t.Fatalf("%s: cdc-flush want 2 PUTs, %d manifest GETs, and 0 LIST/COPY/DELETE, got %+v", series.name, wantManifestGets, snap)
	}
	series.record(t, dur, snap, isWarmup)
}

func runTimedCompaction(
	ctx context.Context, t *testing.T, env *Env, wide SchemaRef, skipVerify bool,
	counts *s3EndpointCounts, series *writeSeries, isWarmup bool,
) {
	t.Helper()
	counts.reset()
	t0 := time.Now()
	res, err := env.RunCompactionWith(ctx, wide, CompactionOverrides{
		DirtyRatioPct:           5,
		SkipInputChecksumVerify: skipVerify,
	})
	if err != nil {
		t.Fatalf("%s: %v", series.name, err)
	}
	dur := time.Since(t0)
	if res.Outcome != compaction.RewriteApplied || res.FilesMerged != 2 || res.RowsIn != 300 || res.RowsOut != 300 {
		t.Fatalf("%s: expected 300-row 2-file rewrite, got %+v", series.name, res)
	}
	snap := counts.snapshot()
	if snap.put != 2 || snap.getManifest != 1 || snap.list != 0 || snap.copyOp != 0 || snap.del != 0 {
		t.Fatalf("%s: compactor must issue 2 PUTs, 1 manifest GET, and 0 LIST/COPY/DELETE, got %+v", series.name, snap)
	}
	series.record(t, dur, snap, isWarmup)
}

func measureReconcilePhases(
	ctx context.Context, t *testing.T, env *Env, wide SchemaRef,
	counts *s3EndpointCounts, series *publishBenchmarkSeries, isWarmup bool,
) {
	t.Helper()
	rec := env.reconciler(t, wide, perfReconcileOptions(false))
	defer func() { _ = rec.DB.Close() }()

	// Phase 1: lock-free, read-only diagnosis.
	counts.reset()
	t0 := time.Now()
	if _, err := rec.Run(ctx); err != nil {
		t.Fatalf("%s: %v", series.reconcileDiagnose.name, err)
	}
	diagDur := time.Since(t0)
	diagSnap := counts.snapshot()
	if diagSnap.list != 1 || diagSnap.put != 0 || diagSnap.copyOp != 0 || diagSnap.del != 0 {
		t.Fatalf("reconcile diagnose must issue 1 LIST and 0 PUT/COPY/DELETE at S3 endpoint, got %+v", diagSnap)
	}
	series.reconcileDiagnose.record(t, diagDur, diagSnap, isWarmup)

	// Phase 2: --gc retire pass (raises horizon, retires superseded generations).
	rec.Opts = perfReconcileOptions(true)
	counts.reset()
	t0 = time.Now()
	if _, err := rec.Run(ctx); err != nil {
		t.Fatalf("%s: %v", series.reconcileGCRetire.name, err)
	}
	retireDur := time.Since(t0)
	retireSnap := counts.snapshot()
	if retireSnap.list != 1 || retireSnap.put != 0 || retireSnap.copyOp != 0 || retireSnap.del != 0 {
		t.Fatalf("reconcile --gc retire must issue 1 LIST and 0 PUT/COPY/DELETE at S3 endpoint, got %+v", retireSnap)
	}
	series.reconcileGCRetire.record(t, retireDur, retireSnap, isWarmup)

	// Phase 3: --gc collect pass after retention grace G expires.
	time.Sleep(perfReconcileGrace)
	counts.reset()
	t0 = time.Now()
	if _, err := rec.Run(ctx); err != nil {
		t.Fatalf("%s: %v", series.reconcileGCCollect.name, err)
	}
	gcDur := time.Since(t0)
	gcSnap := counts.snapshot()
	if gcSnap.list != 1 || gcSnap.del == 0 {
		t.Fatalf("reconcile --gc collect must observe 1 LIST and DELETEs at S3 endpoint, got %+v", gcSnap)
	}
	series.reconcileGCCollect.record(t, gcDur, gcSnap, isWarmup)
}
