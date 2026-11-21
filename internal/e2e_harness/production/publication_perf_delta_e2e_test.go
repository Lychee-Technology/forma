//go:build e2e

package production

import (
	"context"
	"os"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/cdc"
)

type s3OpCounts struct {
	get  atomic.Int64
	put  atomic.Int64
	head atomic.Int64
	copy atomic.Int64
	del  atomic.Int64
}

func (c *s3OpCounts) snapshot() (get, put, head, copyOp, del int64) {
	return c.get.Load(), c.put.Load(), c.head.Load(), c.copy.Load(), c.del.Load()
}

func (c *s3OpCounts) reset() {
	c.get.Store(0)
	c.put.Store(0)
	c.head.Store(0)
	c.copy.Store(0)
	c.del.Store(0)
}

type countingPubS3 struct {
	inner  PublicationS3Client
	counts *s3OpCounts
}

func (c *countingPubS3) GetObject(ctx context.Context, in *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	c.counts.get.Add(1)
	return c.inner.GetObject(ctx, in, optFns...)
}

func (c *countingPubS3) PutObject(ctx context.Context, in *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	c.counts.put.Add(1)
	return c.inner.PutObject(ctx, in, optFns...)
}

func (c *countingPubS3) HeadObject(ctx context.Context, in *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	c.counts.head.Add(1)
	return c.inner.HeadObject(ctx, in, optFns...)
}

type countingFullS3 struct {
	inner  cdc.S3FullClient
	counts *s3OpCounts
}

func (c *countingFullS3) GetObject(ctx context.Context, in *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	c.counts.get.Add(1)
	return c.inner.GetObject(ctx, in, optFns...)
}

func (c *countingFullS3) PutObject(ctx context.Context, in *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	c.counts.put.Add(1)
	return c.inner.PutObject(ctx, in, optFns...)
}

func (c *countingFullS3) HeadObject(ctx context.Context, in *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	c.counts.head.Add(1)
	return c.inner.HeadObject(ctx, in, optFns...)
}

func (c *countingFullS3) CopyObject(ctx context.Context, in *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	c.counts.copy.Add(1)
	return c.inner.CopyObject(ctx, in, optFns...)
}

func (c *countingFullS3) DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	c.counts.del.Add(1)
	return c.inner.DeleteObject(ctx, in, optFns...)
}

// TestPublicationPerfDelta measures P50/P95/P99 latency and S3/DuckDB
// operation counts for Protocol V2 normally-unpublished reads, normally-
// published reads (warm and cold manifest cache), and publishing workloads
// (cdc-flush, cdc-init, compactor, manifest-reconcile). Opt-in via
// FORMA_PUBLICATION_PERF_DELTA=1 so standard CI E2E runtime stays bounded.
func TestPublicationPerfDelta(t *testing.T) {
	if os.Getenv("FORMA_PUBLICATION_PERF_DELTA") != "1" {
		t.Skip("set FORMA_PUBLICATION_PERF_DELTA=1 to run Protocol V2 P95/cost delta measurements")
	}

	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t), WithDuckThreads(4))
	wide := DefaultSchemaFixtures()[1]
	readCounts := &s3OpCounts{}
	env.PublicationS3Wrap = func(inner PublicationS3Client) PublicationS3Client {
		return &countingPubS3{inner: inner, counts: readCounts}
	}

	seedMatrixRows(ctx, t, env, wide, "perf", 250, 1000)

	const readSamples = 25
	// 1. PG-only baseline via PostgresEntityManager (no DuckDB, no publication read).
	pgMgr := env.PostgresEntityManager()
	pgLat := measureDurations(t, 3, readSamples, func() error {
		_, err := pgMgr.Query(ctx, &forma.QueryRequest{
			SchemaName:   wide.Name,
			Condition:    &forma.KvCondition{Attr: "count", Value: "gte:1050"},
			SortBy:       []string{"count"},
			ItemsPerPage: 20,
			Page:         1,
		})
		return err
	})
	logReadStats(t, "pg_only_baseline", pgLat, 0, readCounts)

	// 2. Normally UNPUBLISHED reads through the federated engine.
	q := Query{
		Schema:  wide,
		Filters: []Filter{{Attr: "count", Op: "gte", Value: "1050"}},
		Sorts:   []Sort{{Attr: "count"}},
		Limit:   20,
	}
	readCounts.reset()
	unpubDuck := 0
	unpubLat := measureDurations(t, 3, readSamples, func() error {
		res, err := env.Query(ctx, q)
		if err != nil {
			return err
		}
		if planHasEngine(res.Plan, "duckdb") {
			unpubDuck++
		}
		return nil
	})
	logReadStats(t, "v2_unpublished_federated", unpubLat, unpubDuck, readCounts)

	// 3. Publishing workloads: cdc-init (250 rows), cdc-flush (50 rows), compactor, reconcile.
	measurePublishingWorkloads(ctx, t, env, wide)

	// Add 10 hot rows so PUBLISHED reads exercise all three tiers (base + delta + hot).
	seedMatrixRows(ctx, t, env, wide, "perf-hot", 10, 2000)

	// 4. Normally PUBLISHED reads (warm manifest cache).
	if _, err := env.Query(ctx, q); err != nil {
		t.Fatalf("warmup published query: %v", err)
	}
	readCounts.reset()
	warmDuck := 0
	warmLat := measureDurations(t, 2, readSamples, func() error {
		res, err := env.Query(ctx, q)
		if err != nil {
			return err
		}
		if planHasEngine(res.Plan, "duckdb") {
			warmDuck++
		}
		return nil
	})
	logReadStats(t, "v2_published_warm_cache", warmLat, warmDuck, readCounts)

	// 5. Normally PUBLISHED reads (cold manifest cache miss: fresh engine per sample).
	readCounts.reset()
	coldDuck := 0
	coldLat := make([]time.Duration, 0, 10)
	for i := 0; i < 10; i++ {
		if err := env.ReopenDuckDB(); err != nil {
			t.Fatalf("reopen duckdb sample %d: %v", i, err)
		}
		start := time.Now()
		res, err := env.Query(ctx, q)
		if err != nil {
			t.Fatalf("cold sample %d: %v", i, err)
		}
		coldLat = append(coldLat, time.Since(start))
		if planHasEngine(res.Plan, "duckdb") {
			coldDuck++
		}
	}
	slices.Sort(coldLat)
	logReadStats(t, "v2_published_cold_cache", coldLat, coldDuck, readCounts)
}

func measurePublishingWorkloads(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	t.Helper()
	writeCounts := &s3OpCounts{}
	s3Wrap := &countingFullS3{inner: env.Cluster.S3, counts: writeCounts}

	// cdc-init (250 live rows -> base parquet + manifest + handoff).
	writeCounts.reset()
	t0 := time.Now()
	if _, err := env.RunInitWith(ctx, wide, InitOverrides{S3: s3Wrap}); err != nil {
		t.Fatalf("perf cdc-init: %v", err)
	}
	initDur := time.Since(t0)
	logWriteStats(t, "v2_cdc_init_250rows", initDur, writeCounts)

	// cdc-flush (50 dirty rows -> delta parquet + manifest + atomic marks).
	seedMatrixRows(ctx, t, env, wide, "perf-delta", 50, 1500)
	writeCounts.reset()
	t0 = time.Now()
	if _, err := env.RunFlushWith(ctx, FlushOverrides{S3: s3Wrap}); err != nil {
		t.Fatalf("perf cdc-flush: %v", err)
	}
	flushDur := time.Since(t0)
	logWriteStats(t, "v2_cdc_flush_50rows", flushDur, writeCounts)

	// compactor (merge base + delta with pre-merge checksum verification ON).
	writeCounts.reset()
	t0 = time.Now()
	res, err := env.RunCompactionWith(ctx, wide, CompactionOverrides{S3: s3Wrap, TargetBaseSizeBytes: 1})
	if err != nil {
		t.Fatalf("perf compactor: %v", err)
	}
	compDur := time.Since(t0)
	if res.Generation == 0 {
		t.Fatalf("perf compactor did not commit: %+v", res)
	}
	logWriteStats(t, "v2_compactor_300rows", compDur, writeCounts)

	// manifest-reconcile diagnosis (read-only) and --gc pass.
	t0 = time.Now()
	if _, err := env.reconciler(t, wide, reconcileOptions(false)).Run(ctx); err != nil {
		t.Fatalf("perf manifest-reconcile diagnose: %v", err)
	}
	t.Logf("PERF_WRITE name=v2_reconcile_diagnose dur_ms=%.2f", durMs(time.Since(t0)))

	if _, err := env.reconciler(t, wide, reconcileOptions(true)).Run(ctx); err != nil {
		t.Fatalf("perf manifest-reconcile retire: %v", err)
	}
	time.Sleep(reconcileGrace)
	tGC := time.Now()
	if _, err := env.reconciler(t, wide, reconcileOptions(true)).Run(ctx); err != nil {
		t.Fatalf("perf manifest-reconcile gc: %v", err)
	}
	t.Logf("PERF_WRITE name=v2_reconcile_gc_collect dur_ms=%.2f", durMs(time.Since(tGC)))
}

func measureDurations(t *testing.T, warmup, samples int, fn func() error) []time.Duration {
	t.Helper()
	for i := 0; i < warmup; i++ {
		if err := fn(); err != nil {
			t.Fatalf("warmup %d: %v", i, err)
		}
	}
	out := make([]time.Duration, 0, samples)
	for i := 0; i < samples; i++ {
		start := time.Now()
		if err := fn(); err != nil {
			t.Fatalf("sample %d: %v", i, err)
		}
		out = append(out, time.Since(start))
	}
	slices.Sort(out)
	return out
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func durMs(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func logReadStats(t *testing.T, name string, sorted []time.Duration, duckdbExecs int, counts *s3OpCounts) {
	t.Helper()
	g, p, h, cp, dl := counts.snapshot()
	t.Logf("PERF_READ name=%s n=%d p50_ms=%.2f p95_ms=%.2f p99_ms=%.2f duckdb_execs=%d s3_get=%d s3_head=%d s3_put=%d s3_copy=%d s3_del=%d",
		name, len(sorted), durMs(percentile(sorted, 0.50)), durMs(percentile(sorted, 0.95)), durMs(percentile(sorted, 0.99)),
		duckdbExecs, g, h, p, cp, dl)
}

func logWriteStats(t *testing.T, name string, dur time.Duration, counts *s3OpCounts) {
	t.Helper()
	g, p, h, cp, dl := counts.snapshot()
	t.Logf("PERF_WRITE name=%s dur_ms=%.2f s3_put=%d s3_get=%d s3_head=%d s3_copy=%d s3_del=%d",
		name, durMs(dur), p, g, h, cp, dl)
}
