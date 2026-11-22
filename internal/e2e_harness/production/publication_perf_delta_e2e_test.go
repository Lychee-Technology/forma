//go:build e2e

package production

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/compaction"
	"github.com/lychee-technology/forma/internal/e2e_harness"
)

// s3EndpointCounts records every HTTP request observed at the S3 endpoint
// reverse proxy, covering Go SDK clients (publication reader, CDC, compactor,
// manifest-reconcile) and DuckDB httpfs alike.
type s3EndpointCounts struct {
	total       atomic.Int64
	getTotal    atomic.Int64
	getManifest atomic.Int64
	getParquet  atomic.Int64
	headTotal   atomic.Int64
	listTotal   atomic.Int64
	putTotal    atomic.Int64
	copyTotal   atomic.Int64
	delTotal    atomic.Int64
	otherTotal  atomic.Int64
}

type s3CountSnapshot struct {
	total, getTotal, getManifest, getParquet int64
	head, list, put, copyOp, del, other      int64
}

func (c *s3EndpointCounts) snapshot() s3CountSnapshot {
	return s3CountSnapshot{
		total:       c.total.Load(),
		getTotal:    c.getTotal.Load(),
		getManifest: c.getManifest.Load(),
		getParquet:  c.getParquet.Load(),
		head:        c.headTotal.Load(),
		list:        c.listTotal.Load(),
		put:         c.putTotal.Load(),
		copyOp:      c.copyTotal.Load(),
		del:         c.delTotal.Load(),
		other:       c.otherTotal.Load(),
	}
}

func (c *s3EndpointCounts) reset() {
	c.total.Store(0)
	c.getTotal.Store(0)
	c.getManifest.Store(0)
	c.getParquet.Store(0)
	c.headTotal.Store(0)
	c.listTotal.Store(0)
	c.putTotal.Store(0)
	c.copyTotal.Store(0)
	c.delTotal.Store(0)
	c.otherTotal.Store(0)
}

func (c *s3EndpointCounts) record(r *http.Request, bucket string) {
	c.total.Add(1)
	switch r.Method {
	case http.MethodHead:
		c.headTotal.Add(1)
	case http.MethodGet:
		if isS3ListRequest(r, bucket) {
			c.listTotal.Add(1)
			return
		}
		c.getTotal.Add(1)
		switch {
		case strings.Contains(r.URL.Path, "/_manifests/"):
			c.getManifest.Add(1)
		case strings.HasSuffix(r.URL.Path, ".parquet"):
			c.getParquet.Add(1)
		}
	case http.MethodPut:
		if r.Header.Get("X-Amz-Copy-Source") != "" {
			c.copyTotal.Add(1)
			return
		}
		c.putTotal.Add(1)
	case http.MethodDelete:
		c.delTotal.Add(1)
	case http.MethodPost:
		if r.URL.Query().Has("delete") {
			c.delTotal.Add(1)
			return
		}
		c.otherTotal.Add(1)
	default:
		c.otherTotal.Add(1)
	}
}

func isS3ListRequest(r *http.Request, bucket string) bool {
	q := r.URL.Query()
	if q.Has("list-type") || q.Has("prefix") || q.Has("delimiter") {
		return true
	}
	trimmed := strings.Trim(r.URL.Path, "/")
	return trimmed == "" || trimmed == bucket
}

// attachCountingS3Proxy interposes a local HTTP reverse proxy in front of a
// dedicated cluster's S3 endpoint so every S3 request — Go SDK manifest calls,
// DuckDB httpfs parquet reads/writes, CDC, compactor, and manifest-reconcile —
// is counted at the object-store boundary. NewSingleHostReverseProxy preserves
// the incoming Host header so AWS SigV4 verification succeeds unchanged.
func attachCountingS3Proxy(ctx context.Context, t *testing.T, cluster *Cluster) *s3EndpointCounts {
	t.Helper()
	target, err := url.Parse(cluster.S3Endpoint)
	if err != nil {
		t.Fatalf("parse cluster S3 endpoint %q: %v", cluster.S3Endpoint, err)
	}
	counts := &s3EndpointCounts{}
	rp := httputil.NewSingleHostReverseProxy(target)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		counts.record(r, cluster.Bucket)
		rp.ServeHTTP(w, r)
	}))

	origEndpoint := cluster.S3Endpoint
	origS3 := cluster.S3
	proxiedClient, err := e2e_harness.NewS3Client(ctx, srv.URL, cluster.S3Region, cluster.S3AccessKey, cluster.S3SecretKey)
	if err != nil {
		srv.Close()
		t.Fatalf("create proxied S3 client: %v", err)
	}
	cluster.S3Endpoint = srv.URL
	cluster.S3 = proxiedClient
	t.Cleanup(func() {
		cluster.S3 = origS3
		cluster.S3Endpoint = origEndpoint
		srv.Close()
	})
	return counts
}

// TestPublicationPerfDelta measures P50/P95/P99 latency and full endpoint-level
// S3 HTTP operation counts (covering Go SDK clients, DuckDB httpfs, and
// manifest-reconcile) for Protocol V2 unpublished reads, published reads (warm
// and cold manifest cache) against equivalent PG-only baselines, and publishing
// workloads (cdc-init, cdc-flush, compactor, manifest-reconcile). Opt-in via
// FORMA_PUBLICATION_PERF_DELTA=1 so standard CI E2E runtime stays bounded.
func TestPublicationPerfDelta(t *testing.T) {
	if os.Getenv("FORMA_PUBLICATION_PERF_DELTA") != "1" {
		t.Skip("set FORMA_PUBLICATION_PERF_DELTA=1 to run Protocol V2 P95/cost delta measurements")
	}

	ctx := context.Background()
	cluster := DedicatedCluster(t)
	counts := attachCountingS3Proxy(ctx, t, cluster)
	env := NewEnv(t, cluster, WithDuckThreads(4))
	wide := DefaultSchemaFixtures()[1]

	seedPerfRows(ctx, t, env, wide, "perf", 250, 1000)

	const readSamples = 25
	q := Query{
		Schema:  wide,
		Filters: []Filter{{Attr: "count", Op: "gte", Value: "1050"}},
		Sorts:   []Sort{{Attr: "count"}},
		Limit:   20,
	}

	// 1. Unpublished dataset (250 rows): PG-only baseline vs V2 UNPUBLISHED.
	measurePGOnlyRead(ctx, t, env, wide, "pg_only_baseline_unpublished", readSamples, counts)
	unpubSnap := measureFederatedRead(ctx, t, env, q, "v2_unpublished_federated", 3, readSamples, counts, false)
	if unpubSnap.total != 0 {
		t.Fatalf("UNPUBLISHED reads must issue 0 S3 HTTP requests, got %+v", unpubSnap)
	}

	// 2. Prepare the 3-tier published dataset (300 compacted base rows in 1 base
	// object + 20 flushed delta rows in 1 delta object + 10 hot rows = 330 rows).
	preparePublishedThreeTierDataset(ctx, t, env, wide)

	// 3. Published 3-tier dataset (330 rows): PG-only baseline vs V2 warm & cold cache.
	measurePGOnlyRead(ctx, t, env, wide, "pg_only_baseline_published", readSamples, counts)
	warmSnap := measureFederatedRead(ctx, t, env, q, "v2_published_warm_cache", 3, readSamples, counts, true)
	if warmSnap.getManifest != 0 || warmSnap.list != 0 {
		t.Fatalf("warm-cache published reads must issue 0 manifest GETs and 0 LISTs, got %+v", warmSnap)
	}

	coldSnap := measureColdCacheRead(ctx, t, env, q, 10, counts)
	if coldSnap.getManifest != 10 || coldSnap.list != 0 {
		t.Fatalf("cold-cache published reads must issue 10 manifest GETs and 0 LISTs, got %+v", coldSnap)
	}

	// 4. Publishing & maintenance workloads: repeated independently prepared
	// runs (1 warmup + 20 samples) with in-tree comparable baselines.
	measurePublishingWorkloads(ctx, t, cluster, wide, counts)
}

func preparePublishedThreeTierDataset(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	t.Helper()
	if _, err := cdc.RunInit(ctx, perfInitOptions(env, wide, false)); err != nil {
		t.Fatalf("prep published reads cdc-init: %v", err)
	}
	seedPerfRows(ctx, t, env, wide, "perf-delta", 50, 1500)
	mustFlush(ctx, t, env)
	res, err := env.RunCompactionWith(ctx, wide, CompactionOverrides{DirtyRatioPct: 5})
	if err != nil {
		t.Fatalf("prep published reads compactor: %v", err)
	}
	if res.Outcome != compaction.RewriteApplied || res.RowsOut != 300 {
		t.Fatalf("prep published reads expected 300-row rewrite, got %+v", res)
	}
	seedPerfRows(ctx, t, env, wide, "perf-delta2", 20, 1800)
	mustFlush(ctx, t, env)
	seedPerfRows(ctx, t, env, wide, "perf-hot", 10, 2000)
}

func measurePGOnlyRead(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, name string, samples int, counts *s3EndpointCounts) {
	t.Helper()
	pgMgr := env.PostgresEntityManager()
	lat := measureDurations(t, 3, samples, counts.reset, func() error {
		_, err := pgMgr.Query(ctx, &forma.QueryRequest{
			SchemaName:   wide.Name,
			Condition:    &forma.KvCondition{Attr: "count", Value: "gte:1050"},
			SortBy:       []string{"count"},
			ItemsPerPage: 20,
			Page:         1,
		})
		return err
	})
	logReadStats(t, name, lat, 0, counts.snapshot())
}

func measureFederatedRead(ctx context.Context, t *testing.T, env *Env, q Query, name string, warmup, samples int, counts *s3EndpointCounts, expectDuckDB bool) s3CountSnapshot {
	t.Helper()
	duckExecs := 0
	lat := measureDurations(t, warmup, samples, func() {
		counts.reset()
		duckExecs = 0
	}, func() error {
		res, err := env.Query(ctx, q)
		if err != nil {
			return err
		}
		if planHasEngine(res.Plan, "duckdb") {
			duckExecs++
		}
		return nil
	})
	if expectDuckDB && duckExecs != samples {
		t.Fatalf("%s: duckdb execs = %d, want %d", name, duckExecs, samples)
	}
	if !expectDuckDB && duckExecs != 0 {
		t.Fatalf("%s: duckdb execs = %d, want 0", name, duckExecs)
	}
	snap := counts.snapshot()
	logReadStats(t, name, lat, duckExecs, snap)
	return snap
}

func measureColdCacheRead(ctx context.Context, t *testing.T, env *Env, q Query, samples int, counts *s3EndpointCounts) s3CountSnapshot {
	t.Helper()
	counts.reset()
	coldDuck := 0
	coldLat := make([]time.Duration, 0, samples)
	for i := 0; i < samples; i++ {
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
	snap := counts.snapshot()
	logReadStats(t, "v2_published_cold_cache", coldLat, coldDuck, snap)
	return snap
}

func measureDurations(t *testing.T, warmup, samples int, afterWarmup func(), fn func() error) []time.Duration {
	t.Helper()
	for i := 0; i < warmup; i++ {
		if err := fn(); err != nil {
			t.Fatalf("warmup %d: %v", i, err)
		}
	}
	if afterWarmup != nil {
		afterWarmup()
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

// percentile computes the nearest-rank percentile over a non-empty sorted slice.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func durMs(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func assertCountAccounting(t *testing.T, name string, s s3CountSnapshot) {
	t.Helper()
	sum := s.getTotal + s.head + s.list + s.put + s.copyOp + s.del + s.other
	if sum != s.total {
		t.Fatalf("%s: S3 HTTP accounting mismatch: total=%d sum=%d (%+v)", name, s.total, sum, s)
	}
}

func logReadStats(t *testing.T, name string, sorted []time.Duration, duckdbExecs int, s s3CountSnapshot) {
	t.Helper()
	assertCountAccounting(t, name, s)
	t.Logf("PERF_READ name=%s n=%d p50_ms=%.2f p95_ms=%.2f p99_ms=%.2f duckdb_execs=%d s3_total=%d s3_get=%d (manifest=%d parquet=%d) s3_head=%d s3_list=%d s3_put=%d s3_copy=%d s3_del=%d s3_other=%d",
		name, len(sorted), durMs(percentile(sorted, 0.50)), durMs(percentile(sorted, 0.95)), durMs(percentile(sorted, 0.99)),
		duckdbExecs, s.total, s.getTotal, s.getManifest, s.getParquet, s.head, s.list, s.put, s.copyOp, s.del, s.other)
}
