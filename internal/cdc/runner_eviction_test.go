package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestRunnerS3RuntimeDistinctEndpointsCoexist pins the eviction boundary
// (#331): supersession is per group (region/endpoint/path-style). Two configs
// with different endpoints are different groups — the second must not evict
// the first, and the first must still be a cache hit afterwards.
func TestRunnerS3RuntimeDistinctEndpointsCoexist(t *testing.T) {
	loadCalls := 0
	stubLoadAWSConfig(t, func(context.Context, ...func(*config.LoadOptions) error) (aws.Config, error) {
		loadCalls++
		return aws.Config{Region: "us-east-1"}, nil
	})
	stubNewS3Client(t, func(aws.Config, string, bool) *s3.Client { return &s3.Client{} })

	runner := NewRunner(zap.NewNop())
	cfgA := CDCConfig{S3Region: "us-east-1", S3Endpoint: "http://minio-a:9000"}
	cfgB := CDCConfig{S3Region: "us-east-1", S3Endpoint: "http://minio-b:9000"}

	rtA, err := runner.getOrCreateS3Runtime(context.Background(), cfgA)
	require.NoError(t, err)
	rtB, err := runner.getOrCreateS3Runtime(context.Background(), cfgB)
	require.NoError(t, err)
	require.NotSame(t, rtA, rtB)
	require.Len(t, runner.s3Runtimes, 2)

	rtA2, err := runner.getOrCreateS3Runtime(context.Background(), cfgA)
	require.NoError(t, err)
	require.Same(t, rtA, rtA2)
	require.Equal(t, 2, loadCalls)
}

// stubDuckExporterWithDB routes newDuckExporterFn to real :memory: DuckDB
// handles and records every DB it opens, so tests can assert which handles
// are still alive after rotations. The returned accessor snapshots the list
// under a mutex: exporter builds happen outside the Runner's lock, so
// concurrent tests (the -race storm) hit this stub concurrently. Open errors
// are returned, never require'd — the stub runs on non-test goroutines.
// t.Cleanup twin of the inline stubs in runner_test.go; no t.Parallel
// (process-global seam).
func stubDuckExporterWithDB(t *testing.T) func() []*sql.DB {
	t.Helper()
	previous := newDuckExporterFn
	var mu sync.Mutex
	var opened []*sql.DB
	newDuckExporterFn = func(ctx context.Context, cfg CDCConfig, s3AccessKey, s3Secret, s3SessionToken string, logger *zap.Logger) (*DuckExporter, error) {
		db, err := sql.Open("duckdb", ":memory:")
		if err != nil {
			return nil, fmt.Errorf("open stub duckdb: %w", err)
		}
		mu.Lock()
		opened = append(opened, db)
		mu.Unlock()
		return &DuckExporter{DB: db, Logger: logger}, nil
	}
	t.Cleanup(func() { newDuckExporterFn = previous })
	return func() []*sql.DB {
		mu.Lock()
		defer mu.Unlock()
		return append([]*sql.DB(nil), opened...)
	}
}

func duckEvictionFixture() (CDCConfig, *cachedS3Runtime, *cachedS3Runtime) {
	cfg := CDCConfig{
		DuckDBPath:   ":memory:",
		DuckThreads:  4,
		DuckMemLimit: "4GB",
		S3Region:     "us-east-1",
		S3UseSSL:     true,
	}
	old := &cachedS3Runtime{region: "us-east-1", accessKeyID: "key", secretAccessKey: "secret", sessionToken: "token-1"}
	rotated := &cachedS3Runtime{region: "us-east-1", accessKeyID: "key", secretAccessKey: "secret", sessionToken: "token-2"}
	return cfg, old, rotated
}

// TestRunnerDuckExporterRotationClosesSuperseded is the #331 red anchor: after
// a credential rotation the superseded exporter's DuckDB handle must be
// closed and its slot replaced — not stranded open until process exit.
func TestRunnerDuckExporterRotationClosesSuperseded(t *testing.T) {
	stubDuckExporterWithDB(t)
	runner := NewRunner(zap.NewNop())
	cfg, old, rotated := duckEvictionFixture()

	exp1, release1, err := runner.getOrCreateDuckExporter(context.Background(), cfg, old)
	require.NoError(t, err)
	release1()

	exp2, release2, err := runner.getOrCreateDuckExporter(context.Background(), cfg, rotated)
	require.NoError(t, err)
	defer release2()

	require.NotSame(t, exp1, exp2)
	require.Error(t, exp1.DB.PingContext(context.Background()), "superseded idle exporter must be closed")
	require.NoError(t, exp2.DB.PingContext(context.Background()))
	require.Len(t, runner.duckExporters, 1)
}

// TestRunnerDuckExporterInFlightSupersedeDefersClose pins the refcount rule:
// an exporter superseded while a RunOnce frame still holds it stays open
// until that frame's release — then closes. The live replacement's own
// release must NOT close it (it is cached, not doomed).
func TestRunnerDuckExporterInFlightSupersedeDefersClose(t *testing.T) {
	stubDuckExporterWithDB(t)
	runner := NewRunner(zap.NewNop())
	cfg, old, rotated := duckEvictionFixture()
	ctx := context.Background()

	exp1, release1, err := runner.getOrCreateDuckExporter(ctx, cfg, old)
	require.NoError(t, err)
	// No release yet: exp1 is "in flight" when the rotation lands.
	exp2, release2, err := runner.getOrCreateDuckExporter(ctx, cfg, rotated)
	require.NoError(t, err)

	require.NoError(t, exp1.DB.PingContext(ctx), "in-flight exporter must survive supersession")
	require.Len(t, runner.duckExporters, 1)

	release1()
	require.Error(t, exp1.DB.PingContext(ctx), "last release of a doomed exporter must close it")
	require.NoError(t, exp2.DB.PingContext(ctx))

	release2()
	require.NoError(t, exp2.DB.PingContext(ctx), "release of the live cached exporter must not close it")
	require.Len(t, runner.duckExporters, 1)
}

// TestRunnerDuckExporterRotationLeavesSingleLiveHandle is the leak assertion
// stated by the issue: N rotations leave exactly one live DuckDB handle, not N.
func TestRunnerDuckExporterRotationLeavesSingleLiveHandle(t *testing.T) {
	opened := stubDuckExporterWithDB(t)
	runner := NewRunner(zap.NewNop())
	cfg, _, _ := duckEvictionFixture()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		runtime := &cachedS3Runtime{
			region:          "us-east-1",
			accessKeyID:     "key",
			secretAccessKey: "secret",
			sessionToken:    fmt.Sprintf("token-%d", i),
		}
		_, release, err := runner.getOrCreateDuckExporter(ctx, cfg, runtime)
		require.NoError(t, err)
		release()
	}

	require.Len(t, opened(), 5)
	live := 0
	for _, db := range opened() {
		if db.PingContext(ctx) == nil {
			live++
		}
	}
	require.Equal(t, 1, live, "every superseded handle must be closed")
	require.Len(t, runner.duckExporters, 1)
}

// TestRunnerDuckExporterDistinctGroupsCoexist pins the eviction boundary:
// different non-credential configs are different groups; neither supersedes
// the other and both stay open.
func TestRunnerDuckExporterDistinctGroupsCoexist(t *testing.T) {
	stubDuckExporterWithDB(t)
	runner := NewRunner(zap.NewNop())
	cfg, old, _ := duckEvictionFixture()
	cfgB := cfg
	cfgB.S3Endpoint = "http://minio-b:9000"
	ctx := context.Background()

	expA, releaseA, err := runner.getOrCreateDuckExporter(ctx, cfg, old)
	require.NoError(t, err)
	releaseA()
	expB, releaseB, err := runner.getOrCreateDuckExporter(ctx, cfgB, old)
	require.NoError(t, err)
	releaseB()

	require.NotSame(t, expA, expB)
	require.Len(t, runner.duckExporters, 2)
	require.NoError(t, expA.DB.PingContext(ctx))
	require.NoError(t, expB.DB.PingContext(ctx))
}

// TestRunnerDuckExporterConcurrentRotationIsSafe hammers acquire→use→release
// across two alternating credential triples. The refcount contract under test:
// a held exporter always answers Ping (never closed underneath its holder),
// and after the storm exactly one cached entry remains, alive. Run with -race
// (CI does) — the interleaving itself is the assertion surface.
func TestRunnerDuckExporterConcurrentRotationIsSafe(t *testing.T) {
	opened := stubDuckExporterWithDB(t)
	runner := NewRunner(zap.NewNop())
	cfg, old, rotated := duckEvictionFixture()
	runtimes := []*cachedS3Runtime{old, rotated}
	ctx := context.Background()

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				exporter, release, err := runner.getOrCreateDuckExporter(ctx, cfg, runtimes[(g+i)%2])
				if err != nil {
					t.Errorf("goroutine %d iter %d: acquire: %v", g, i, err)
					return
				}
				if pingErr := exporter.DB.PingContext(ctx); pingErr != nil {
					t.Errorf("goroutine %d iter %d: held exporter closed underneath us: %v", g, i, pingErr)
				}
				release()
			}
		}(g)
	}
	wg.Wait()

	require.Len(t, runner.duckExporters, 1)
	live := 0
	for _, db := range opened() {
		if db.PingContext(ctx) == nil {
			live++
		}
	}
	require.Equal(t, 1, live, "storm settled: exactly the cached entry's handle stays open")
}
