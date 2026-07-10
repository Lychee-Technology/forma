package production

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma/internal/e2e_harness"
)

// Environment variables understood by the production harness.
const (
	// KeepEnvVar keeps containers, databases, and S3 state alive after the
	// test for manual inspection. The harness disables the testcontainers
	// reaper (ryuk) itself when this is set — see enforceContainerRetention —
	// so no extra environment variables are required.
	KeepEnvVar = "KEEP_E2E_ENV"

	// ryukDisabledVar is testcontainers-go's own switch for the ryuk reaper,
	// which would otherwise terminate containers on process exit regardless
	// of KEEP_E2E_ENV.
	ryukDisabledVar = "TESTCONTAINERS_RYUK_DISABLED"
	// SeedVar pins the cluster base seed for deterministic fixture replay.
	SeedVar = "E2E_SEED"

	externalPGDSNVar       = "PRODUCTION_E2E_EXTERNAL_PG_DSN"
	externalS3EndpointVar  = "PRODUCTION_E2E_EXTERNAL_S3_ENDPOINT"
	externalS3BucketVar    = "PRODUCTION_E2E_EXTERNAL_S3_BUCKET"
	externalS3RegionVar    = "PRODUCTION_E2E_EXTERNAL_S3_REGION"
	externalS3AccessKeyVar = "PRODUCTION_E2E_EXTERNAL_S3_ACCESS_KEY"
	externalS3SecretKeyVar = "PRODUCTION_E2E_EXTERNAL_S3_SECRET_KEY"
)

// Cluster owns the container set shared by every test in one test binary:
// Postgres (admin database used only for CREATE/DROP DATABASE) and MinIO
// with one bucket per run. Per-test state lives in Env.
type Cluster struct {
	Base   *e2e_harness.TestHarness
	S3     *s3.Client
	Bucket string
	RunID  string
	Seed   int64

	PGHost     string
	PGPort     string
	PGUser     string
	PGPassword string
	PGSSLMode  string

	S3Endpoint  string
	S3Region    string
	S3AccessKey string
	S3SecretKey string

	external bool
	envSeq   atomic.Int64
}

// ClusterOption customizes StartCluster.
type ClusterOption func(*clusterOptions)

type clusterOptions struct {
	seed int64
}

// WithClusterSeed overrides the base seed (default: E2E_SEED env or wall clock).
func WithClusterSeed(seed int64) ClusterOption {
	return func(o *clusterOptions) { o.seed = seed }
}

// KeepEnv reports whether KEEP_E2E_ENV=1 is set.
func KeepEnv() bool {
	v := os.Getenv(KeepEnvVar)
	return v == "1" || strings.EqualFold(v, "true")
}

// enforceContainerRetention makes KEEP_E2E_ENV self-sufficient: it disables
// the testcontainers ryuk reaper, which would otherwise remove the containers
// on process exit even though Shutdown/teardown were skipped. It must run
// before the first container is created (testcontainers reads its config
// once per process).
func enforceContainerRetention() {
	if !KeepEnv() || os.Getenv(ryukDisabledVar) != "" {
		return
	}
	if err := os.Setenv(ryukDisabledVar, "true"); err == nil {
		fmt.Printf("%s=1: disabling testcontainers reaper (%s=true) so containers survive the run\n",
			KeepEnvVar, ryukDisabledVar)
	}
}

// StartCluster starts (or, with PRODUCTION_E2E_EXTERNAL_*, connects to) the
// shared infrastructure and creates the run bucket.
func StartCluster(ctx context.Context, opts ...ClusterOption) (*Cluster, error) {
	options := clusterOptions{seed: resolveSeed()}
	for _, opt := range opts {
		opt(&options)
	}
	enforceContainerRetention()

	c := &Cluster{
		Base:        &e2e_harness.TestHarness{},
		Seed:        options.seed,
		RunID:       fmt.Sprintf("%s-%06x", time.Now().UTC().Format("20060102-150405"), uint64(options.seed)&0xffffff),
		PGSSLMode:   "disable",
		S3Region:    "us-east-1",
		S3AccessKey: "minioadmin",
		S3SecretKey: "minioadmin",
	}

	if err := c.startInfra(ctx); err != nil {
		return nil, err
	}

	s3Client, err := e2e_harness.NewS3Client(ctx, c.S3Endpoint, c.S3Region, c.S3AccessKey, c.S3SecretKey)
	if err != nil {
		_ = c.stopInfra(ctx)
		return nil, fmt.Errorf("create s3 client: %w", err)
	}
	c.S3 = s3Client

	if c.Bucket == "" {
		c.Bucket = "e2e-" + strings.ToLower(c.RunID)
	}
	// Idempotent: external buckets may already exist.
	_, _ = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(c.Bucket)})

	return c, nil
}

// startInfra brings up containers, or wires external infrastructure when the
// PRODUCTION_E2E_EXTERNAL_* escape hatch is set (mirrors FEDERATED_E2E_EXTERNAL_*).
func (c *Cluster) startInfra(ctx context.Context) error {
	externalDSN := os.Getenv(externalPGDSNVar)
	externalS3 := os.Getenv(externalS3EndpointVar)
	if externalDSN != "" && externalS3 != "" {
		return c.connectExternal(ctx, externalDSN, externalS3)
	}

	if _, err := c.Base.StartPostgres(ctx); err != nil {
		return fmt.Errorf("start postgres container: %w", err)
	}
	if _, err := c.Base.StartS3(ctx); err != nil {
		_ = c.Base.StopPostgres(ctx)
		return fmt.Errorf("start minio container: %w", err)
	}
	c.S3Endpoint = c.Base.S3Endpoint
	c.PGUser = "postgres"
	c.PGPassword = "password"
	c.PGHost, c.PGPort = pgHostPortFromDSN(c.Base.PGDSN)
	return nil
}

func (c *Cluster) connectExternal(ctx context.Context, pgDSN, s3Endpoint string) error {
	cfg, err := pgx.ParseConfig(pgDSN)
	if err != nil {
		return fmt.Errorf("parse external pg dsn: %w", err)
	}
	if err := c.Base.ConnectPostgres(ctx, pgDSN); err != nil {
		return fmt.Errorf("connect external postgres: %w", err)
	}
	c.external = true
	c.PGHost = cfg.Host
	c.PGPort = strconv.Itoa(int(cfg.Port))
	c.PGUser = cfg.User
	c.PGPassword = cfg.Password
	c.S3Endpoint = s3Endpoint
	c.Base.S3Endpoint = s3Endpoint
	if v := os.Getenv(externalS3BucketVar); v != "" {
		c.Bucket = v
	}
	if v := os.Getenv(externalS3RegionVar); v != "" {
		c.S3Region = v
	}
	if v := os.Getenv(externalS3AccessKeyVar); v != "" {
		c.S3AccessKey = v
	}
	if v := os.Getenv(externalS3SecretKeyVar); v != "" {
		c.S3SecretKey = v
	}
	return nil
}

// Shutdown stops the shared containers. Under KEEP_E2E_ENV=1 it prints
// connection info instead and leaves everything running.
func (c *Cluster) Shutdown(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if KeepEnv() {
		fmt.Printf("KEEP_E2E_ENV=1: leaving cluster running\n"+
			"  postgres: %s\n  s3:       %s (bucket %s, key %s / %s)\n",
			c.Base.PGDSN, c.S3Endpoint, c.Bucket, c.S3AccessKey, c.S3SecretKey)
		return nil
	}
	return c.stopInfra(ctx)
}

func (c *Cluster) stopInfra(ctx context.Context) error {
	var errs []string
	if !c.external {
		if err := c.Base.StopS3(ctx); err != nil {
			errs = append(errs, fmt.Sprintf("stop s3: %v", err))
		}
	}
	if err := c.Base.StopPostgres(ctx); err != nil {
		errs = append(errs, fmt.Sprintf("stop postgres: %v", err))
	}
	if len(errs) > 0 {
		return fmt.Errorf("cluster shutdown: %s", strings.Join(errs, "; "))
	}
	return nil
}

var (
	sharedOnce    sync.Once
	sharedCluster *Cluster
	sharedErr     error
)

// SharedCluster returns the per-test-binary cluster, starting it on first
// use. When infrastructure cannot be started (typically: docker unavailable)
// the calling test is skipped, matching the repo's docker-gated test pattern.
func SharedCluster(t *testing.T) *Cluster {
	t.Helper()
	sharedOnce.Do(func() {
		sharedCluster, sharedErr = StartCluster(context.Background())
	})
	if sharedErr != nil {
		t.Skipf("production e2e cluster unavailable (docker required): %v", sharedErr)
	}
	return sharedCluster
}

// ShutdownSharedCluster releases the SharedCluster infrastructure; call it
// from TestMain after m.Run().
func ShutdownSharedCluster(ctx context.Context) error {
	if sharedCluster == nil {
		return nil
	}
	return sharedCluster.Shutdown(ctx)
}

func resolveSeed() int64 {
	if v := os.Getenv(SeedVar); v != "" {
		if seed, err := strconv.ParseInt(v, 10, 64); err == nil {
			return seed
		}
	}
	return time.Now().UnixNano()
}

// pgHostPortFromDSN extracts host and mapped port from a postgres:// DSN.
func pgHostPortFromDSN(dsn string) (string, string) {
	host, port := "localhost", "5432"
	rest := strings.TrimPrefix(strings.TrimPrefix(dsn, "postgres://"), "postgresql://")
	if at := strings.Index(rest, "@"); at >= 0 {
		rest = rest[at+1:]
	}
	if slash := strings.Index(rest, "/"); slash >= 0 {
		rest = rest[:slash]
	}
	if q := strings.Index(rest, "?"); q >= 0 {
		rest = rest[:q]
	}
	if colon := strings.LastIndex(rest, ":"); colon >= 0 {
		host, port = rest[:colon], rest[colon+1:]
	} else if rest != "" {
		host = rest
	}
	return host, port
}
