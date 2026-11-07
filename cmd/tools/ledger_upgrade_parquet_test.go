package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/lychee-technology/forma/internal/manifest"
	"github.com/lychee-technology/forma/internal/testdb"
	"go.uber.org/zap"
)

// UpgradeS3EndpointEnv names an S3-compatible endpoint for the ledger upgrade
// suite's parquet tier, such as http://127.0.0.1:4566. make
// test-dsql-ministack sets it to MiniStack's own S3, beside the DSQL proxy.
const upgradeS3EndpointEnv = "FORMA_DSQL_S3_ENDPOINT"

const (
	upgradeS3Region         = "us-east-1"
	upgradeManifestTemplate = "manifest/{{.SchemaID}}.json"
)

// TestLedgerUpgradeBaselineAuditsAParquetTierOnS3 is the runbook's third step
// with a lakehouse tier: baseline-schema's own tier, from its flags, resolves
// each schema's live objects through its S3 manifest and decodes them in
// DuckDB over httpfs. DuckDB never reads PostgreSQL here, so this runs
// through MiniStack's proxy, which the DuckDB postgres extension cannot
// read through (#691). A live object holding 12.5 for an integer refuses the
// baseline for every schema; once the manifest lists a corrected object, the
// baseline records generation 1 of each.
func TestLedgerUpgradeBaselineAuditsAParquetTierOnS3(t *testing.T) {
	endpoint := os.Getenv(upgradeS3EndpointEnv)
	if endpoint == "" {
		if testdb.MiniStackTarget() {
			t.Fatalf("%s=ministack without %s: the lane serves the parquet tier from MiniStack's S3", testdb.DSQLTargetEnv, upgradeS3EndpointEnv)
		}
		t.Skipf("%s names no S3-compatible endpoint; TestBaselineSchemaRefusesAParquetValueTheFooterHides audits local objects", upgradeS3EndpointEnv)
	}
	// The endpoint is an emulator, which accepts any key, so the test sends a
	// placeholder that names no account rather than whatever the shell holds.
	const placeholder = "forma-test"
	for _, name := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"} {
		t.Setenv(name, placeholder)
	}
	t.Setenv("AWS_SESSION_TOKEN", "")
	u := newUpgradeTarget(t)
	u.addLedger(t)
	b := newUpgradeBucket(t, u.ctx, endpoint)
	objects := t.TempDir()
	b.putObject(t, "data/100/delta-1.parquet", writeUpgradeObject(t, objects, "orders.parquet", "CAST(7 AS SMALLINT) AS qty, 'first' AS note"))
	b.putObject(t, "data/101/delta-1.parquet", writeUpgradeObject(t, objects, "bad.parquet", "CAST(12.5 AS DOUBLE) AS count"))
	b.putObject(t, "data/101/delta-2.parquet", writeUpgradeObject(t, objects, "good.parquet", "CAST(41 AS DOUBLE) AS count"))
	b.putManifest(t, upgradeOrders, "data/100/delta-1.parquet")
	b.putManifest(t, upgradeVisits, "data/101/delta-1.parquet")

	flags := baselineParquetFlags{
		s3:               s3Flags{bucket: b.name, endpoint: endpoint, region: upgradeS3Region, usePath: true},
		manifestTemplate: upgradeManifestTemplate,
		duck:             duckExportFlags{duckThreads: 1, duckMemLimit: "1GB", queryTimeout: 5 * time.Minute},
	}
	if err := flags.validate(); err != nil {
		t.Fatalf("the tier's flags: %v", err)
	}
	tier, closeTier, err := flags.open(u.ctx, zap.NewNop())
	if err != nil {
		t.Fatalf("open the parquet tier: %v", err)
	}
	t.Cleanup(closeTier)

	out, err := u.baseline(t, false, tier)
	if err == nil {
		t.Fatalf("baseline passed with 12.5 live for an integer:\n%s", out)
	}
	mentionsAll(t, "the refusal", out, "schema visits (id 101) attribute count declared integer",
		fmt.Sprintf("object s3://%s/data/101/delta-1.parquet", b.name), "12.5")
	u.requirePointers(t, 0, 0)

	b.putManifest(t, upgradeVisits, "data/101/delta-2.parquet")
	out, err = u.baseline(t, false, tier)
	if err != nil {
		t.Fatalf("baseline over corrected objects: %v\n%s", err, out)
	}
	mentionsAll(t, "the baseline", out, "Recorded generation 1 of schema orders", "Recorded generation 1 of schema visits")
	u.requirePointers(t, 1, 1)
}

// upgradeBucket is a bucket of its own on the S3-compatible endpoint.
type upgradeBucket struct {
	ctx    context.Context
	client *s3.Client
	name   string
	keys   []string
}

func newUpgradeBucket(t *testing.T, ctx context.Context, endpoint string) *upgradeBucket {
	t.Helper()
	client, err := buildToolS3Client(ctx, upgradeS3Region, endpoint, true)
	if err != nil {
		t.Fatalf("build the s3 client: %v", err)
	}
	b := &upgradeBucket{ctx: ctx, client: client, name: fmt.Sprintf("forma-ledger-%d", time.Now().UnixNano())}
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(b.name)}); err != nil {
		t.Fatalf("create bucket %s on %s: %v", b.name, endpoint, err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, key := range b.keys {
			_, _ = client.DeleteObject(cleanup, &s3.DeleteObjectInput{Bucket: aws.String(b.name), Key: aws.String(key)})
		}
		_, _ = client.DeleteBucket(cleanup, &s3.DeleteBucketInput{Bucket: aws.String(b.name)})
	})
	return b
}

func (b *upgradeBucket) put(t *testing.T, key string, body []byte) {
	t.Helper()
	_, err := b.client.PutObject(b.ctx, &s3.PutObjectInput{Bucket: aws.String(b.name), Key: aws.String(key), Body: bytes.NewReader(body)})
	if err != nil {
		t.Fatalf("put s3://%s/%s: %v", b.name, key, err)
	}
	b.keys = append(b.keys, key)
}

func (b *upgradeBucket) putObject(t *testing.T, key, file string) {
	t.Helper()
	body, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("read %s: %v", file, err)
	}
	b.put(t, key, body)
}

// putManifest makes key the one live object of schemaID, as a CDC flush
// lists it: a bucket-relative delta entry.
func (b *upgradeBucket) putManifest(t *testing.T, schemaID int16, key string) {
	t.Helper()
	path, err := manifest.PathResolver{PathTemplate: upgradeManifestTemplate}.Resolve(schemaID)
	if err != nil {
		t.Fatalf("resolve the manifest of schema %d: %v", schemaID, err)
	}
	body, err := json.Marshal(manifest.Manifest{
		SchemaID: schemaID, Version: 1, UpdatedAtMs: time.Now().UnixMilli(),
		Files: []manifest.FileEntry{{Tier: "delta", Path: key, RowCount: 1}},
	})
	if err != nil {
		t.Fatalf("encode the manifest of schema %d: %v", schemaID, err)
	}
	b.put(t, path, body)
}

// writeUpgradeObject writes a one-row parquet object in the delta layout,
// with the attribute columns attrs selects, and answers its path.
func writeUpgradeObject(t *testing.T, dir, name, attrs string) string {
	t.Helper()
	duck, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer duck.Close()
	path := filepath.Join(dir, name)
	copyStmt := fmt.Sprintf("COPY (SELECT gen_random_uuid() AS row_id, CAST(100 AS BIGINT) AS changed_at, CAST(0 AS BIGINT) AS deleted_at, "+
		"CAST(50 AS BIGINT) AS ltbase_created_at, %s) TO '%s' (FORMAT PARQUET)", attrs, path)
	if _, err := duck.Exec(copyStmt); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}
