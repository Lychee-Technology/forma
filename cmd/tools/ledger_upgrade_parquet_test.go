package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/lychee-technology/forma/internal/testdb"
	"go.uber.org/zap"
)

// UpgradeS3EndpointEnv names an S3-compatible endpoint for the ledger upgrade
// suite's parquet tier, such as http://127.0.0.1:4566. make
// test-dsql-ministack sets it to MiniStack's own S3, beside the DSQL proxy.
const upgradeS3EndpointEnv = "FORMA_DSQL_S3_ENDPOINT"

const upgradeS3Region = "us-east-1"

// TestLedgerUpgradeBaselineAuditsAParquetTierOnS3 is the runbook's third step
// with a lakehouse tier: baseline-schema's own tier, from its flags, resolves
// each schema's live objects from its publication record and its published
// generation's manifest on S3, and decodes them in DuckDB over httpfs. DuckDB never reads PostgreSQL here, so this runs
// through MiniStack's proxy, which the DuckDB postgres extension cannot
// read through (#691). A live object holding 12.5 for an integer refuses the
// baseline for every schema; once a new generation lists a corrected object
// instead, the baseline records generation 1 of each.
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
	u.addPublication(t)
	b := newUpgradeBucket(t, u.ctx, endpoint)
	objects := t.TempDir()
	b.publish(t, u, upgradeOrders, writeUpgradeObject(t, objects, "orders.parquet", "CAST(7 AS SMALLINT) AS qty, 'first' AS note"))
	bad := b.publish(t, u, upgradeVisits, writeUpgradeObject(t, objects, "bad.parquet", "CAST(12.5 AS DOUBLE) AS count"))

	flags := baselineParquetFlags{
		s3:   s3Flags{bucket: b.name, endpoint: endpoint, region: upgradeS3Region, usePath: true},
		duck: duckExportFlags{duckThreads: 1, duckMemLimit: "1GB", queryTimeout: 5 * time.Minute},
	}
	if err := flags.validate(); err != nil {
		t.Fatalf("the tier's flags: %v", err)
	}
	tier, closeTier, err := flags.open(u.ctx, zap.NewNop(), u.pool, u.tables.EntityMain)
	if err != nil {
		t.Fatalf("open the parquet tier: %v", err)
	}
	t.Cleanup(closeTier)

	out, err := u.baseline(t, false, tier)
	if err == nil {
		t.Fatalf("baseline passed with 12.5 live for an integer:\n%s", out)
	}
	mentionsAll(t, "the refusal", out, "schema visits (id 101) attribute count declared integer",
		fmt.Sprintf("object s3://%s/%s", b.name, bad), "12.5")
	u.requirePointers(t, 0, 0)

	b.publish(t, u, upgradeVisits, writeUpgradeObject(t, objects, "good.parquet", "CAST(41 AS DOUBLE) AS count"))
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

// addPublication creates the publication tables of the target's entity_main,
// rendered for DSQL, each in its own transaction.
func (u *upgradeTarget) addPublication(t *testing.T) {
	t.Helper()
	for _, stmt := range publication.Statements(forma.EngineAuroraDSQL, u.tables.EntityMain) {
		u.exec(t, stmt)
	}
	tables := publication.TablesFor(u.tables.EntityMain)
	t.Cleanup(func() {
		for _, table := range []string{tables.Record, tables.Generation, tables.Epoch} {
			_, _ = u.pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table)
		}
	})
}

// publish makes the parquet object at file the one live object of schemaID,
// as a cdc-init publishes it: the object at its canonical key, a manifest
// listing it alone, and the record pointed at that generation. It answers
// the object's key.
func (b *upgradeBucket) publish(t *testing.T, u *upgradeTarget, schemaID int16, file string) string {
	t.Helper()
	tables := publication.TablesFor(u.tables.EntityMain)
	store := &manifestv2.S3Store{Client: b.client, Bucket: b.name}
	layout := manifestv2.Layout{Prefix: "data"}
	cur := u.publicationRecord(t, tables, schemaID)
	h, err := manifestv2.Stage(cur, publication.KindInit, uuid.New(), uuid.New())
	if err != nil {
		t.Fatalf("stage an init of schema %d: %v", schemaID, err)
	}
	key, err := layout.NewParquetKey(schemaID, cur.Epoch, manifestv2.TierBase)
	if err != nil {
		t.Fatalf("mint an object key of schema %d: %v", schemaID, err)
	}
	b.putObject(t, key, file)
	m, err := manifestv2.NewInit(h, []manifestv2.File{describeUpgradeObject(t, file, key)})
	if err != nil {
		t.Fatalf("build the init manifest of schema %d: %v", schemaID, err)
	}
	manifestKey, digest, err := manifestv2.PutManifest(b.ctx, store, layout, m)
	if err != nil {
		t.Fatalf("put the init manifest of schema %d: %v", schemaID, err)
	}
	tx, err := u.pool.Begin(u.ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(u.ctx) }()
	var stagedAt int64
	if err := tx.QueryRow(u.ctx, "SELECT "+sqlgen.ServerTimeMillisSQL).Scan(&stagedAt); err != nil {
		t.Fatalf("read the server time: %v", err)
	}
	if _, err := publication.Publish(u.ctx, publication.PgxTx(tx), tables, cur, publication.Next{
		Kind: publication.KindInit, Token: h.Token, ManifestKey: manifestKey, ManifestSHA256: digest,
		Lineage: h.Lineage, StagedAt: stagedAt, StagingBudget: time.Minute,
	}); err != nil {
		t.Fatalf("publish the init of schema %d: %v", schemaID, err)
	}
	if err := tx.Commit(u.ctx); err != nil {
		t.Fatalf("commit the init of schema %d: %v", schemaID, err)
	}
	return key
}

// publicationRecord reads schemaID's record, creating it UNPUBLISHED first
// when the schema has none, as registration does.
func (u *upgradeTarget) publicationRecord(t *testing.T, tables publication.Tables, schemaID int16) publication.Record {
	t.Helper()
	tx, err := u.pool.Begin(u.ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(u.ctx) }()
	if _, err := publication.Initialize(u.ctx, publication.PgxTx(tx), tables, schemaID); err != nil {
		t.Fatalf("initialize the record of schema %d: %v", schemaID, err)
	}
	snap, err := publication.Read(u.ctx, publication.PgxTx(tx), tables, schemaID)
	if err != nil {
		t.Fatalf("read the record of schema %d: %v", schemaID, err)
	}
	if err := tx.Commit(u.ctx); err != nil {
		t.Fatalf("commit the record of schema %d: %v", schemaID, err)
	}
	return snap.Record
}

// describeUpgradeObject answers the manifest entry of the one-row object at
// file, stored at key.
func describeUpgradeObject(t *testing.T, file, key string) manifestv2.File {
	t.Helper()
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("read %s: %v", file, err)
	}
	sum := sha256.Sum256(raw)
	entry := manifestv2.File{Tier: manifestv2.TierBase, Key: key, SizeBytes: int64(len(raw)), SHA256: hex.EncodeToString(sum[:]),
		RowCount: 1, RowIDMin: upgradeObjectRowID, RowIDMax: upgradeObjectRowID, Columns: map[string]string{}}
	duck, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer duck.Close()
	rows, err := duck.Query(fmt.Sprintf("SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_parquet('%s'))", file))
	if err != nil {
		t.Fatalf("describe %s: %v", file, err)
	}
	defer rows.Close()
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			t.Fatalf("describe %s: %v", file, err)
		}
		entry.Columns[name] = typ
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("describe %s: %v", file, err)
	}
	return entry
}

// upgradeObjectRowID is the row ID of every one-row upgrade object.
var upgradeObjectRowID = uuid.MustParse("00000000-0000-7000-8000-000000000001")

// writeUpgradeObject writes a one-row parquet object in the export layout,
// with the attribute columns attrs selects, and answers its path.
func writeUpgradeObject(t *testing.T, dir, name, attrs string) string {
	t.Helper()
	duck, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer duck.Close()
	path := filepath.Join(dir, name)
	copyStmt := fmt.Sprintf("COPY (SELECT CAST('%s' AS UUID) AS row_id, CAST(100 AS BIGINT) AS changed_at, CAST(0 AS BIGINT) AS deleted_at, "+
		"CAST(50 AS BIGINT) AS ltbase_created_at, %s) TO '%s' (FORMAT PARQUET)", upgradeObjectRowID, attrs, path)
	if _, err := duck.Exec(copyStmt); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}
