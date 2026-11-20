package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/testdb"
)

// memBucket is an in-memory bucket for the CDC tools' object client: it
// honours If-None-Match: * as S3 does, and answers NoSuchKey for a key it
// does not hold.
type memBucket struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func (b *memBucket) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	body, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, fmt.Errorf("read the body of %s: %w", aws.ToString(in.Key), err)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	key := aws.ToString(in.Key)
	if _, ok := b.objects[key]; ok && aws.ToString(in.IfNoneMatch) == "*" {
		return nil, &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "the key exists"}
	}
	b.objects[key] = body
	return &s3.PutObjectOutput{}, nil
}

func (b *memBucket) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	body, ok := b.objects[aws.ToString(in.Key)]
	if !ok {
		return nil, &types.NoSuchKey{}
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(body))}, nil
}

// stubCDCObjectClient routes the CDC tools' object writes to bucket.
func stubCDCObjectClient(t *testing.T, bucket *memBucket) {
	t.Helper()
	old := toolCDCObjectClientFn
	t.Cleanup(func() { toolCDCObjectClientFn = old })
	toolCDCObjectClientFn = func(context.Context, string, string, bool) (cdc.S3Client, error) { return bucket, nil }
}

// pgArgs answers the -pg-* flags naming the integration database.
func pgArgs(t *testing.T) []string {
	t.Helper()
	u, err := url.Parse(testdb.ResolveDSN())
	if err != nil {
		t.Fatalf("parse the test DSN: %v", err)
	}
	password, _ := u.User.Password()
	t.Setenv("PGPASSWORD", password)
	sslMode := u.Query().Get("sslmode")
	if sslMode == "" {
		sslMode = "disable"
	}
	return []string{"-pg-host", u.Hostname(), "-pg-port", u.Port(), "-pg-user", u.User.Username(),
		"-pg-db", u.Path[1:], "-pg-ssl-mode", sslMode}
}

// TestCDCInitCLIPublishesAndHandsOffInTheDeploymentsChangeLog runs init-db
// and then the cdc-init command, flags and all, against a deployment whose
// change log is not the flag default change_log: init-db created it under
// the name its -change-log-table gave. cdc-init named the same table through
// its own -change-log-table, published an init generation of the schema's
// live rows and handed their entries off in that table, leaving none dirty.
func TestCDCInitCLIPublishesAndHandsOffInTheDeploymentsChangeLog(t *testing.T) {
	tg := newInitDBTarget(t)
	if tg.opts.changeLog == "change_log" {
		t.Fatal("the fixture's change log must not be the flag default")
	}
	tg.writeSchema(t, "orders", map[string]string{"title": "text_01"}, nil)
	if out, err := tg.run(t); err != nil {
		t.Fatalf("init-db: %v\n%s", err, out)
	}
	ids, err := loadRegisteredSchemaIDs(context.Background(), tg.pool, tg.opts.schemaTable)
	if err != nil {
		t.Fatalf("read the registry: %v", err)
	}
	id := ids["orders"]
	tg.insertRow(t, id, 0)
	tg.insertRow(t, id, 0)
	for _, name := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"} {
		t.Setenv(name, "")
	}
	bucket := &memBucket{objects: map[string][]byte{}}
	stubCDCObjectClient(t, bucket)

	args := append(pgArgs(t),
		"-change-log-table", tg.opts.changeLog,
		"-entity-main-table", tg.opts.entityMain,
		"-eav-table", tg.opts.eavTable,
		"-s3-bucket", "cdc-init-cli",
		"-schema-registry-table", tg.opts.schemaTable,
		"-schema-dir", tg.opts.schemaDir,
		"-schema-id", fmt.Sprint(id),
	)
	var runErr error
	captureStdStreams(t, func() { runErr = runCDCInit(context.Background(), args) })
	if runErr != nil {
		t.Fatalf("cdc-init with -change-log-table %s: %v", tg.opts.changeLog, runErr)
	}

	rec := tg.records(t)["orders"]
	if rec.State != publication.Published {
		t.Fatalf("schema %d is %q after cdc-init, want %s", id, rec.State, publication.Published)
	}
	ref, err := manifestv2.RecordRef(rec)
	if err != nil {
		t.Fatalf("name the generation: %v", err)
	}
	m, err := manifestv2.Fetch(context.Background(), &manifestv2.S3Store{Client: bucket, Bucket: "cdc-init-cli"}, ref)
	if err != nil {
		t.Fatalf("fetch the published manifest: %v", err)
	}
	var rows int64
	for _, f := range m.Files {
		rows += f.RowCount
	}
	if m.Kind != publication.KindInit || rows != 2 {
		t.Fatalf("published a %s generation of %d rows, want an init of 2", m.Kind, rows)
	}
	var dirty int
	if err := tg.pool.QueryRow(context.Background(), "SELECT count(*) FROM "+quoteIdentifier(tg.opts.changeLog)+
		" WHERE schema_id = $1 AND flushed_at = 0", id).Scan(&dirty); err != nil {
		t.Fatalf("count the dirty entries: %v", err)
	}
	if dirty != 0 {
		t.Fatalf("%d entries stay dirty in %s after the handoff, want 0", dirty, tg.opts.changeLog)
	}
}
