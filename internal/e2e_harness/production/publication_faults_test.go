//go:build e2e

package production

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/google/uuid"

	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// The helpers below stage a fault in a schema's published inventory, each
// named for the state it leaves (#901): an entry that no longer lists its
// object, a stamp that no longer matches its bytes, an object no writer would
// list. They are the only place a test changes what a schema publishes.
// Protocol V2 generations are immutable (docs/publication-protocol-v2.md), so
// each one publishes a test-made generation: the current generation's files,
// edited, committed through the same compare-and-set the writers use. The
// fault is the generation's content; its publication is a real one. Only a
// test may publish outside internal/cdcv2, which is why they live in a test
// file.

// testStagingBudget is W for a test-made generation, which commits right
// after it is staged.
const testStagingBudget = time.Minute

// publishTestGeneration commits, as schema's next generation, a flush
// generation that lists edit's answer for the current generation's files (none
// while the schema is unpublished), keeping the current lineage. It answers the
// generation it published.
func publishTestGeneration(ctx context.Context, t *testing.T, env *Env, schema SchemaRef,
	edit func([]manifestv2.File) []manifestv2.File) *Manifest {
	t.Helper()
	tables := publication.TablesFor(env.Tables.EntityMain)
	tx, err := env.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin the test publication of schema %d: %v", schema.ID, err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	snap, err := publication.Read(ctx, publication.PgxTx(tx), tables, schema.ID)
	if err != nil {
		t.Fatalf("read the record of schema %d: %v", schema.ID, err)
	}
	cur, lineage := snap.Record, uuid.New()
	var files []manifestv2.File
	if cur.State == publication.Published {
		lineage = cur.Lineage
		files = currentFiles(ctx, t, env, cur)
	}
	files = edit(files)
	slices.SortFunc(files, func(a, b manifestv2.File) int { return strings.Compare(a.Key, b.Key) })
	h, err := manifestv2.Stage(cur, publication.KindFlush, lineage, uuid.New())
	if err != nil {
		t.Fatalf("stage a test generation of schema %d: %v", schema.ID, err)
	}
	m := manifestv2.Manifest{
		Format: publication.ManifestFormat, SchemaID: h.SchemaID, Epoch: h.Epoch, Generation: h.Generation,
		ParentGeneration: h.ParentGeneration, Lineage: h.Lineage, Token: h.Token, Kind: h.Kind,
		Files: append([]manifestv2.File{}, files...),
	}
	key, digest, err := manifestv2.PutManifest(ctx, env.manifestStore(), env.manifestLayout(), m)
	if err != nil {
		t.Fatalf("write the test generation %d of schema %d: %v", h.Generation, schema.ID, err)
	}
	out, err := publication.Publish(ctx, publication.PgxTx(tx), tables, cur, publication.Next{
		Kind: h.Kind, Token: h.Token, ManifestKey: key, ManifestSHA256: digest, Lineage: h.Lineage,
		StagedAt: snap.ReadAt, StagingBudget: testStagingBudget,
	})
	if err != nil {
		t.Fatalf("publish the test generation %d of schema %d: %v", h.Generation, schema.ID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit the test generation %d of schema %d: %v", h.Generation, schema.ID, err)
	}
	view, err := manifestView(ctx, env.manifestStore(), out)
	if err != nil {
		t.Fatalf("read back the test generation of schema %d: %v", schema.ID, err)
	}
	return view
}

// currentFiles answers a copy of the files cur's generation lists.
func currentFiles(ctx context.Context, t *testing.T, env *Env, cur publication.Record) []manifestv2.File {
	t.Helper()
	ref, err := manifestv2.RecordRef(cur)
	if err != nil {
		t.Fatalf("name schema %d's generation: %v", cur.SchemaID, err)
	}
	doc, err := manifestv2.Fetch(ctx, env.manifestStore(), ref)
	if err != nil {
		t.Fatalf("fetch schema %d's generation %d: %v", cur.SchemaID, cur.Generation, err)
	}
	files := make([]manifestv2.File, 0, len(doc.Files))
	for _, f := range doc.Files {
		f.Columns = copyColumns(f.Columns)
		files = append(files, f)
	}
	return files
}

// poisonManifestChecksum publishes a generation whose entry at path carries a
// well-formed but wrong digest: a stamp/bytes divergence whose object is still
// decodable.
func poisonManifestChecksum(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, path string) {
	t.Helper()
	editFile(ctx, t, env, schema, path, "", func(f *manifestv2.File) { f.SHA256 = strings.Repeat("0", 64) })
}

// restampBaseColumns publishes a generation whose base entry at path carries
// columns as its column set, after a test rewrote the object in place, leaving
// its key and every other entry alone.
func restampBaseColumns(ctx context.Context, t *testing.T, env *Env, schema SchemaRef,
	path string, columns map[string]string) {
	t.Helper()
	editFile(ctx, t, env, schema, path, manifestv2.TierBase, func(f *manifestv2.File) { f.Columns = copyColumns(columns) })
}

// editFile publishes a generation in which the entry at path (on tier, when
// set) is edited, failing the test when the inventory lists none.
func editFile(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, path string, tier manifestv2.Tier,
	edit func(*manifestv2.File)) {
	t.Helper()
	publishTestGeneration(ctx, t, env, schema, func(files []manifestv2.File) []manifestv2.File {
		found := false
		for i := range files {
			if files[i].Key == path && (tier == "" || files[i].Tier == tier) {
				edit(&files[i])
				found = true
			}
		}
		if !found {
			t.Fatalf("schema %d's generation lists no %s entry at %s: %+v", schema.ID, tier, path, files)
		}
		return files
	})
}

// newTestParquetKey answers a fresh object key of schema's current epoch on
// tier in the V2 layout, for a test that fabricates an object to list.
func newTestParquetKey(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, tier manifestv2.Tier) string {
	t.Helper()
	rec, err := env.readRecord(ctx, schema.ID)
	if err != nil {
		t.Fatalf("read the record of schema %d: %v", schema.ID, err)
	}
	key, err := env.manifestLayout().NewParquetKey(schema.ID, rec.Epoch, tier)
	if err != nil {
		t.Fatalf("mint a %s key for schema %d: %v", tier, schema.ID, err)
	}
	return key
}

// RegisterParquetInManifest publishes a generation that lists the object at
// key on tier besides the current generation's files, so manifest-driven
// reads see a file the production exporter would never write (e.g. a
// fabricated wrong-schema parquet, #187). The entry describes the object as
// it is: its size, digest, row count and footer columns, and its row_id range
// when it has a UUID row_id column; a file without one claims the whole key
// range. key must be a V2 key of the schema's epoch (newTestParquetKey): a
// manifest listing any other is refused before it is written.
func (e *Env) RegisterParquetInManifest(ctx context.Context, schema SchemaRef, key, tier string) error {
	e.T.Helper()
	f, err := e.describeObject(ctx, key, manifestv2.Tier(tier))
	if err != nil {
		return fmt.Errorf("describe %s to list it: %w", key, err)
	}
	publishTestGeneration(ctx, e.T, e, schema, func(files []manifestv2.File) []manifestv2.File {
		return append(files, f)
	})
	return nil
}

// describeObject measures the object at key as a manifest entry.
func (e *Env) describeObject(ctx context.Context, key string, tier manifestv2.Tier) (manifestv2.File, error) {
	raw, err := e.objectBytes(ctx, key)
	if err != nil {
		return manifestv2.File{}, err
	}
	digest := sha256.Sum256(raw)
	f := manifestv2.File{Tier: tier, Key: key, SizeBytes: int64(len(raw)), SHA256: hex.EncodeToString(digest[:])}
	if f.Columns, err = e.parquetColumns(ctx, key); err != nil {
		return manifestv2.File{}, err
	}
	uri := e.ParquetURI(key)
	if err := e.Duck.DB.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s')", uri)).Scan(&f.RowCount); err != nil {
		return manifestv2.File{}, fmt.Errorf("count the rows of %s: %w", key, err)
	}
	f.RowIDMin, f.RowIDMax = uuid.MustParse("00000000-0000-0000-0000-000000000001"), uuid.Max
	if f.Columns["row_id"] != "UUID" {
		return f, nil
	}
	if err := e.Duck.DB.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT MIN(row_id)::VARCHAR, MAX(row_id)::VARCHAR FROM read_parquet('%s')", uri)).Scan(&f.RowIDMin, &f.RowIDMax); err != nil {
		return manifestv2.File{}, fmt.Errorf("read the row_id range of %s: %w", key, err)
	}
	return f, nil
}

// parquetColumns answers the footer schema of the object at key.
func (e *Env) parquetColumns(ctx context.Context, key string) (map[string]string, error) {
	rows, err := e.Duck.DB.QueryContext(ctx, fmt.Sprintf("DESCRIBE SELECT * FROM read_parquet('%s')", e.ParquetURI(key)))
	if err != nil {
		return nil, fmt.Errorf("describe %s: %w", key, err)
	}
	defer func() { _ = rows.Close() }()
	cols := map[string]string{}
	for rows.Next() {
		var name, typ string
		var null, keyCol, def, extra any
		if err := rows.Scan(&name, &typ, &null, &keyCol, &def, &extra); err != nil {
			return nil, fmt.Errorf("scan the description of %s: %w", key, err)
		}
		cols[name] = typ
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("describe %s: %w", key, err)
	}
	return cols, nil
}

// objectBytes answers the bytes of the object at key.
func (e *Env) objectBytes(ctx context.Context, key string) ([]byte, error) {
	out, err := e.Cluster.S3.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(e.Cluster.Bucket), Key: aws.String(key)})
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", key, err)
	}
	defer func() { _ = out.Body.Close() }()
	raw, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", key, err)
	}
	return raw, nil
}

// RunInitKeepingDelta runs a real cdc-init and then publishes a generation
// that lists the delta files the schema's generation before the init listed
// beside the init's base: the layout every pre-#371 re-init left behind, a
// fresh base with older delta listed under it. No V2 tool produces it, since
// an init generation lists its own export's base files only, but the reader
// must keep folding such a set by version (#210 strict recency), so the
// harness publishes it as a test-made generation. With no delta listed before
// the init it is a plain RunInit.
func (e *Env) RunInitKeepingDelta(ctx context.Context, schema SchemaRef) (*InitReport, error) {
	e.T.Helper()
	rec, err := e.readRecord(ctx, schema.ID)
	if err != nil {
		return nil, fmt.Errorf("keep delta: %w", err)
	}
	var kept []manifestv2.File
	if rec.State == publication.Published {
		for _, f := range currentFiles(ctx, e.T, e, rec) {
			if f.Tier == manifestv2.TierDelta {
				kept = append(kept, f)
			}
		}
	}
	report, err := e.RunInit(ctx, schema)
	if err != nil || len(kept) == 0 {
		return report, err
	}
	report.Manifest = publishTestGeneration(ctx, e.T, e, schema, func(files []manifestv2.File) []manifestv2.File {
		return append(files, kept...)
	})
	return report, nil
}

// redropSchema drops schema and registers it again under its id, as
// drop-schema and init-db do: Reset, then Activate, each in its own
// transaction. Its rows stay (no purge, no residue check): the tests that use
// it only observe that every process that loaded the schema before is fenced
// off it. The record ends two epochs on, UNPUBLISHED.
func redropSchema(ctx context.Context, t *testing.T, env *Env, schema SchemaRef) publication.Record {
	t.Helper()
	tables := publication.TablesFor(env.Tables.EntityMain)
	var rec publication.Record
	for _, step := range []func(publication.Tx, publication.Record) (publication.Record, error){
		func(tx publication.Tx, cur publication.Record) (publication.Record, error) {
			return publication.Reset(ctx, tx, tables, cur)
		},
		func(tx publication.Tx, cur publication.Record) (publication.Record, error) {
			return publication.Activate(ctx, tx, tables, cur, nil)
		},
	} {
		tx, err := env.Pool.Begin(ctx)
		if err != nil {
			t.Fatalf("redrop %s: begin: %v", schema.Name, err)
		}
		snap, err := publication.Read(ctx, publication.PgxTx(tx), tables, schema.ID)
		if err == nil {
			rec, err = step(publication.PgxTx(tx), snap.Record)
		}
		if err == nil {
			err = tx.Commit(ctx)
		}
		if err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("redrop %s: %v", schema.Name, err)
		}
	}
	return rec
}

// dropSchemaRecord transitions schema's publication record to DROPPED via
// publication.Reset, returning the new epoch and generation counter.
func dropSchemaRecord(ctx context.Context, t *testing.T, env *Env, schema SchemaRef) (epoch, gen int64) {
	t.Helper()
	rec := runRecordTransition(ctx, t, env, schema, func(tx publication.Tx, tables publication.Tables, cur publication.Record) (publication.Record, error) {
		return publication.Reset(ctx, tx, tables, cur)
	})
	return rec.Epoch, rec.Generation
}

// readSchemaRecordState reads schema's current publication record state,
// epoch, and generation counter without exposing internal/publication types
// to e2e test files.
func readSchemaRecordState(ctx context.Context, t *testing.T, env *Env, schema SchemaRef) (state string, epoch, gen int64) {
	t.Helper()
	rec, err := env.readRecord(ctx, schema.ID)
	if err != nil {
		t.Fatalf("read record state for %s (%d): %v", schema.Name, schema.ID, err)
	}
	return string(rec.State), rec.Epoch, rec.Generation
}

func runRecordTransition(ctx context.Context, t *testing.T, env *Env, schema SchemaRef,
	fn func(publication.Tx, publication.Tables, publication.Record) (publication.Record, error)) publication.Record {
	t.Helper()
	tables := publication.TablesFor(env.Tables.EntityMain)
	tx, err := env.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin record transition for %s: %v", schema.Name, err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	snap, err := publication.Read(ctx, publication.PgxTx(tx), tables, schema.ID)
	if err != nil {
		t.Fatalf("read record for %s: %v", schema.Name, err)
	}
	rec, err := fn(publication.PgxTx(tx), tables, snap.Record)
	if err != nil {
		t.Fatalf("transition record for %s: %v", schema.Name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit record transition for %s: %v", schema.Name, err)
	}
	return rec
}

// deletePublicationRecord removes schema's publication record row to simulate
// a missing record (ErrNoRecord) for fail-closed reader tests.
func deletePublicationRecord(ctx context.Context, t *testing.T, env *Env, schema SchemaRef) {
	t.Helper()
	tables := publication.TablesFor(env.Tables.EntityMain)
	if _, err := env.Pool.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE schema_id = $1", tables.Record), schema.ID); err != nil {
		t.Fatalf("delete publication record for %s: %v", schema.Name, err)
	}
}

// forceAwaitingInitRecord rewrites schema's publication record into the
// AWAITING_INIT cutover state for fail-closed reader tests.
func forceAwaitingInitRecord(ctx context.Context, t *testing.T, env *Env, schema SchemaRef) {
	t.Helper()
	tables := publication.TablesFor(env.Tables.EntityMain)
	q := fmt.Sprintf(
		"UPDATE %s SET state = $1, parent_generation = 0, manifest_key = '', manifest_sha256 = '', lineage = NULL WHERE schema_id = $2",
		tables.Record,
	)
	if _, err := env.Pool.Exec(ctx, q, string(publication.AwaitingInit), schema.ID); err != nil {
		t.Fatalf("force AWAITING_INIT for %s: %v", schema.Name, err)
	}
}

// pointRecordAtStaleManifest repoints schema's current publication record at an
// older generation's manifest key and digest while keeping the newer
// generation/token, proving manifest identity verification catches stale pointers.
func pointRecordAtStaleManifest(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, stale *Manifest) {
	t.Helper()
	tables := publication.TablesFor(env.Tables.EntityMain)
	q := fmt.Sprintf("UPDATE %s SET manifest_key = $1, manifest_sha256 = $2 WHERE schema_id = $3", tables.Record)
	if _, err := env.Pool.Exec(ctx, q, stale.Key, stale.SHA256, schema.ID); err != nil {
		t.Fatalf("point record of %s at stale manifest %s: %v", schema.Name, stale.Key, err)
	}
}

// publishIllicitEmptyFlushManifest commits a valid-digest kind=flush manifest
// with an empty file list directly, bypassing manifestv2's writer guard so the
// reader's fail-closed check against empty flush generations is exercised.
func publishIllicitEmptyFlushManifest(ctx context.Context, t *testing.T, env *Env, schema SchemaRef) {
	t.Helper()
	tables := publication.TablesFor(env.Tables.EntityMain)
	tx, err := env.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin empty-flush publish for %s: %v", schema.Name, err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	snap, err := publication.Read(ctx, publication.PgxTx(tx), tables, schema.ID)
	if err != nil {
		t.Fatalf("read record for %s: %v", schema.Name, err)
	}
	cur := snap.Record
	lineage := cur.Lineage
	if lineage == uuid.Nil {
		lineage = uuid.New()
	}
	h, err := manifestv2.Stage(cur, publication.KindFlush, lineage, uuid.New())
	if err != nil {
		t.Fatalf("stage empty-flush header for %s: %v", schema.Name, err)
	}
	doc := manifestv2.Manifest{
		Format: publication.ManifestFormat, SchemaID: h.SchemaID, Epoch: h.Epoch, Generation: h.Generation,
		ParentGeneration: h.ParentGeneration, Lineage: h.Lineage, Token: h.Token, Kind: publication.KindFlush,
		Files: []manifestv2.File{},
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal empty-flush manifest: %v", err)
	}
	sum := sha256.Sum256(raw)
	digest := hex.EncodeToString(sum[:])
	key := env.manifestLayout().ManifestKey(h.SchemaID, h.Epoch, h.Generation, h.Token)
	if _, err := env.Cluster.S3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(env.Cluster.Bucket), Key: aws.String(key), Body: bytes.NewReader(raw),
	}); err != nil {
		t.Fatalf("put empty-flush manifest %s: %v", key, err)
	}
	if _, err := publication.Publish(ctx, publication.PgxTx(tx), tables, cur, publication.Next{
		Kind: h.Kind, Token: h.Token, ManifestKey: key, ManifestSHA256: digest, Lineage: h.Lineage,
		StagedAt: snap.ReadAt, StagingBudget: testStagingBudget,
	}); err != nil {
		t.Fatalf("publish empty-flush generation for %s: %v", schema.Name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit empty-flush generation for %s: %v", schema.Name, err)
	}
}

// overwriteS3Object replaces key's bytes in S3 with payload (for testing corrupt
// manifests or tampered parquet objects).
func overwriteS3Object(ctx context.Context, t *testing.T, env *Env, key string, payload []byte) {
	t.Helper()
	if _, err := env.Cluster.S3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(env.Cluster.Bucket), Key: aws.String(key), Body: bytes.NewReader(payload),
	}); err != nil {
		t.Fatalf("overwrite %s: %v", key, err)
	}
}

// accessDeniedPublicationS3 wraps a PublicationS3Client and answers 403
// AccessDenied for GetObject and/or HeadObject calls whose key matches
// keyContains (or all keys when empty), modelling an S3 principal without
// s3:ListBucket or denied object access.
type accessDeniedPublicationS3 struct {
	inner       PublicationS3Client
	denyGet     bool
	denyHead    bool
	keyContains string
}

func (a *accessDeniedPublicationS3) matches(key string) bool {
	return a.keyContains == "" || strings.Contains(key, a.keyContains)
}

func (a *accessDeniedPublicationS3) GetObject(ctx context.Context, in *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if a.denyGet && a.matches(aws.ToString(in.Key)) {
		return nil, &smithy.GenericAPIError{Code: "AccessDenied", Message: "Access Denied"}
	}
	return a.inner.GetObject(ctx, in, optFns...)
}

func (a *accessDeniedPublicationS3) PutObject(ctx context.Context, in *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return a.inner.PutObject(ctx, in, optFns...)
}

func (a *accessDeniedPublicationS3) HeadObject(ctx context.Context, in *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if a.denyHead && a.matches(aws.ToString(in.Key)) {
		return nil, &smithy.GenericAPIError{Code: "AccessDenied", Message: "Access Denied"}
	}
	return a.inner.HeadObject(ctx, in, optFns...)
}
