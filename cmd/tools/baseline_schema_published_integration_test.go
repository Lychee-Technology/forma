package main

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/lychee-technology/forma/internal/testdb"
)

// baseline-schema audits the objects a federated read scans: a PUBLISHED
// record's generation's, addressed as the reader addresses them, and nothing
// for a schema without a record or with an UNPUBLISHED one, whose reads scan
// no parquet.
func TestBaselinePublishedObjectsAreTheGenerationsListedObjects(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool := testdb.Connect(t, ctx)
	entityMain := "bpo_" + uuid.NewString()[:8] + "_entity_main"
	tables := publication.TablesFor(entityMain)
	for _, stmt := range publication.Statements(forma.EnginePostgreSQL, entityMain) {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	t.Cleanup(func() {
		for _, table := range []string{tables.Record, tables.Generation, tables.Epoch, tables.Storage} {
			_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table)
		}
	})
	store := &manifestv2.FSStore{Dir: t.TempDir()}
	layout := manifestv2.Layout{Prefix: "data"}
	const unrecorded, unpublished, published int16 = 1, 2, 3

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, id := range []int16{unpublished, published} {
		if _, err := publication.Initialize(ctx, publication.PgxTx(tx), tables, id); err != nil {
			t.Fatalf("initialize schema %d: %v", id, err)
		}
	}
	cur, err := publication.Read(ctx, publication.PgxTx(tx), tables, published)
	if err != nil {
		t.Fatalf("read schema %d: %v", published, err)
	}
	h, err := manifestv2.Stage(cur.Record, publication.KindInit, uuid.New(), uuid.New())
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	key, err := layout.NewParquetKey(published, cur.Epoch, manifestv2.TierBase)
	if err != nil {
		t.Fatalf("mint a key: %v", err)
	}
	file := writeUpgradeObject(t, t.TempDir(), "orders.parquet", "CAST(7 AS SMALLINT) AS qty")
	m, err := manifestv2.NewInit(h, []manifestv2.File{describeUpgradeObject(t, file, key)})
	if err != nil {
		t.Fatalf("build the manifest: %v", err)
	}
	manifestKey, digest, err := manifestv2.PutManifest(ctx, store, layout, m)
	if err != nil {
		t.Fatalf("put the manifest: %v", err)
	}
	var stagedAt int64
	if err := tx.QueryRow(ctx, "SELECT "+sqlgen.ServerTimeMillisSQL).Scan(&stagedAt); err != nil {
		t.Fatalf("server time: %v", err)
	}
	if _, err := publication.Publish(ctx, publication.PgxTx(tx), tables, cur.Record, publication.Next{
		Kind: publication.KindInit, Token: h.Token, ManifestKey: manifestKey, ManifestSHA256: digest,
		Lineage: h.Lineage, StagedAt: stagedAt, StagingBudget: time.Minute,
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	objects := publishedObjects(pool, tables, store, "bkt")
	for id, want := range map[int16][]string{unrecorded: nil, unpublished: nil, published: {"s3://bkt/" + key}} {
		got, err := objects(ctx, id)
		if err != nil {
			t.Fatalf("objects of schema %d: %v", id, err)
		}
		if len(got) != len(want) || (len(want) > 0 && got[0] != want[0]) {
			t.Fatalf("objects of schema %d: %v, want %v", id, got, want)
		}
	}

	if _, err := pool.Exec(ctx, "DELETE FROM "+tables.Record+" WHERE schema_id = $1", published); err != nil {
		t.Fatalf("drop the record: %v", err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO "+tables.Record+
		" SELECT $1, epoch, state, generation, parent_generation, 'data/elsewhere.json', manifest_sha256, lineage, cas_token, updated_at, gc_horizon FROM "+
		tables.Record+" WHERE schema_id = $2", published, unpublished); err != nil {
		t.Fatalf("plant an invalid record: %v", err)
	}
	if _, err := objects(ctx, published); err == nil {
		t.Fatal("an invalid record must refuse the baseline")
	}
}
