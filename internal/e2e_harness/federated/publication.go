package federated

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"

	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// The harness's side of Protocol V2 (docs/publication-protocol-v2.md). The
// harness registers a schema the way init-db does, its registry row and its
// UNPUBLISHED publication record in one transaction (registerSchema), and
// PublishTiers publishes parquet as cdc-init and cdc-flush do: objects under
// fresh canonical keys, an immutable manifest that describes every one of
// them, then the record's compare-and-set. The product reader then resolves
// exactly those objects from the record in its pass snapshot
// (PublicationReader), with no glob and no hint.
//
// Only PublishTiers publishes. The harness's simulated flush and compaction
// (RunCDCFlush, RunCompaction) and WriteParquet keep writing the V1-shaped
// {prefix}/{schema}/{base|delta}/ layout that the harness's own DuckDB oracle
// reads with globs, and PublishTiers writes its rows there too; nothing in
// the product reads that layout.
//
// This file is named in internal/publication's owners guard
// (TestOnlyTheProtocolOwnersMoveAPublicationRecord) as an owner of
// Initialize and Publish: the harness stands in for init-db's registration
// and for the V2 writers, over synthetic parquet that has no PostgreSQL rows
// for internal/cdcv2's writer to export.

// publicationStagingBudget is W of the harness's publications: how long after
// its staging began a publication may still commit. No collector runs
// against the harness, so it only has to outlast the uploads.
const publicationStagingBudget = time.Hour

// publicationTables names the harness's publication tables.
func (h *FederatedTestHarness) publicationTables() publication.Tables {
	return publication.TablesFor(h.tables.EntityMain)
}

// objectLayout builds the harness's V2 object keys under its S3 prefix.
func (h *FederatedTestHarness) objectLayout() manifestv2.Layout {
	return manifestv2.Layout{Prefix: h.S3Prefix}
}

// objectStore writes the harness's V2 objects once each, and reads them back.
func (h *FederatedTestHarness) objectStore() *manifestv2.S3Store {
	return &manifestv2.S3Store{Client: h.s3Client, Bucket: h.S3Bucket}
}

// registerSchema writes schemaID's registry row and, in the same
// transaction, its UNPUBLISHED publication record, as init-db's registration
// does. A record that exists is left as it is, so registering a schema again
// never resets what it published.
func (h *FederatedTestHarness) registerSchema(ctx context.Context, schemaID int16, schemaName string) error {
	tx, err := h.PGDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin the registration of schema %d: %w", schemaID, err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO schema_registry (schema_id, schema_name) VALUES ($1, $2) ON CONFLICT (schema_id) DO UPDATE SET schema_name = $2`,
		schemaID, schemaName); err != nil {
		return fmt.Errorf("register schema %d (%s): %w", schemaID, schemaName, err)
	}
	if _, err := publication.Initialize(ctx, publication.SQLTx(tx), h.publicationTables(), schemaID); err != nil {
		return fmt.Errorf("initialize the publication record of schema %d: %w", schemaID, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit the registration of schema %d: %w", schemaID, err)
	}
	return nil
}

// retirePublishedGenerations replaces the current generation of every
// PUBLISHED schema with an empty init generation, before ClearAllData
// deletes the objects under the harness's prefix, and answers the keys of
// the empty generations' manifests, which the reset keeps: a record never
// points at a manifest or an object the reset deleted. An empty init
// generation says the parquet tiers hold nothing, so a schema the next
// dataset leaves without parquet reads from PostgreSQL alone, and
// PublishTiers replaces it for one that has parquet.
func (h *FederatedTestHarness) retirePublishedGenerations(ctx context.Context) (map[string]bool, error) {
	keep := map[string]bool{}
	epochs, err := publication.Epochs(ctx, publication.DB(h.PGDB), h.publicationTables())
	if err != nil {
		return nil, fmt.Errorf("list the publication records to retire: %w", err)
	}
	for schemaID := range epochs {
		snap, err := publication.Read(ctx, publication.DB(h.PGDB), h.publicationTables(), schemaID)
		if err != nil {
			return nil, fmt.Errorf("read the publication record of schema %d: %w", schemaID, err)
		}
		if snap.State != publication.Published {
			continue
		}
		stagedAt, err := h.databaseMillis(ctx)
		if err != nil {
			return nil, err
		}
		empty, err := h.publish(ctx, snap.Record, stagedAt, publication.KindInit, uuid.New(),
			func(hdr manifestv2.Header) (manifestv2.Manifest, error) { return manifestv2.NewInit(hdr, nil) })
		if err != nil {
			return nil, fmt.Errorf("replace the published generation of schema %d before the reset deletes its objects: %w", schemaID, err)
		}
		keep[empty.record.ManifestKey] = true
	}
	return keep, nil
}

// publishedGeneration is a generation the harness committed: the record the
// compare-and-set answered and the manifest it names.
type publishedGeneration struct {
	record   publication.Record
	manifest manifestv2.Manifest
}

// PublishTiers publishes schemaID's parquet as V2 generations: an init
// generation of base's rows, which replaces whatever the schema published
// before, then, when delta has rows, a flush generation that adds delta's
// rows. Each tier's rows go to one object under a fresh canonical key. The
// schema must be registered (SetupSchema). No change_log entry is marked:
// the rows are synthetic parquet history with no PostgreSQL entry.
//
// The same rows are also written to the V1-shaped {base|delta}/ layout
// (writeOracleTiers), which the harness's own DuckDB query path
// (ExecuteFederatedQuery, the benchmark's truth pass) reads with globs, so
// that independent oracle sees what the product reader scans.
func (h *FederatedTestHarness) PublishTiers(ctx context.Context, schemaID int16, base, delta []TestRecord) error {
	if h.s3Disabled {
		return fmt.Errorf("S3 is disabled (simulated failure)")
	}
	if err := h.writeOracleTiers(ctx, schemaID, base, delta); err != nil {
		return err
	}
	snap, err := publication.Read(ctx, publication.DB(h.PGDB), h.publicationTables(), schemaID)
	if err != nil {
		return fmt.Errorf("read the publication record of schema %d: %w", schemaID, err)
	}
	stagedAt, err := h.databaseMillis(ctx)
	if err != nil {
		return err
	}
	baseFiles, err := h.stageObjects(ctx, schemaID, snap.Epoch, manifestv2.TierBase, base)
	if err != nil {
		return err
	}
	initGen, err := h.publish(ctx, snap.Record, stagedAt, publication.KindInit, uuid.New(),
		func(hdr manifestv2.Header) (manifestv2.Manifest, error) { return manifestv2.NewInit(hdr, baseFiles) })
	if err != nil || len(delta) == 0 {
		return err
	}
	if stagedAt, err = h.databaseMillis(ctx); err != nil {
		return err
	}
	deltaFiles, err := h.stageObjects(ctx, schemaID, initGen.record.Epoch, manifestv2.TierDelta, delta)
	if err != nil {
		return err
	}
	_, err = h.publish(ctx, initGen.record, stagedAt, publication.KindFlush, initGen.record.Lineage,
		func(hdr manifestv2.Header) (manifestv2.Manifest, error) {
			return manifestv2.NewFlush(&initGen.manifest, hdr, deltaFiles)
		})
	return err
}

// writeOracleTiers writes base and delta, each when it has rows, as one
// parquet file per tier under schemaID's V1-shaped {base|delta}/ layout,
// the harness oracle's input. Nothing in the product reads it.
func (h *FederatedTestHarness) writeOracleTiers(ctx context.Context, schemaID int16, base, delta []TestRecord) error {
	previous := h.SchemaID
	h.SchemaID = schemaID
	defer func() { h.SchemaID = previous }()
	for _, tier := range []struct {
		name    string
		records []TestRecord
	}{{"base", base}, {"delta", delta}} {
		if len(tier.records) == 0 {
			continue
		}
		if err := h.WriteParquet(ctx, tier.name, fmt.Sprintf("oracle_%s_%d.parquet", tier.name, schemaID), tier.records); err != nil {
			return fmt.Errorf("write the oracle's %s parquet of schema %d: %w", tier.name, schemaID, err)
		}
	}
	return nil
}

// publish stages the manifest build answers for the generation after cur
// and commits it with the record's compare-and-set.
func (h *FederatedTestHarness) publish(ctx context.Context, cur publication.Record, stagedAt int64, kind publication.Kind, lineage uuid.UUID,
	build func(manifestv2.Header) (manifestv2.Manifest, error)) (publishedGeneration, error) {
	token := uuid.New()
	hdr, err := manifestv2.Stage(cur, kind, lineage, token)
	if err != nil {
		return publishedGeneration{}, fmt.Errorf("stage a %s of schema %d: %w", kind, cur.SchemaID, err)
	}
	m, err := build(hdr)
	if err != nil {
		return publishedGeneration{}, fmt.Errorf("build the %s manifest of schema %d: %w", kind, cur.SchemaID, err)
	}
	key, digest, err := manifestv2.PutManifest(ctx, h.objectStore(), h.objectLayout(), m)
	if err != nil {
		return publishedGeneration{}, fmt.Errorf("stage the %s manifest of schema %d: %w", kind, cur.SchemaID, err)
	}
	tx, err := h.PGDB.BeginTx(ctx, nil)
	if err != nil {
		return publishedGeneration{}, fmt.Errorf("begin the %s publication of schema %d: %w", kind, cur.SchemaID, err)
	}
	defer func() { _ = tx.Rollback() }()
	out, err := publication.Publish(ctx, publication.SQLTx(tx), h.publicationTables(), cur, publication.Next{
		Kind: kind, Token: token, ManifestKey: key, ManifestSHA256: digest, Lineage: lineage,
		StagedAt: stagedAt, StagingBudget: publicationStagingBudget,
	})
	if err != nil {
		return publishedGeneration{}, fmt.Errorf("publish the %s generation of schema %d: %w", kind, cur.SchemaID, err)
	}
	if err := tx.Commit(); err != nil {
		return publishedGeneration{}, fmt.Errorf("commit the %s generation of schema %d: %w", kind, cur.SchemaID, err)
	}
	return publishedGeneration{record: out, manifest: m}, nil
}

// databaseMillis reads the database clock, the clock a publication's staging
// time is measured on.
func (h *FederatedTestHarness) databaseMillis(ctx context.Context) (int64, error) {
	var ms int64
	if err := h.PGDB.QueryRowContext(ctx, "SELECT "+sqlgen.ServerTimeMillisSQL).Scan(&ms); err != nil {
		return 0, fmt.Errorf("read the database clock: %w", err)
	}
	return ms, nil
}

// stageObjects writes records, when there are any, to one parquet object of
// tier under a fresh canonical key, once, and answers its manifest entry.
func (h *FederatedTestHarness) stageObjects(ctx context.Context, schemaID int16, epoch int64, tier manifestv2.Tier, records []TestRecord) ([]manifestv2.File, error) {
	if len(records) == 0 {
		return nil, nil
	}
	path, err := h.writeLocalParquet(ctx, fmt.Sprintf("publish_%s.parquet", uuid.NewString()), records)
	if err != nil {
		return nil, fmt.Errorf("write the %s parquet of schema %d: %w", tier, schemaID, err)
	}
	defer func() { _ = os.Remove(path) }()
	file, err := h.describeParquet(ctx, path)
	if err != nil {
		return nil, err
	}
	if file.Key, err = h.objectLayout().NewParquetKey(schemaID, epoch, tier); err != nil {
		return nil, fmt.Errorf("name the %s object of schema %d: %w", tier, schemaID, err)
	}
	file.Tier = tier
	body, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open the %s parquet of schema %d: %w", tier, schemaID, err)
	}
	defer body.Close()
	if err := h.objectStore().Put(ctx, file.Key, body); err != nil {
		return nil, fmt.Errorf("stage the %s object of schema %d: %w", tier, schemaID, err)
	}
	return []manifestv2.File{file}, nil
}

// PublishedFiles answers the objects schemaID's current generation lists,
// from its verified manifest. A schema that has published nothing fails.
func (h *FederatedTestHarness) PublishedFiles(ctx context.Context, schemaID int16) ([]manifestv2.File, error) {
	snap, err := publication.Read(ctx, publication.DB(h.PGDB), h.publicationTables(), schemaID)
	if err != nil {
		return nil, fmt.Errorf("read the publication record of schema %d: %w", schemaID, err)
	}
	if snap.State != publication.Published {
		return nil, fmt.Errorf("schema %d is %s, not %s", schemaID, snap.State, publication.Published)
	}
	ref, err := manifestv2.RecordRef(snap.Record)
	if err != nil {
		return nil, fmt.Errorf("reference the manifest of schema %d: %w", schemaID, err)
	}
	m, err := manifestv2.Fetch(ctx, h.objectStore(), ref)
	if err != nil {
		return nil, fmt.Errorf("fetch the manifest of schema %d generation %d: %w", schemaID, snap.Generation, err)
	}
	return m.Files, nil
}

// ObjectExists answers whether the harness's bucket holds key.
func (h *FederatedTestHarness) ObjectExists(ctx context.Context, key string) (bool, error) {
	exists, err := manifestv2.S3Exists(h.s3Client, h.S3Bucket)(ctx, key)
	if err != nil {
		return false, fmt.Errorf("probe object %s: %w", key, err)
	}
	return exists, nil
}
