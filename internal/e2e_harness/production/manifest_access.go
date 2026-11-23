package production

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// The test files of this package reach the published file inventory only
// through this file, publication_state.go and the fault helpers of
// publication_faults_test.go; no e2e scenario imports internal/manifestv2 or
// internal/publication (#901). Under Protocol V2 (docs/publication-protocol-v2.md)
// a schema's inventory is the immutable manifest of the generation its
// publication record names: a test observes it through loadSchemaManifest,
// TierEntries and the entries they answer, compares publications through
// publication_state.go and stages a fault by publishing a test-made
// generation (publication_faults_test.go). It never writes a record or a
// manifest itself.

// Manifest is a harness-side view of the generation a schema's publication
// record names: the record's identity of it and the files its verified
// manifest lists.
type Manifest struct {
	SchemaID int16
	Epoch    int64
	// Generation is the committed generation's number in its epoch, and
	// ParentGeneration the one it replaced (0 for the first).
	Generation, ParentGeneration int64
	Kind                         publication.Kind
	Lineage                      uuid.UUID
	Token                        uuid.UUID
	// Key and SHA256 name the manifest object, as the record committed them.
	Key, SHA256 string
	Files       []ManifestEntry
}

// ManifestEntry is one parquet object a generation lists: Path is its
// bucket-relative object key and Checksum its SHA-256 as 64 lowercase hex
// digits.
type ManifestEntry struct {
	Tier               string
	Path               string
	RowIDMin, RowIDMax string
	SizeBytes          int64
	RowCount           int64
	Checksum           string
	Columns            map[string]string
}

// TierEntries returns the entries m lists on tier ("base" or "delta").
func TierEntries(m *Manifest, tier string) []ManifestEntry {
	var out []ManifestEntry
	for _, f := range m.Files {
		if f.Tier == tier {
			out = append(out, f)
		}
	}
	return out
}

// loadSchemaManifest loads the generation the schema's record names, failing
// the test when the schema has not published.
func loadSchemaManifest(ctx context.Context, t *testing.T, env *Env, schema SchemaRef) *Manifest {
	t.Helper()
	rec, err := env.readRecord(ctx, schema.ID)
	if err != nil {
		t.Fatalf("read publication record for schema %d (%s): %v", schema.ID, schema.Name, err)
	}
	m, err := manifestView(ctx, env.manifestStore(), rec)
	if err != nil {
		t.Fatalf("load manifest for schema %d (%s): %v", schema.ID, schema.Name, err)
	}
	if m == nil {
		t.Fatalf("schema %d (%s) has no published generation", schema.ID, schema.Name)
	}
	return m
}

// loadManifests answers the published generation of every fixture schema
// whose record is PUBLISHED, read in one snapshot. A schema whose record is
// UNPUBLISHED is absent from the map; a missing record, one that is
// AWAITING_INIT or DROPPED, and a manifest that is missing or fails
// verification surface as errors, since a reader would fail closed on each.
func (e *Env) loadManifests(ctx context.Context) (map[int16]*Manifest, error) {
	records, err := e.readRecords(ctx, fixtureSchemaIDs())
	if err != nil {
		return nil, err
	}
	store := e.manifestStore()
	manifests := make(map[int16]*Manifest)
	for _, rec := range records {
		m, err := manifestView(ctx, store, rec)
		if err != nil {
			return nil, err
		}
		if m != nil {
			manifests[rec.SchemaID] = m
		}
	}
	return manifests, nil
}

// readRecords reads the records of ids in one REPEATABLE READ snapshot.
func (e *Env) readRecords(ctx context.Context, ids []int16) ([]publication.Record, error) {
	tx, err := e.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("open the snapshot the publication records are read in: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	tables := publication.TablesFor(e.Tables.EntityMain)
	records := make([]publication.Record, 0, len(ids))
	for _, id := range ids {
		snap, err := publication.Read(ctx, publication.PgxTx(tx), tables, id)
		if err != nil {
			return nil, fmt.Errorf("read the publication record of schema %d: %w", id, err)
		}
		records = append(records, snap.Record)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("end the snapshot the publication records were read in: %w", err)
	}
	return records, nil
}

// readRecord reads one schema's publication record.
func (e *Env) readRecord(ctx context.Context, schemaID int16) (publication.Record, error) {
	records, err := e.readRecords(ctx, []int16{schemaID})
	if err != nil {
		return publication.Record{}, err
	}
	return records[0], nil
}

// manifestView answers the view of rec's generation, fetched from store and
// verified against what the record committed, or nil when rec is
// UNPUBLISHED. Only UNPUBLISHED reads as "nothing published": a published
// generation whose manifest is missing is loss, never an empty inventory
// (#464).
func manifestView(ctx context.Context, store manifestv2.Store, rec publication.Record) (*Manifest, error) {
	if err := rec.Readable(); err != nil {
		return nil, fmt.Errorf("schema %d's record at epoch %d: %w", rec.SchemaID, rec.Epoch, err)
	}
	if rec.State == publication.Unpublished {
		return nil, nil
	}
	ref, err := manifestv2.RecordRef(rec)
	if err != nil {
		return nil, fmt.Errorf("name schema %d's generation: %w", rec.SchemaID, err)
	}
	doc, err := manifestv2.Fetch(ctx, store, ref)
	if err != nil {
		return nil, fmt.Errorf("fetch schema %d's generation %d manifest %s: %w", rec.SchemaID, rec.Generation, rec.ManifestKey, err)
	}
	m := &Manifest{
		SchemaID: rec.SchemaID, Epoch: rec.Epoch, Generation: rec.Generation, ParentGeneration: rec.ParentGeneration, Kind: doc.Kind,
		Lineage: rec.Lineage, Token: rec.Token, Key: rec.ManifestKey, SHA256: rec.ManifestSHA256,
		Files: make([]ManifestEntry, 0, len(doc.Files)),
	}
	for _, f := range doc.Files {
		m.Files = append(m.Files, ManifestEntry{
			Tier: string(f.Tier), Path: f.Key,
			RowIDMin: f.RowIDMin.String(), RowIDMax: f.RowIDMax.String(),
			SizeBytes: f.SizeBytes, RowCount: f.RowCount, Checksum: f.SHA256,
			Columns: copyColumns(f.Columns),
		})
	}
	return m, nil
}

// fetchGenerationKeys fetches the manifest of m's generation from the store
// afresh, past any reader's cache, verifies it against the identity and
// digest its record committed, and answers the keys it lists. A generation
// the record no longer names stays fetchable until garbage collection
// retires it (#891).
func (e *Env) fetchGenerationKeys(ctx context.Context, m *Manifest) ([]string, error) {
	ref := manifestv2.Ref{
		SchemaID: m.SchemaID, Epoch: m.Epoch, Generation: m.Generation, ParentGeneration: m.ParentGeneration,
		Token: m.Token, Lineage: m.Lineage, Kind: m.Kind, ManifestKey: m.Key, ManifestSHA256: m.SHA256,
	}
	doc, err := manifestv2.Fetch(ctx, e.manifestStore(), ref)
	if err != nil {
		return nil, fmt.Errorf("fetch schema %d's generation %d manifest %s: %w", m.SchemaID, m.Generation, m.Key, err)
	}
	return doc.Keys(), nil
}

// manifestStore is the write-once store the Env's manifests live in.
func (e *Env) manifestStore() manifestv2.Store {
	return &manifestv2.S3Store{Client: clusterS3Client{e.Cluster}, Bucket: e.Cluster.Bucket}
}

// manifestLayout is the V2 object layout of the Env's data prefix, the one
// cdc-flush and cdc-init write under (CDCConfig.S3Prefix).
func (e *Env) manifestLayout() manifestv2.Layout {
	return manifestv2.Layout{Prefix: e.CDC.S3Prefix}
}

// manifestKeyPrefix answers the key prefix every V2 manifest of schemaID
// lives under, whatever its epoch and generation, for a test that faults the
// manifest put alone.
func (e *Env) manifestKeyPrefix(schemaID int16) string {
	key := e.manifestLayout().ManifestKey(schemaID, 1, 1, uuid.Nil)
	return key[:strings.LastIndex(key, "/e1/")+1]
}

// fixtureSchemaIDs answers the ids of the bundled fixture schemas.
func fixtureSchemaIDs() []int16 {
	refs := DefaultSchemaFixtures()
	ids := make([]int16, 0, len(refs))
	for _, ref := range refs {
		ids = append(ids, ref.ID)
	}
	return ids
}

func copyColumns(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
