// Package federatedv2 is the federated reader's Protocol V2 source
// (docs/publication-protocol-v2.md, "Readers"): it reads a schema's
// publication record in a pass's snapshot, refuses a record the reader must
// fail closed on, and answers the objects the record's generation lists, from
// its verified manifest. The federated engine calls it through
// federated.PublicationReader (WithPublicationReader).
//
// Nothing outside this package calls it until the writers and the reader move
// onto the publication record together (#896): the guard in
// internal/publication (TestOnlyInitDBDDLReachesThePublicationTables) refuses
// a production import of it from anywhere else.
package federatedv2

import (
	"context"
	"errors"
	"fmt"
	"maps"

	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// ErrEpochChanged reports a record at another epoch than the one the process
// pinned when it loaded the schema's metadata: the schema was dropped, and
// maybe created again, since.
var ErrEpochChanged = errors.New("publication epoch differs from the process's pin")

// ErrNotPinned reports a schema the process holds no epoch pin for. A reader
// that took the record's epoch instead would serve a re-created schema with
// the dropped one's metadata.
var ErrNotPinned = errors.New("no epoch pinned for the schema")

// Config is what a Source reads with.
type Config struct {
	// EntityMain names the publication tables (publication.TablesFor).
	EntityMain string
	// Manifests fetches and verifies manifests, once per process for each
	// key (manifestv2.Cache).
	Manifests *manifestv2.Cache
	// Epochs are the process's epoch pins: each schema's epoch as read when
	// the process loaded its metadata. A schema without one fails closed.
	Epochs map[int16]int64
	// ObjectURI is how DuckDB addresses a listed key: S3URI for a bucket.
	ObjectURI func(key string) string
	// Exists probes one key (HeadObject on S3). It runs only after a failed
	// read, to tell a lost object from any other failure.
	Exists func(ctx context.Context, key string) (bool, error)
}

// S3URI addresses the keys of bucket as s3:// URIs.
func S3URI(bucket string) func(string) string {
	return func(key string) string { return "s3://" + bucket + "/" + key }
}

// Source implements federated.PublicationReader.
type Source struct {
	tables    publication.Tables
	manifests *manifestv2.Cache
	epochs    map[int16]int64
	uri       func(string) string
	exists    func(context.Context, string) (bool, error)
}

var _ federated.PublicationReader = (*Source)(nil)

// New answers a Source over cfg. It copies the pins.
func New(cfg Config) (*Source, error) {
	if cfg.EntityMain == "" || cfg.Manifests == nil || cfg.ObjectURI == nil || cfg.Exists == nil {
		return nil, errors.New("a V2 federated source needs the entity_main table, a manifest cache, an object URI and an existence probe")
	}
	return &Source{
		tables: publication.TablesFor(cfg.EntityMain), manifests: cfg.Manifests,
		epochs: maps.Clone(cfg.Epochs), uri: cfg.ObjectURI, exists: cfg.Exists,
	}, nil
}

// ReadRecord reads schemaID's record on tx, the pass's snapshot, and refuses
// a record the reader must fail closed on: absent, AWAITING_INIT, DROPPED, or
// at an epoch the process did not pin. Each refusal is a
// *federated.PublicationConsistencyError; a failed read is a plain error.
func (s *Source) ReadRecord(ctx context.Context, tx pgx.Tx, schemaID int16) (federated.PublicationRecord, error) {
	snap, err := publication.Read(ctx, publication.PgxTx(tx), s.tables, schemaID)
	if errors.Is(err, publication.ErrNoRecord) {
		return federated.PublicationRecord{}, inconsistent(schemaID, 0, 0, err)
	}
	if err != nil {
		return federated.PublicationRecord{}, fmt.Errorf("read the publication record of schema %d: %w", schemaID, err)
	}
	r := snap.Record
	if err := r.Readable(); err != nil {
		return federated.PublicationRecord{}, inconsistent(schemaID, r.Epoch, r.Generation, err)
	}
	pin, ok := s.epochs[schemaID]
	if !ok {
		return federated.PublicationRecord{}, inconsistent(schemaID, r.Epoch, r.Generation, ErrNotPinned)
	}
	if r.Epoch != pin {
		return federated.PublicationRecord{}, inconsistent(schemaID, r.Epoch, r.Generation,
			fmt.Errorf("the record is at epoch %d and the process pinned epoch %d: %w", r.Epoch, pin, ErrEpochChanged))
	}
	return federated.PublicationRecord{
		SchemaID: schemaID, Epoch: r.Epoch, Generation: r.Generation,
		Published: r.State == publication.Published, Detail: r,
	}, nil
}

// ResolveGeneration fetches a published record's manifest through the cache,
// verified against what the record committed, and answers the objects it
// lists. A missing or invalid manifest is a
// *federated.PublicationConsistencyError; a failed fetch is a plain error.
func (s *Source) ResolveGeneration(ctx context.Context, rec federated.PublicationRecord) (federated.PublishedGeneration, error) {
	r, ok := rec.Detail.(publication.Record)
	if !ok || !rec.Published || r.SchemaID != rec.SchemaID || r.Epoch != rec.Epoch || r.Generation != rec.Generation {
		return federated.PublishedGeneration{}, fmt.Errorf("schema %d epoch %d generation %d: not a published record this source read", rec.SchemaID, rec.Epoch, rec.Generation)
	}
	ref, err := manifestv2.RecordRef(r)
	if err != nil {
		return federated.PublishedGeneration{}, fmt.Errorf("reference the manifest of schema %d generation %d: %w", r.SchemaID, r.Generation, err)
	}
	m, err := s.manifests.Get(ctx, ref)
	var ce *manifestv2.ConsistencyError
	if errors.As(err, &ce) {
		return federated.PublishedGeneration{}, inconsistent(r.SchemaID, r.Epoch, r.Generation, err)
	}
	if err != nil {
		return federated.PublishedGeneration{}, fmt.Errorf("fetch the manifest of schema %d generation %d: %w", r.SchemaID, r.Generation, err)
	}
	gen := federated.PublishedGeneration{
		SchemaID: r.SchemaID, Epoch: r.Epoch, Generation: r.Generation,
		Paths:   make([]string, 0, len(m.Files)),
		Keys:    make([]string, 0, len(m.Files)),
		Columns: make(map[string]map[string]string, len(m.Files)),
	}
	for _, f := range m.Files {
		uri := s.uri(f.Key)
		gen.Paths = append(gen.Paths, uri)
		gen.Keys = append(gen.Keys, f.Key)
		gen.Columns[uri] = f.Columns
	}
	return gen, nil
}

// MissingIn probes the objects of gen among scanned and answers the keys the
// store does not hold. A path gen does not list is not probed: its absence
// would prove nothing about the generation.
func (s *Source) MissingIn(ctx context.Context, gen federated.PublishedGeneration, scanned []string) ([]string, error) {
	keys := make(map[string]string, len(gen.Paths))
	for i, p := range gen.Paths {
		keys[p] = gen.Keys[i]
	}
	var missing []string
	for _, p := range scanned {
		key, listed := keys[p]
		if !listed {
			continue
		}
		ok, err := s.exists(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("probe object %s of schema %d generation %d: %w", key, gen.SchemaID, gen.Generation, err)
		}
		if !ok {
			missing = append(missing, key)
		}
	}
	return missing, nil
}

func inconsistent(schemaID int16, epoch, generation int64, err error) error {
	return &federated.PublicationConsistencyError{SchemaID: schemaID, Epoch: epoch, Generation: generation, Err: err}
}
