package cdc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/cdcv2"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/s3endpoint"
)

// S3Client is the object API cdc-flush and cdc-init write through
// (docs/publication-protocol-v2.md, "Immutable manifest generations"): every
// parquet object and every manifest is written once with a conditional put
// (If-None-Match: *), and a flush gets its parent manifest. *s3.Client
// satisfies it.
type S3Client interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// manifestCacheEntries bounds a run's cache of verified parent manifests. A
// flush reads one parent per generation, and a run flushes each schema once.
const manifestCacheEntries = 64

// settleTimeout bounds the one in-process attempt to settle a publication
// whose COMMIT went without an answer (cdcv2.Writer.Settle, ResumeInit). It
// runs on a fresh context: the run's own may be what ended.
const settleTimeout = 2 * time.Minute

// ErrBatchSizeTooLarge marks a flush configuration whose batch the V2 writer
// cannot mark on PostgreSQL: a flush generation there is the whole batch,
// marked in its publication's one statement, and a statement binds at most
// cdcv2.MaxMarkEntries entries. Such a batch would stage, then roll back at
// every attempt, so the run is refused before any side effect.
var ErrBatchSizeTooLarge = errors.New("flush batch size exceeds what one PostgreSQL mark statement binds")

// ValidateFlush refuses a flush configuration the V2 writer cannot run, after
// WithDefaults: an unknown engine, and on PostgreSQL a BatchSize above
// cdcv2.MaxMarkEntries. On Aurora DSQL a batch publishes in generations of at
// most cdcv2.DSQLGenerationLimit entries, so any size runs.
func (c CDCConfig) ValidateFlush() error {
	cfg := c.WithDefaults()
	if err := cfg.Engine.Validate(); err != nil {
		return fmt.Errorf("cdc flush database engine %q: %w", cfg.Engine, err)
	}
	if err := cfg.validateLayout(); err != nil {
		return fmt.Errorf("cdc flush: %w", err)
	}
	if cfg.Engine.Resolved() != forma.EngineAuroraDSQL && cfg.BatchSize > cdcv2.MaxMarkEntries {
		return fmt.Errorf("batch size %d: a PostgreSQL flush marks its whole batch in one statement, which binds at most %d entries; lower the batch size: %w",
			cfg.BatchSize, cdcv2.MaxMarkEntries, ErrBatchSizeTooLarge)
	}
	return nil
}

// ValidateInit refuses an init configuration the V2 writer cannot run, after
// WithDefaults: an unknown engine, and a data prefix the reader would refuse.
// RunInit runs it before any AWS, PostgreSQL or DuckDB setup.
func (c CDCConfig) ValidateInit() error {
	cfg := c.WithDefaults()
	if err := cfg.Engine.Validate(); err != nil {
		return fmt.Errorf("cdc init database engine %q: %w", cfg.Engine, err)
	}
	if err := cfg.validateLayout(); err != nil {
		return fmt.Errorf("cdc init: %w", err)
	}
	return nil
}

// validateLayout refuses an S3Prefix holding a glob metacharacter or a `.`
// or `..` segment, as the tools' -s3-prefix flag does
// (forma.ValidateParquetPrefix). Every object key and manifest key the writer
// publishes starts with the prefix, and the federated reader refuses a
// generation listing a path DuckDB would expand or normalize
// (federated.ErrPublishedPathNotConcrete, ErrPublishedPathNotCanonical). A
// writer that staged and committed under such a prefix would mark rows
// flushed in a generation no read can use, so an embedded caller is refused
// here, before any staging.
func (c CDCConfig) validateLayout() error {
	if err := forma.ValidateParquetPrefix("S3Prefix", c.S3Prefix); err != nil {
		return fmt.Errorf("data prefix %q: %w", c.S3Prefix, err)
	}
	return nil
}

// newObjectStore answers the write-once store of the run's bucket over
// client. An AWS SDK client is copied to follow no HTTP redirect
// (s3endpoint.WithoutRedirects), so its objects land at the storage target
// the writer binds; one whose HTTP client may follow redirects on its own is
// refused.
func newObjectStore(client S3Client, bucket string) (manifestv2.Store, error) {
	if client == nil {
		return nil, errors.New("s3 client is required")
	}
	if c, ok := client.(*s3.Client); ok && c == nil {
		return nil, errors.New("s3 client is required")
	}
	if bucket == "" {
		return nil, errors.New("an S3 bucket is required: the V2 writer stages every object and manifest in it")
	}
	if c, ok := client.(*s3.Client); ok {
		bound, err := s3endpoint.WithoutRedirects(c)
		if err != nil {
			return nil, fmt.Errorf("the V2 writer's S3 client: %w", err)
		}
		client = bound
	}
	return &manifestv2.S3Store{Client: client, Bucket: bucket}, nil
}

// resolveS3Client answers the caller's client, or the run's default one when
// the caller gave none (a nil interface, or one holding a nil *s3.Client).
func resolveS3Client(provided S3Client, fallback *s3.Client) S3Client {
	if provided == nil {
		return fallback
	}
	if c, ok := provided.(*s3.Client); ok && c == nil {
		return fallback
	}
	return provided
}

// newPublicationWriter builds the V2 writer of a run over db and store, with
// the layout of cfg.S3Prefix. batchSize is a flush's batch (0 for an init,
// which marks only in handoff chunks), initBatchSize the rows one init export
// snapshot reads and one base object holds (0 for a flush).
func newPublicationWriter(cfg CDCConfig, db *sql.DB, store manifestv2.Store, batchSize, initBatchSize int) (*cdcv2.Writer, error) {
	cache, err := manifestv2.NewCache(store, manifestCacheEntries)
	if err != nil {
		return nil, fmt.Errorf("build the manifest cache: %w", err)
	}
	cfg = cfg.WithDefaults()
	if err := cfg.validateLayout(); err != nil {
		return nil, fmt.Errorf("build the V2 publication writer: %w", err)
	}
	wcfg := cdcv2.Config{
		DB:            db,
		Engine:        cfg.Engine,
		EntityMain:    cfg.EntityMainTable,
		ChangeLog:     cfg.ChangeLogTable,
		Layout:        manifestv2.Layout{Prefix: cfg.S3Prefix},
		Store:         store,
		Cache:         cache,
		BatchSize:     batchSize,
		InitBatchSize: initBatchSize,
	}
	if hook := cfg.PublishFailpoint; hook != nil {
		wcfg.Failpoint = func(ctx context.Context, p cdcv2.Point) error { return hook(ctx, string(p)) }
	}
	w, err := cdcv2.New(wcfg)
	if err != nil {
		return nil, fmt.Errorf("build the V2 publication writer: %w", err)
	}
	return w, nil
}

// readPublicationRecord reads schemaID's publication record as the run loads
// the schema. Its epoch is the pin every publication of the run checks: a
// record of another epoch fails the write closed (cdcv2.ErrEpochChanged). A
// schema without a record is refused here, before any export.
func readPublicationRecord(ctx context.Context, db *sql.DB, entityMain string, schemaID int16) (publication.Record, error) {
	if entityMain == "" {
		entityMain = "entity_main"
	}
	snap, err := publication.Read(ctx, publication.DB(db), publication.TablesFor(entityMain), schemaID)
	if errors.Is(err, publication.ErrNoRecord) {
		return publication.Record{}, fmt.Errorf("schema %d has no publication record; init-db writes it when it registers the schema "+
			"or recovers a lost record, and nothing of the schema is published until then: %w", schemaID, err)
	}
	if err != nil {
		return publication.Record{}, fmt.Errorf("load the publication record of schema %d: %w", schemaID, err)
	}
	return snap.Record, nil
}

// ErrRegistryEpochsRequired refuses a run whose caller gave no
// CDCConfig.RegistryEpochs. The run's schema registry is loaded before the
// run starts, so an epoch the run read itself could already be a re-created
// schema's, paired with the dropped schema's metadata the registry holds.
var ErrRegistryEpochsRequired = errors.New("CDCConfig.RegistryEpochs is required: the publication epochs read before the schema registry loaded")

// runEpochs answers the epochs the run's schema metadata belongs to:
// cfg.RegistryEpochs, which the caller read before it loaded the run's
// registry (ReadRegistryEpochs). A run without them is refused: it cannot
// tell what epoch an already-loaded registry belongs to.
func runEpochs(cfg CDCConfig) (map[int16]int64, error) {
	if cfg.RegistryEpochs == nil {
		return nil, fmt.Errorf("read them with ReadRegistryEpochs before loading the schema registry the run is given: %w", ErrRegistryEpochsRequired)
	}
	return cfg.RegistryEpochs, nil
}

// ReadRegistryEpochs reads every schema's publication epoch on db, for
// CDCConfig.RegistryEpochs: call it before loading the schema registry a run
// is given. entityMain names the publication tables ("entity_main" when
// empty).
func ReadRegistryEpochs(ctx context.Context, db *sql.DB, entityMain string) (map[int16]int64, error) {
	if entityMain == "" {
		entityMain = "entity_main"
	}
	epochs, err := publication.Epochs(ctx, publication.DB(db), publication.TablesFor(entityMain))
	if err != nil {
		return nil, fmt.Errorf("read the publication epochs before loading the schema registry: %w", err)
	}
	return epochs, nil
}

// refuseMovedEpoch refuses a schema whose record, read under its schema lock,
// is not at the epoch read before the run's metadata was loaded: the schema
// was dropped, or dropped and registered again, since, and the metadata may
// describe the dropped schema rather than the record's.
func refuseMovedEpoch(rec publication.Record, epochs map[int16]int64) error {
	was, ok := epochs[rec.SchemaID]
	if ok && was == rec.Epoch {
		return nil
	}
	before := "no record"
	if ok {
		before = fmt.Sprintf("epoch %d", was)
	}
	return fmt.Errorf("schema %d: its record is at epoch %d, and the run's schema metadata was loaded at %s: "+
		"the schema was dropped or registered again since, so the metadata may be the dropped schema's; "+
		"load the schema registry again and rerun: %w", rec.SchemaID, rec.Epoch, before, cdcv2.ErrEpochChanged)
}

// refuseUnflushable refuses a flush of a schema whose record no flush may
// move, with what the operator does about it. The writer refuses the same
// states itself; this names them before the run takes the lock or exports.
func refuseUnflushable(rec publication.Record) error {
	switch rec.State {
	case publication.AwaitingInit:
		return fmt.Errorf("schema %d at epoch %d awaits a cdc-init: run cdc-init for it; until it publishes, "+
			"cdc-flush exports nothing of the schema, its rows stay unflushed and federated reads of it fail closed: %w",
			rec.SchemaID, rec.Epoch, publication.ErrAwaitingInit)
	case publication.Dropped:
		return fmt.Errorf("schema %d at epoch %d is dropped; nothing of it is flushed until it is registered again: %w",
			rec.SchemaID, rec.Epoch, publication.ErrDropped)
	}
	return nil
}

// explainWriterError adds to a V2 writer error what it means for the
// operator, for the refusals that need an action of theirs.
func explainWriterError(tool string, schemaID int16, err error) error {
	switch {
	case errors.Is(err, publication.ErrAwaitingInit):
		return fmt.Errorf("schema %d awaits a cdc-init; run cdc-init for it: %w", schemaID, err)
	case errors.Is(err, cdcv2.ErrEpochChanged):
		return fmt.Errorf("schema %d was dropped, or dropped and registered again, while %s ran; nothing it staged is visible, run it again: %w", schemaID, tool, err)
	case errors.Is(err, publication.ErrDropped):
		return fmt.Errorf("schema %d is dropped; nothing of it is published until it is registered again: %w", schemaID, err)
	}
	return err
}

// unresolvedNote describes a publication whose outcome stayed unknown after
// the writer and one in-process settling tried: the generation it staged,
// which the record shows if it committed, and what stays as it is.
func unresolvedNote(u *cdcv2.UnresolvedError) string {
	return fmt.Sprintf("the %s staged generation %d of schema %d at epoch %d with token %s (manifest %s), "+
		"and whether its COMMIT landed is unknown; the record names that token if it did",
		u.Kind, u.Cur.Generation+1, u.Cur.SchemaID, u.Cur.Epoch, u.Next.Token, u.Next.ManifestKey)
}
