package factory

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/bootstrap"
	"github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/federatedv2"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/schemameta"
	"go.uber.org/zap"
)

// manifestCacheEntries bounds the process-wide cache of verified manifests:
// one entry per generation a pass reads, so it holds a few generations of
// each schema. A miss costs one GET of an immutable object.
const manifestCacheEntries = 1024

// publicationS3Client is the S3 surface the publication reader needs: GET for
// manifests, HEAD for the missing-object probe. PutObject is part of the
// manifest store's interface and is never called by a reader.
type publicationS3Client interface {
	manifestv2.S3Client
	manifestv2.S3HeadClient
}

// newPublicationS3Client is the seam for the S3 client behind the publication
// reader. Tests swap it to keep factory construction hermetic; production
// always resolves the real credential chain.
var newPublicationS3Client = func(ctx context.Context, cfg forma.DuckDBConfig) (publicationS3Client, error) {
	client, err := bootstrap.NewS3Client(ctx, bootstrap.S3Options{
		Region:    cfg.S3Region,
		Endpoint:  cfg.S3Endpoint,
		AccessKey: cfg.S3AccessKey,
		SecretKey: cfg.S3SecretKey,
	})
	if err != nil {
		return nil, fmt.Errorf("build s3 client: %w", err)
	}
	return client, nil
}

var _ publicationS3Client = (*s3.Client)(nil)

// snapshotPool opens the snapshot the epoch pins are read in.
// *pgxpool.Pool satisfies it.
type snapshotPool interface {
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
}

// publicationReaderSpec is what the factory builds the publication reader
// from.
type publicationReaderSpec struct {
	duck forma.DuckDBConfig
	pool snapshotPool
	meta *schemameta.MetadataCache
	// metadataEpochs are the records' epochs as read before meta was loaded
	// (loadMetadataWithEpochs); nil when they were not read.
	metadataEpochs map[int16]int64
	// registryEpochs are the records' epochs as read before the caller's
	// schema registry loaded (forma.Config.SchemaRegistryEpochs). The reader
	// refuses to start without them: the registry resolves the schemas, their
	// sort attributes and their decoding for every query, and it loaded
	// before the factory read anything.
	registryEpochs map[int16]int64
	// entityMain names the publication tables (publication.TablesFor).
	entityMain string
}

// loadMetadataWithEpochs loads the schema metadata after reading every
// publication record's epoch, when the federated reader will be built. A
// drop or a registration of a dropped id moves a record to its next epoch,
// so a schema whose epoch is the same before the load and when the reader
// pins it was neither dropped nor registered again in between: the metadata
// loaded belongs to the pinned epoch (bindEpochPins).
func loadMetadataWithEpochs(ctx context.Context, loader metadataLoader, duck forma.DuckDBConfig, pool snapshotPool, entityMain string) (*schemameta.MetadataCache, map[int16]int64, error) {
	var before map[int16]int64
	if duck.Enabled && pool != nil {
		var err error
		if before, err = readPublicationEpochs(ctx, pool, entityMain); err != nil {
			return nil, nil, err
		}
	}
	meta, err := loader.LoadMetadata(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load metadata: %w", err)
	}
	return meta, before, nil
}

// readPublicationEpochs reads every record's epoch in one REPEATABLE READ
// READ ONLY snapshot.
func readPublicationEpochs(ctx context.Context, pool snapshotPool, entityMain string) (map[int16]int64, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("failed to open the snapshot the publication epochs are read in: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	epochs, err := publication.Epochs(ctx, publication.PgxTx(tx), publication.TablesFor(entityMain))
	if err != nil {
		return nil, fmt.Errorf("failed to read the publication epochs before loading the schema metadata: %w", err)
	}
	return epochs, nil
}

// bindEpochPins keeps the pins whose epoch is the one each of befores read:
// before the factory's metadata loaded, and before the caller's schema
// registry loaded. A schema whose record moved in between, or had none then,
// was dropped, or dropped and registered again, while that metadata loaded:
// it may be the dropped schema's, so the schema gets no pin and every read of
// it fails closed (federatedv2.ErrNotPinned) until the process loads its
// metadata again. A nil reading binds nothing.
func bindEpochPins(pins map[int16]int64, befores ...map[int16]int64) map[int16]int64 {
	for _, before := range befores {
		if before == nil {
			continue
		}
		for id, epoch := range pins {
			if was, ok := before[id]; !ok || was != epoch {
				zap.S().Warnw("schema's publication record moved while its metadata loaded; its federated reads fail closed until the process restarts",
					"schema_id", id, "epoch_before_load", was, "epoch_after_load", epoch, "had_record_before_load", ok)
				delete(pins, id)
			}
		}
	}
	return pins
}

// ErrSchemaRegistryEpochsRequired refuses a federated reader whose caller
// gave no forma.Config.SchemaRegistryEpochs: without the epochs read before
// the caller's registry loaded, no pin proves that the registry describes the
// schema the record belongs to.
var ErrSchemaRegistryEpochsRequired = errors.New("config.SchemaRegistryEpochs is required when DuckDB is enabled")

// ReadSchemaRegistryEpochs reads every schema's publication epoch, in one
// snapshot, for config.SchemaRegistryEpochs. Call it before loading
// config.SchemaRegistry, on the pool the factory is given: the federated
// reader pins only the schemas whose record is still at the epoch read here,
// so a drop or re-registration that commits while the registry loads cannot
// pair the registry's view of the dropped schema with the new schema's
// generations. It answers nil, reading nothing, when DuckDB is disabled.
func ReadSchemaRegistryEpochs(ctx context.Context, config *forma.Config, pool *pgxpool.Pool) (map[int16]int64, error) {
	if config == nil || !config.DuckDB.Enabled {
		return nil, nil
	}
	if pool == nil {
		return nil, errors.New("read the schema registry's publication epochs: a database pool is required")
	}
	qualified := configWithQualifiedTables(config, normalizeSchemaName(config.Database.Schema))
	epochs, err := readPublicationEpochs(ctx, pool, qualified.Database.TableNames.EntityMain)
	if err != nil {
		return nil, fmt.Errorf("read the schema registry's publication epochs: %w", err)
	}
	return epochs, nil
}

// LoadFileSchemaRegistryContext loads the file schema registry of config
// (config.Database.TableNames.SchemaRegistry over schemaDir) on pool, after
// reading the publication epochs it belongs to, and sets both
// config.SchemaRegistry and config.SchemaRegistryEpochs. It is the startup
// order the server and the Lambda follow: epochs, registry, then the factory.
func LoadFileSchemaRegistryContext(ctx context.Context, config *forma.Config, pool *pgxpool.Pool, schemaDir string) error {
	epochs, err := ReadSchemaRegistryEpochs(ctx, config, pool)
	if err != nil {
		return err
	}
	registry, err := schemameta.NewFileSchemaRegistryContext(ctx, pool, config.Database.TableNames.SchemaRegistry, schemaDir)
	if err != nil {
		return fmt.Errorf("failed to create schema registry: %w", err)
	}
	config.SchemaRegistry, config.SchemaRegistryEpochs = registry, epochs
	return nil
}

// newPublicationReader builds the federated reader's Protocol V2 source
// (docs/publication-protocol-v2.md, "Readers"): a federatedv2.Source over a
// verifying manifest cache on the configured bucket, with the epoch of every
// schema the metadata cache holds pinned as of one snapshot read now, beside
// the metadata, and kept only where the epoch is the one read before the
// metadata loaded (bindEpochPins). The metadata is loaded once per process,
// so the pins are too.
//
// Every failure is fatal: an engine with DuckDB enabled and no reader fails
// every federated read, so startup fails instead, naming the cause.
func newPublicationReader(ctx context.Context, spec publicationReaderSpec) (federated.PublicationReader, error) {
	if spec.pool == nil {
		return nil, fmt.Errorf("the publication reader needs a database pool to pin the schemas' epochs")
	}
	if spec.registryEpochs == nil {
		return nil, fmt.Errorf("read them with factory.ReadSchemaRegistryEpochs before loading config.SchemaRegistry: %w", ErrSchemaRegistryEpochsRequired)
	}
	pins, err := loadEpochPins(ctx, spec.pool, spec.entityMain, registeredSchemaIDs(spec.meta))
	if err != nil {
		return nil, err
	}
	pins = bindEpochPins(pins, spec.metadataEpochs, spec.registryEpochs)
	client, err := newPublicationS3Client(ctx, spec.duck)
	if err != nil {
		return nil, fmt.Errorf("failed to build the s3 client of the publication reader: %w", err)
	}
	return newPublicationSource(client, spec.duck.S3Bucket, spec.entityMain, pins)
}

// newPublicationSource assembles the source over client and bucket.
func newPublicationSource(client publicationS3Client, bucket, entityMain string, pins map[int16]int64) (*federatedv2.Source, error) {
	cache, err := manifestv2.NewCache(&manifestv2.S3Store{Client: client, Bucket: bucket}, manifestCacheEntries)
	if err != nil {
		return nil, fmt.Errorf("failed to build the manifest cache: %w", err)
	}
	src, err := federatedv2.New(federatedv2.Config{
		EntityMain: entityMain,
		Manifests:  cache,
		Epochs:     pins,
		ObjectURI:  federatedv2.S3URI(bucket),
		Exists:     manifestv2.S3Exists(client, bucket),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to build the publication reader: %w", err)
	}
	return src, nil
}

// loadEpochPins reads the epoch pins of schemaIDs in one REPEATABLE READ READ
// ONLY snapshot.
func loadEpochPins(ctx context.Context, pool snapshotPool, entityMain string, schemaIDs []int16) (map[int16]int64, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("failed to open the snapshot the publication epochs are pinned in: %w", err)
	}
	// A read-only transaction has nothing to undo: after Commit this is a
	// no-op, and after a failure it releases the snapshot.
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	pins, err := federatedv2.LoadEpochPins(ctx, tx, entityMain, schemaIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to pin the publication epochs of %d schemas: %w", len(schemaIDs), err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to end the snapshot the publication epochs were pinned in: %w", err)
	}
	return pins, nil
}

// registeredSchemaIDs answers the IDs of every schema meta holds, sorted.
func registeredSchemaIDs(meta *schemameta.MetadataCache) []int16 {
	if meta == nil {
		return nil
	}
	names := meta.ListSchemas()
	ids := make([]int16, 0, len(names))
	for _, name := range names {
		if id, ok := meta.GetSchemaID(name); ok {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}
