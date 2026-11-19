package factory

import (
	"context"
	"fmt"
	"slices"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/bootstrap"
	"github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/federatedv2"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/schemameta"
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
	// entityMain names the publication tables (publication.TablesFor).
	entityMain string
}

// newPublicationReader builds the federated reader's Protocol V2 source
// (docs/publication-protocol-v2.md, "Readers"): a federatedv2.Source over a
// verifying manifest cache on the configured bucket, with the epoch of every
// schema the metadata cache holds pinned as of one snapshot read now, beside
// the metadata. The metadata is loaded once per process, so the pins are too.
//
// Every failure is fatal: an engine with DuckDB enabled and no reader fails
// every federated read, so startup fails instead, naming the cause.
func newPublicationReader(ctx context.Context, spec publicationReaderSpec) (federated.PublicationReader, error) {
	if spec.pool == nil {
		return nil, fmt.Errorf("the publication reader needs a database pool to pin the schemas' epochs")
	}
	pins, err := loadEpochPins(ctx, spec.pool, spec.entityMain, registeredSchemaIDs(spec.meta))
	if err != nil {
		return nil, err
	}
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
