package cdc

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/manifest"
	"go.uber.org/zap"
)

// resolvedS3Clients is the run's S3 client pair. The two travel as one value so
// a caller cannot carry the object client forward while dropping the full
// client every S3-derived flush-context field is built from — the manifest
// store and the content-checksum seam (#347).
type resolvedS3Clients struct {
	object S3ObjectClient
	full   S3FullClient
}

// resolveS3Clients picks the run's client pair: the caller's client when it
// provided one, the ambient default otherwise. requireFull rejects an
// object-only caller client when the run needs manifest access.
func resolveS3Clients(
	provided S3ObjectClient,
	fallback S3FullClient,
	requireFull bool,
) (resolvedS3Clients, error) {
	if provided == nil {
		if fallback == nil {
			return resolvedS3Clients{}, fmt.Errorf("s3 client is required")
		}
		return resolvedS3Clients{object: fallback, full: fallback}, nil
	}

	fullClient, ok := provided.(S3FullClient)
	if !ok {
		if requireFull {
			return resolvedS3Clients{}, fmt.Errorf("manifest requires S3FullClient when custom s3 client is provided")
		}
		return resolvedS3Clients{object: provided}, nil
	}

	return resolvedS3Clients{object: provided, full: fullClient}, nil
}

// flushContextParams carries the per-run values both RunOnce frames resolve
// before a flush context can be built.
type flushContextParams struct {
	cfg            CDCConfig
	db             *sql.DB
	duck           *DuckExporter
	clients        resolvedS3Clients
	pgPassword     string
	dryRun         bool
	logger         *zap.Logger
	schemaRegistry forma.SchemaRegistry
}

// newSchemaFlushContext assembles the flush context both entrypoints run —
// package-level RunOnce (cmd/tools cdc-flush) and Runner.RunOnce. It is the one
// place the S3-derived fields are wired, so a field that exists only here, like
// the content-checksum seam (#347), cannot be live on one path and silently
// absent on the other; both frames are a single call to this function and carry
// no wiring of their own.
func newSchemaFlushContext(p flushContextParams) *schemaFlushContext {
	tableName := p.cfg.ChangeLogTable
	if tableName == "" {
		tableName = "change_log"
	}

	flushCtx := &schemaFlushContext{
		db:             p.db,
		duck:           p.duck,
		s3Client:       p.clients.object,
		cfg:            p.cfg,
		tableName:      tableName,
		pgPassword:     p.pgPassword,
		dryRun:         p.dryRun,
		logger:         p.logger,
		schemaRegistry: p.schemaRegistry,
		checksumObject: newChecksumSeam(p.clients.full, p.cfg.S3Bucket),
	}

	if p.cfg.ManifestTemplate != "" {
		flushCtx.manifestStore = &manifest.S3Store{
			Client: p.clients.full,
			Bucket: p.cfg.S3Bucket,
		}
		flushCtx.manifestResolver = manifest.PathResolver{
			Prefix:       p.cfg.ManifestPrefix,
			PathTemplate: p.cfg.ManifestTemplate,
		}
	}

	return flushCtx
}

// newChecksumSeam builds the executor's manifest content-hash seam over the
// resolved full S3 client (#347). It returns a literal nil closure when no
// usable client exists, so the executor's `checksumObject == nil` gate stays
// honest and entries simply go unstamped.
//
// The nil judgment happens here, on the concrete client, rather than inside the
// executor: resolveS3Clients hands back an S3FullClient interface, and an
// interface holding a typed-nil pointer passes `!= nil` and would panic on the
// first GetObject (#302).
func newChecksumSeam(client S3FullClient, bucket string) func(ctx context.Context, key string) (string, error) {
	if client == nil {
		return nil
	}
	if c, ok := client.(*s3.Client); ok && c == nil {
		return nil
	}
	return func(ctx context.Context, key string) (string, error) {
		return ObjectSHA256(ctx, client, bucket, key)
	}
}
