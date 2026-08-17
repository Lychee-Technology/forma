package cdc

import (
	"context"
	"fmt"

	"github.com/lychee-technology/forma/internal/parquetcheck"
	"go.uber.org/zap"
)

// initStampColumns best-effort probes the just-published base object's
// footer for the manifest column stamp (#256). Mirrors the flush path's
// stampColumns: tmp→final is byte-identical, failure leaves the entry
// unstamped (readers fall back to probing) and never fails the init run.
func initStampColumns(ctx context.Context, runCtx *initRunContext, schemaID int16, batch schemaBatchExport) map[string]string {
	if runCtx.manifestStore == nil {
		// No manifest to stamp: the fileEntries are never persisted
		// (updateSchemaManifest no-ops), so a footer probe would be a
		// discarded S3 read per batch.
		return nil
	}

	describe := runCtx.describeColumns
	if describe == nil {
		if runCtx.duck == nil || runCtx.duck.DB == nil {
			return nil
		}
		describe = func(ctx context.Context, uri string) (map[string]string, error) {
			return parquetcheck.DescribeColumns(ctx, runCtx.duck.DB, uri)
		}
	}
	uri := fmt.Sprintf("s3://%s/%s", runCtx.cfg.S3Bucket, batch.finalKey)
	cols, err := describe(ctx, uri)
	if err != nil {
		runCtx.logger.Warn("failed to describe final base object; manifest entry stays unstamped",
			zap.Int16("schema_id", schemaID),
			zap.String("final_key", batch.finalKey),
			zap.Error(err))
		return nil
	}
	return cols
}

// initStampChecksum best-effort hashes the just-published base object for the
// manifest content checksum (#347). Mirrors the flush path's stampChecksum:
// tmp→final is a byte-identical CopyObject, so the final's bytes are the
// export's bytes. A failed hash leaves the entry unstamped — verification
// passes skip an empty Checksum — so stamping never fails the init run.
//
// Like initStampColumns it short-circuits without a manifest store: the
// fileEntries are then never persisted, so the hash would be a discarded
// full-object read per batch.
func initStampChecksum(ctx context.Context, runCtx *initRunContext, schemaID int16, batch schemaBatchExport) string {
	if runCtx.manifestStore == nil || runCtx.checksumObject == nil {
		return ""
	}
	sum, err := runCtx.checksumObject(ctx, batch.finalKey)
	if err != nil {
		runCtx.logger.Warn("failed to checksum final base object; manifest entry stays unstamped",
			zap.Int16("schema_id", schemaID),
			zap.String("final_key", batch.finalKey),
			zap.Error(err))
		return ""
	}
	return sum
}
