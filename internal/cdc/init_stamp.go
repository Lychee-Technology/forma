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
