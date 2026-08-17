package cdc

import (
	"github.com/lychee-technology/forma/internal/manifest"
)

// applyInitS3Wiring sets every S3-derived field of an init run context: the
// object client the export path copies and stats through, the manifest store
// and resolver, and the content-checksum seam (#347). It is the one place
// those fields are wired, so a field that exists only here cannot be live on
// some construction paths and silently absent on others — newInitRunContext,
// which opens Postgres and DuckDB and so cannot be exercised by a unit test,
// carries no S3 wiring of its own.
//
// The manifest store and resolver stay behind the template check: without a
// configured template there is no manifest path to resolve, and
// updateSchemaManifest no-ops. The checksum seam is wired regardless — the
// stamping call sites gate on manifestStore themselves.
func applyInitS3Wiring(runCtx *initRunContext, cfg CDCConfig, client S3FullClient) {
	runCtx.s3Client = client
	runCtx.checksumObject = newChecksumSeam(client, cfg.S3Bucket)

	if cfg.ManifestTemplate != "" {
		runCtx.manifestStore = &manifest.S3Store{
			Client: client,
			Bucket: cfg.S3Bucket,
		}
		runCtx.manifestResolver = manifest.PathResolver{
			Prefix:       cfg.ManifestPrefix,
			PathTemplate: cfg.ManifestTemplate,
		}
	}
}
