package cdc

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/lychee-technology/forma/internal/manifest"
)

// S3InitClient is the client surface cdc-init needs: the manifest and
// checksum operations of S3FullClient plus ListObjectsV2 for the delta-tier
// inventory (#371). *s3.Client satisfies it.
type S3InitClient interface {
	S3FullClient
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

// applyInitS3Wiring sets every S3-derived field of an init run context: the
// object client the export path copies and stats through, the manifest store
// and resolver, the content-checksum seam (#347), and the delta-tier listing
// and delete seams (#371). It is the one place those fields are wired:
// newInitRunContext carries no S3 wiring of its own, so a field that exists
// only here cannot be live on some construction paths and silently absent on
// others. That the real constructor still delegates here, with the run's own
// client, is checked directly by
// TestNewInitRunContextWiresChecksumSeamOnTheRealPath.
//
// The manifest store and resolver stay behind the template check: without a
// configured template there is no manifest path to resolve, and
// updateSchemaManifest no-ops. The other seams are wired regardless — their
// call sites gate on manifestStore or on nil themselves.
func applyInitS3Wiring(runCtx *initRunContext, cfg CDCConfig, client S3InitClient) {
	runCtx.s3Client = client
	runCtx.checksumObject = newChecksumSeam(client, cfg.S3Bucket)
	runCtx.listObjectKeys, runCtx.deleteObject = newDeltaTierSeams(client, cfg.S3Bucket)

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

// newDeltaTierSeams builds the listing and delete closures over the resolved
// client, returning literal nils when no usable client exists (the same
// typed-nil judgment newChecksumSeam makes, #302) so the inventory and purge
// sites can gate on nil honestly.
func newDeltaTierSeams(client S3InitClient, bucket string) (
	list func(ctx context.Context, prefix string) ([]string, error),
	del func(ctx context.Context, key string) error,
) {
	if client == nil {
		return nil, nil
	}
	if c, ok := client.(*s3.Client); ok && c == nil {
		return nil, nil
	}
	list = func(ctx context.Context, prefix string) ([]string, error) {
		return ListObjectKeys(ctx, client, bucket, prefix)
	}
	del = func(ctx context.Context, key string) error {
		return DeleteObjectKey(ctx, client, bucket, key)
	}
	return list, del
}

// ListObjectKeys returns every object key under prefix, following
// continuation tokens.
func ListObjectKeys(ctx context.Context, client S3InitClient, bucket, prefix string) ([]string, error) {
	var keys []string
	var token *string
	for {
		out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("list objects under %s: %w", prefix, err)
		}
		for _, obj := range out.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
		if !aws.ToBool(out.IsTruncated) || out.NextContinuationToken == nil {
			return keys, nil
		}
		token = out.NextContinuationToken
	}
}
