package production

import (
	"context"
	"fmt"
	"path"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/manifest"
)

// RunInitKeepingDelta runs a real init and then restores the schema's
// manifest-listed delta objects and entries, reconstructing the bucket
// layout every pre-#371 re-init left behind: a fresh base with the older
// delta generations still listed under it. Since #371 no supported tool
// produces that layout (cdc-init refuses while delta exists and purges it
// with --replace-delta), but live buckets re-initialized before #371 carry
// it, and the reader must keep folding it correctly (#210 strict recency).
// The fixture exists only in this harness; it is not reachable from
// cdc.InitOptions or the CLI, so it is not a bypass of the refusal.
//
// Mechanics: copy every listed delta object aside under a nested `_keep/`
// key (not delta-shaped, so init ignores it), run init with ReplaceDelta,
// move the copies back to their original keys, and re-splice the saved
// delta entries into the manifest the init published.
func (e *Env) RunInitKeepingDelta(ctx context.Context, schema SchemaRef) (*InitReport, error) {
	store, resolver := e.manifestAccess()
	manifestPath, err := resolver.Resolve(schema.ID)
	if err != nil {
		return nil, fmt.Errorf("resolve manifest path for schema %d: %w", schema.ID, err)
	}
	m, _, err := manifest.Load(ctx, store, manifestPath)
	if err != nil {
		// No manifest yet: nothing to keep, a plain init is the same run.
		return e.RunInit(ctx, schema)
	}
	deltaEntries := manifest.FilterByTier(m, "delta")
	if len(deltaEntries) == 0 {
		return e.RunInit(ctx, schema)
	}

	aside := make(map[string]string, len(deltaEntries))
	for _, entry := range deltaEntries {
		key, ok := cdc.NormalizeObjectKey(e.Cluster.Bucket, entry.Path)
		if !ok {
			return nil, fmt.Errorf("keep delta: manifest entry %q is not addressable in bucket %s", entry.Path, e.Cluster.Bucket)
		}
		dir, file := path.Split(key)
		asideKey := dir + "_keep/" + file
		if err := e.copyS3Object(ctx, key, asideKey); err != nil {
			return nil, fmt.Errorf("keep delta: copy %s aside: %w", key, err)
		}
		aside[key] = asideKey
	}

	report, err := e.RunInitWith(ctx, schema, InitOverrides{ReplaceDelta: true})
	if err != nil {
		return nil, fmt.Errorf("keep delta: init with replace-delta: %w", err)
	}

	for key, asideKey := range aside {
		if err := cdc.CopyTmpToFinal(ctx, e.Cluster.S3, e.Cluster.Bucket, asideKey, key, e.logger); err != nil {
			return nil, fmt.Errorf("keep delta: restore %s: %w", key, err)
		}
	}

	m, etag, err := manifest.Load(ctx, store, manifestPath)
	if err != nil {
		return nil, fmt.Errorf("keep delta: reload manifest after init: %w", err)
	}
	manifest.SpliceTierFiles(m, "delta", deltaEntries)
	if _, err := manifest.Save(ctx, store, manifestPath, m, etag); err != nil {
		return nil, fmt.Errorf("keep delta: re-splice %d delta entries: %w", len(deltaEntries), err)
	}
	report.Manifest = m
	return report, nil
}

// manifestAccess returns the store and resolver for the Env's manifests.
func (e *Env) manifestAccess() (manifest.Store, manifest.PathResolver) {
	return &manifest.S3Store{Client: e.Cluster.S3, Bucket: e.Cluster.Bucket},
		manifest.PathResolver{Prefix: e.CDC.ManifestPrefix, PathTemplate: e.CDC.ManifestTemplate}
}

// copyS3Object copies one object within the Env's bucket, leaving the source
// in place.
func (e *Env) copyS3Object(ctx context.Context, srcKey, dstKey string) error {
	if _, err := e.Cluster.S3.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(e.Cluster.Bucket),
		CopySource: aws.String(e.Cluster.Bucket + "/" + srcKey),
		Key:        aws.String(dstKey),
	}); err != nil {
		return fmt.Errorf("copy %s to %s: %w", srcKey, dstKey, err)
	}
	return nil
}
