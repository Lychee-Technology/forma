package cdc

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/lychee-technology/forma/internal/manifest"
)

// ErrManifestStoreUnwired marks a flush refused because it has no manifest
// store (an empty ManifestTemplate) while a manifest-driven reader may depend
// on the manifest it would bypass (#474). Marking a batch flushed drops its
// rows from the hot tier, and the reader's glob fallback only applies to a
// schema whose manifest is empty or absent, so no manifest-driven read would
// ever see them again. It is an operator-visible configuration error: either
// the run did not declare GlobOnly, or it did and the schema's manifest at the
// default location lists files anyway.
var ErrManifestStoreUnwired = errors.New("flush has no manifest store to list its delta in")

// ErrGlobOnlyWithTemplate marks a flush configuration that both declares
// GlobOnly and names a ManifestTemplate: one says no reader uses a manifest,
// the other writes one for them.
var ErrGlobOnlyWithTemplate = errors.New("glob-only flush configured with a manifest template")

// checkManifestMode fails closed on a flush whose manifest configuration is
// unknown (#474). A run with no template cannot locate a manifest a reader
// uses under a custom template, so absence at any path it could probe proves
// nothing; only an explicit GlobOnly declaration makes a template-less flush
// safe to run.
func checkManifestMode(cfg CDCConfig) error {
	if cfg.ManifestTemplate != "" {
		if cfg.GlobOnly {
			return fmt.Errorf("%w: template %q; clear the template or drop the glob-only declaration", ErrGlobOnlyWithTemplate, cfg.ManifestTemplate)
		}
		return nil
	}
	if !cfg.GlobOnly {
		return fmt.Errorf("%w: the manifest template is empty and the run does not declare glob-only reads, "+
			"so a manifest a reader uses (at any template) could not list the flushed delta; "+
			"set the manifest template the readers use, or declare glob-only when no reader uses a manifest", ErrManifestStoreUnwired)
	}
	return nil
}

// unwiredManifestProbe reports whether a schema has a manifest a flush without
// a manifest store would bypass: listed is true when the manifest at path
// lists at least one file. An error means the answer is unknown.
type unwiredManifestProbe func(ctx context.Context, schemaID int16) (path string, listed bool, err error)

// newUnwiredManifestProbe builds the guard probe of a declared glob-only run
// (#474). It cannot prove the declaration (a custom template's manifest is
// invisible to it; checkManifestMode is what keeps an undeclared run out), but
// it catches the contradiction it can see: a manifest at the run's
// ManifestPrefix plus the default template, where every Forma tool writes by
// default, that lists files.
//
// With a store (a full client) the probe reads the manifest and refuses only
// one that lists files: an empty manifest still sends readers to the glob, so
// marking is safe. An object-only client cannot read it, so the probe fails
// closed and treats an existing object as listing files.
func newUnwiredManifestProbe(manifestPrefix string, store manifest.Store, head manifest.S3HeadClient, bucket string) unwiredManifestProbe {
	resolver := manifest.PathResolver{Prefix: manifestPrefix}
	if store != nil {
		return storeManifestProbe(store, resolver)
	}
	if head == nil {
		return nil
	}
	return headManifestProbe(manifest.S3ExistsProbe(head, bucket), resolver)
}

// usableFullClient rejects a nil client and an interface holding a typed-nil
// *s3.Client, which passes `!= nil` and panics on first use (#302).
func usableFullClient(client S3FullClient) bool {
	if client == nil {
		return false
	}
	c, ok := client.(*s3.Client)
	return !ok || c != nil
}

func storeManifestProbe(store manifest.Store, resolver manifest.PathResolver) unwiredManifestProbe {
	return func(ctx context.Context, schemaID int16) (string, bool, error) {
		path, err := resolver.Resolve(schemaID)
		if err != nil {
			return "", false, fmt.Errorf("resolve default manifest path: %w", err)
		}
		m, _, err := manifest.Load(ctx, store, path)
		if manifest.IsNotFound(err) {
			return path, false, nil
		}
		if err != nil {
			return path, false, fmt.Errorf("load manifest %s: %w", path, err)
		}
		return path, len(m.Files) > 0, nil
	}
}

func headManifestProbe(exists func(ctx context.Context, key string) (bool, error), resolver manifest.PathResolver) unwiredManifestProbe {
	return func(ctx context.Context, schemaID int16) (string, bool, error) {
		path, err := resolver.Resolve(schemaID)
		if err != nil {
			return "", false, fmt.Errorf("resolve default manifest path: %w", err)
		}
		found, err := exists(ctx, path)
		if err != nil {
			return path, false, fmt.Errorf("probe manifest %s: %w", path, err)
		}
		return path, found, nil
	}
}

// refuseUnlistedFlush is the batch half of the #474 guard (checkManifestMode
// is the run half): a declared glob-only batch, which cannot append its delta
// to a manifest, must not run when the schema's manifest lists files, since
// marking it flushed would leave its rows in neither the hot tier nor the
// listed delta set, the state the #252 ordering exists to rule out. It runs
// before the batch's first S3 write, so a refused batch leaves no orphan
// object and its rows stay hot. A probe that cannot answer refuses too, and
// so does a store-less batch whose run never declared glob-only, whatever
// built its executor.
func (e *flushBatchExecutor) refuseUnlistedFlush(ctx context.Context) error {
	if e.manifestStore != nil {
		return nil
	}
	if err := checkManifestMode(e.cfg); err != nil {
		return fmt.Errorf("schema %d: %w", e.schemaID, err)
	}
	if e.probeUnwiredManifest == nil {
		return nil
	}
	path, listed, err := e.probeUnwiredManifest(ctx, e.schemaID)
	if err != nil {
		return fmt.Errorf("check schema %d for a manifest this flush cannot append to: %w", e.schemaID, err)
	}
	if listed {
		return fmt.Errorf("%w: schema %d, manifest %s; marking the batch flushed without listing its delta would hide its rows from manifest-driven reads although the run declares glob-only, so set the manifest template the readers use", ErrManifestStoreUnwired, e.schemaID, path)
	}
	return nil
}
