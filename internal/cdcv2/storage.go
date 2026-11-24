package cdcv2

import (
	"context"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/s3endpoint"
)

// storageGate is what a writer knows of the deployment's storage binding
// (#911): the target its store publishes into, once derived, and whether the
// deployment is bound to it, which a long-lived writer then never reads again.
type storageGate struct {
	mu      sync.Mutex
	derived bool
	named   bool // the store names a target (an S3Store over an *s3.Client)
	target  publication.StorageTarget
	bound   bool // bound to target, or the store names none
}

// checkStorage runs before a writer stages anything: it fails closed with
// publication.ErrStorageBoundElsewhere when the deployment is bound to
// another target than the one the writer publishes into, and with
// publication.ErrStorageTableMissing when the binding table does not exist,
// so that neither sends anything to S3. It binds nothing: an unbound
// deployment is bound by bindStorage, once the writer is about to publish.
// Garbage collection deletes only under the bound target
// (manifest-reconcile), so a writer publishing anywhere else is refused here.
//
// The target is the bucket URL the store's AWS SDK client sends requests to
// (s3endpoint.ClientEndpoint, the derivation manifest-reconcile checks its
// own client with), the store's bucket and the layout's prefix. A store that
// is not an S3Store over an *s3.Client (a file store, a caller's own client)
// has no target the collector could be bound to, so nothing is bound or
// checked for it; its deployment is bound by manifest-reconcile
// --bind-storage.
func (w *Writer) checkStorage(ctx context.Context) error {
	g := &w.storage
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.bound {
		return nil
	}
	if err := w.deriveTarget(ctx); err != nil {
		return fmt.Errorf("check the deployment's storage binding: %w", err)
	}
	if !g.named {
		g.bound = true
		return nil
	}
	bound, err := publication.CheckStorage(ctx, publication.DB(w.cfg.DB), w.tables, g.target)
	if err != nil {
		return fmt.Errorf("this writer publishes into %s, and garbage collection deletes only under the deployment's "+
			"storage binding; check the writer's S3 endpoint, bucket and data prefix (docs/manifest-reconcile.md, \"Storage binding\"): %w",
			g.target, err)
	}
	g.bound = bound
	return nil
}

// bindStorage binds an unbound deployment to the writer's target. publish
// runs it after the generation is staged and has passed every check a
// staging makes (the record's state, the published prefix, the manifest's
// verification, the parent manifest's read through the store), right before
// the publication transaction: a run that publishes nothing (an empty flush,
// a compaction that merges nothing or is refused) never binds. A rival
// writer that bound another target first fails it with
// publication.ErrStorageBoundElsewhere, and its staging is left unpublished.
func (w *Writer) bindStorage(ctx context.Context) error {
	g := &w.storage
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.bound {
		return nil
	}
	if err := w.deriveTarget(ctx); err != nil {
		return fmt.Errorf("bind the deployment's storage: %w", err)
	}
	if g.named {
		if _, err := publication.EnsureStorage(ctx, publication.DB(w.cfg.DB), w.tables, g.target); err != nil {
			return fmt.Errorf("this writer publishes into %s, and garbage collection deletes only under the deployment's "+
				"storage binding; check the writer's S3 endpoint, bucket and data prefix (docs/manifest-reconcile.md, \"Storage binding\"): %w",
				g.target, err)
		}
	}
	g.bound = true
	return nil
}

// deriveTarget fills the gate's target once. An *s3.Client whose destination
// s3endpoint.ClientEndpoint cannot establish fails the writer: it publishes
// somewhere no binding can name. The caller holds the gate's lock.
func (w *Writer) deriveTarget(ctx context.Context) error {
	g := &w.storage
	if g.derived {
		return nil
	}
	client, bucket, ok := sdkStore(w.cfg.Store)
	if ok {
		endpoint, err := s3endpoint.ClientEndpoint(ctx, client, bucket)
		if err != nil {
			return fmt.Errorf("tell where the writer's S3 client publishes: %w", err)
		}
		g.named, g.target = true, publication.StorageTarget{Endpoint: endpoint, Bucket: bucket, Prefix: w.cfg.Layout.Prefix}
	}
	g.derived = true
	return nil
}

// sdkStore answers the AWS SDK client and bucket of a store that is an
// S3Store over an *s3.Client.
func sdkStore(st manifestv2.Store) (*s3.Client, string, bool) {
	store, ok := st.(*manifestv2.S3Store)
	if !ok || store == nil {
		return nil, "", false
	}
	client, ok := store.Client.(*s3.Client)
	if !ok || client == nil {
		return nil, "", false
	}
	return client, store.Bucket, true
}

// checkRedirects refuses a store over an AWS SDK client that may follow an
// HTTP redirect. The binding names where the client's requests go, and a
// redirect would take a write elsewhere: the SDK may even take a redirected
// GET's answer for a PUT's success, so a generation would list an object
// that was never stored. s3endpoint.WithoutRedirects builds a client that
// refuses every redirect (s3endpoint.ErrRedirectRefused).
func checkRedirects(st manifestv2.Store) error {
	client, _, ok := sdkStore(st)
	if ok && !s3endpoint.FollowsNoRedirect(client) {
		return fmt.Errorf("a V2 writer's S3 client must follow no HTTP redirect, so that its objects land at the "+
			"storage target it binds; build it with s3endpoint.WithoutRedirects: %w", s3endpoint.ErrUnknownDestination)
	}
	return nil
}
