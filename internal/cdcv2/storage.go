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

// storageGate remembers that a writer's storage target has been bound and
// checked, so that a long-lived writer reads the binding once.
type storageGate struct {
	mu   sync.Mutex
	done bool
}

// ensureStorage binds the deployment to the storage target this writer
// publishes into, or checks it against the binding the deployment has, once
// per writer and before anything is staged (#911). Garbage collection
// deletes only under the bound target (manifest-reconcile), so a writer
// publishing anywhere else fails closed here with
// publication.ErrStorageBoundElsewhere, before it stages or moves a record.
//
// The target is the bucket URL the store's AWS SDK client sends requests to
// (s3endpoint.ClientEndpoint, the derivation manifest-reconcile checks its
// own client with), the store's bucket and the layout's prefix. A store that
// is not an S3Store over an *s3.Client (a file store, a caller's own client)
// has no target the collector could be bound to, so nothing is bound or
// checked for it; its deployment is bound by manifest-reconcile
// --bind-storage.
func (w *Writer) ensureStorage(ctx context.Context) error {
	w.storage.mu.Lock()
	defer w.storage.mu.Unlock()
	if w.storage.done {
		return nil
	}
	target, ok, err := w.storageTarget(ctx)
	if err != nil {
		return err
	}
	if ok {
		if _, err := publication.EnsureStorage(ctx, publication.DB(w.cfg.DB), w.tables, target); err != nil {
			return fmt.Errorf("this writer publishes into %s, and garbage collection deletes only under the deployment's "+
				"storage binding; check the writer's S3 endpoint, bucket and data prefix (docs/manifest-reconcile.md, \"Storage binding\"): %w",
				target, err)
		}
	}
	w.storage.done = true
	return nil
}

// storageTarget answers where the writer's store puts its objects, and false
// when the store cannot tell. An *s3.Client whose destination
// s3endpoint.ClientEndpoint cannot establish fails the writer: it publishes
// somewhere no binding can name.
func (w *Writer) storageTarget(ctx context.Context) (publication.StorageTarget, bool, error) {
	store, ok := w.cfg.Store.(*manifestv2.S3Store)
	if !ok || store == nil {
		return publication.StorageTarget{}, false, nil
	}
	client, ok := store.Client.(*s3.Client)
	if !ok || client == nil {
		return publication.StorageTarget{}, false, nil
	}
	endpoint, err := s3endpoint.ClientEndpoint(ctx, client, store.Bucket)
	if err != nil {
		return publication.StorageTarget{}, false, fmt.Errorf("tell where the writer's S3 client publishes, to bind the deployment's storage: %w", err)
	}
	return publication.StorageTarget{Endpoint: endpoint, Bucket: store.Bucket, Prefix: w.cfg.Layout.Prefix}, true, nil
}
