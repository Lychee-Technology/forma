package cdcv2

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/s3endpoint"
)

// The deployment's storage binding (#911, publication.StorageTarget) names
// the target garbage collection runs against. A writer over an S3 bucket
// keeps it true to where the generations are:
//
//   - Before it stages anything, it checks the binding (checkStorage). A
//     deployment bound to another target fails with
//     publication.ErrStorageBoundElsewhere, and nothing is sent to S3.
//   - In every publication transaction, it checks the binding again and, on
//     an unbound deployment, binds its own target (publication.BindStorageTx).
//     The binding commits with the first generation, so a run that publishes
//     nothing, or a publication that fails, rolls back or never commits,
//     binds nothing. Since every transaction checks, a writer's publications
//     after a rebind are checked against the new binding. A transaction that
//     read the old binding before the row changed is not fenced by that
//     read and may still commit, which is why a rebind needs the writers and
//     GC stopped (docs/manifest-reconcile.md, "Changing the binding").
//
// The target is derived from the writer's own store: the bucket URL its SDK
// client sends requests to (s3endpoint.ClientEndpoint, the derivation the
// collector binds and checks with), its bucket and the layout's prefix. A
// store that is not an S3Store over an *s3.Client (a file store, or an
// embedder's own client) names no target a collector could check, and binds
// and checks nothing.

// ErrRedirectsFollowed refuses a writer whose SDK client may follow an HTTP
// redirect: a redirected PUT or GET would be answered by a route other than
// the bound one, and a redirected PUT's success could publish a generation
// whose object the bound route never stored.
var ErrRedirectsFollowed = errors.New("the writer's S3 client may follow redirects; build it with s3endpoint.WithoutRedirects")

// storageTarget answers the normalized target a writer over store and layout
// publishes into, or nil when store names none.
func storageTarget(store manifestv2.Store, layout manifestv2.Layout) (*publication.StorageTarget, error) {
	s, ok := store.(*manifestv2.S3Store)
	if !ok || s == nil {
		return nil, nil
	}
	c, ok := s.Client.(*s3.Client)
	if !ok || c == nil {
		return nil, nil
	}
	if !s3endpoint.FollowsNoRedirect(c) {
		return nil, fmt.Errorf("bucket %q: %w", s.Bucket, ErrRedirectsFollowed)
	}
	// The probe builds requests without sending them, so it needs no
	// deadline of the caller's.
	endpoint, err := s3endpoint.ClientEndpoint(context.Background(), c, s.Bucket)
	if err != nil {
		return nil, fmt.Errorf("tell where the writer's S3 client sends bucket %q's requests: %w", s.Bucket, err)
	}
	t, err := publication.StorageTarget{Endpoint: endpoint, Bucket: s.Bucket, Prefix: layout.Prefix}.Normalize()
	if err != nil {
		return nil, fmt.Errorf("the writer's storage target: %w", err)
	}
	return &t, nil
}

// checkStorage is the check before a staging: it reads the binding and fails
// when it names another target, or when the binding table is missing.
func (w *Writer) checkStorage(ctx context.Context) error {
	if w.storage == nil {
		return nil
	}
	if err := publication.CheckStorage(ctx, publication.DB(w.cfg.DB), w.tables, *w.storage, w.cfg.Engine); err != nil {
		return fmt.Errorf("check the deployment's storage binding before staging: %w", err)
	}
	return nil
}

// bindStorage checks the binding in the publication transaction tx and binds
// an unbound deployment to the writer's target there.
func (w *Writer) bindStorage(ctx context.Context, tx publication.Tx) error {
	if w.storage == nil {
		return nil
	}
	if err := publication.BindStorageTx(ctx, tx, w.tables, *w.storage); err != nil {
		return fmt.Errorf("bind the deployment's storage in the publication transaction: %w", err)
	}
	return nil
}

// StorageTarget answers the target the writer binds and checks, and false
// for a writer whose store names none.
func (w *Writer) StorageTarget() (publication.StorageTarget, bool) {
	if w.storage == nil {
		return publication.StorageTarget{}, false
	}
	return *w.storage, true
}
