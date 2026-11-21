package reconcile

import (
	"context"
	"fmt"

	"github.com/lychee-technology/forma/internal/publication"
)

// checkStorage verifies that the run's Storage target is valid, matches the
// Layout prefix and the ObjectStore's actual Destination (when supported), and
// matches the deployment's PostgreSQL storage binding. A mismatch or missing
// binding yields a blocking UnboundStorage finding for every schema.
func (r *Reconciler) checkStorage(ctx context.Context) (*Finding, error) {
	norm, err := r.Storage.Normalize()
	if err != nil {
		return nil, fmt.Errorf("validate the run's storage target: %w", err)
	}
	layout := publication.StorageTarget{Endpoint: norm.Endpoint, Bucket: norm.Bucket, Prefix: r.Layout.Prefix}
	if !layout.Same(norm) {
		return nil, fmt.Errorf("the run's storage target %s is not under its layout's prefix %q", norm, r.Layout.Prefix)
	}
	if d, ok := r.Objects.(Destination); ok {
		endpoint, err := d.Endpoint(ctx)
		if err != nil {
			return nil, fmt.Errorf("tell where the run's object store sends its requests: %w", err)
		}
		storeTarget := publication.StorageTarget{Endpoint: endpoint, Bucket: norm.Bucket, Prefix: norm.Prefix}
		if !storeTarget.Same(norm) {
			return nil, fmt.Errorf("the run's storage target %s does not match the endpoint %q its object store sends requests to",
				norm, endpoint)
		}
	}
	b, ok, err := publication.ReadStorage(ctx, publication.DB(r.DB), r.Tables)
	if err != nil {
		return nil, fmt.Errorf("read the deployment's storage binding: %w", err)
	}
	switch {
	case !ok:
		return &Finding{Kind: UnboundStorage, Action: actionUnboundStorage, Detail: fmt.Sprintf(
			"the deployment has no storage binding, so nothing proves that %s is where this database's writers publish", norm)}, nil
	case !b.Target.Same(norm):
		return &Finding{Kind: UnboundStorage, Action: actionRebindStorage, Detail: fmt.Sprintf(
			"the deployment is bound to %s since %d, and this run targets %s", b.Target, b.BoundAt, norm)}, nil
	}
	return nil, nil
}
