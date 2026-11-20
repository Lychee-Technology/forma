package reconcile

import (
	"context"
	"fmt"

	"github.com/lychee-technology/forma/internal/publication"
)

// checkStorage answers the finding every schema carries when the run's
// target is not the storage the deployment is bound to, nil when it is.
//
// Nothing in a bucket proves that it is this database's writers' bucket: a
// manifest that verifies against the digest PostgreSQL committed proves the
// bucket holds this database's committed inventory, and a copy of the bucket
// holds that too. Pointed at a copy that another deployment kept publishing
// into, a collector whose history ends at the copied generation N would
// verify N and take the other deployment's N+1, its manifest and the
// objects only it lists, for old orphans. The binding is the operator's
// statement, kept in PostgreSQL apart from anything a bucket holds, of the
// target the writers publish into; a run configured with any other endpoint,
// bucket or prefix deletes and forgets nothing. The endpoint compared is
// the one the run's object store sends its requests to when the store can
// tell (Destination), so an environment that redirects the client while
// the run's flags stay the same does not pass for the bound target.
func (r *Reconciler) checkStorage(ctx context.Context) (*Finding, error) {
	layout := publication.StorageTarget{Endpoint: r.Storage.Endpoint, Bucket: r.Storage.Bucket, Prefix: r.Layout.Prefix}
	if !layout.Same(r.Storage) {
		return nil, fmt.Errorf("the run's storage target %s is not under its layout's prefix %q", r.Storage, r.Layout.Prefix)
	}
	if d, ok := r.Objects.(Destination); ok {
		endpoint, err := d.Endpoint(ctx)
		if err != nil {
			return nil, fmt.Errorf("tell where the run's object store sends its requests: %w", err)
		}
		if !(publication.StorageTarget{Endpoint: endpoint}).Same(publication.StorageTarget{Endpoint: r.Storage.Endpoint}) {
			return nil, fmt.Errorf("the run's storage target %s does not name the endpoint %q its object store sends its requests to",
				r.Storage, endpoint)
		}
	}
	b, ok, err := publication.ReadStorage(ctx, publication.DB(r.DB), r.Tables)
	if err != nil {
		return nil, fmt.Errorf("read the deployment's storage binding: %w", err)
	}
	switch {
	case !ok:
		return &Finding{Kind: UnboundStorage, Action: actionUnboundStorage, Detail: fmt.Sprintf(
			"the deployment has no storage binding, so nothing proves that %s is where this database's writers publish: "+
				"a bucket holding a copy of their manifests verifies as well as theirs", r.Storage)}, nil
	case !b.Target.Same(r.Storage):
		return &Finding{Kind: UnboundStorage, Action: actionRebindStorage, Detail: fmt.Sprintf(
			"the deployment is bound to %s since %d, and this run is configured with %s", b.Target, b.BoundAt, r.Storage)}, nil
	}
	return nil, nil
}
