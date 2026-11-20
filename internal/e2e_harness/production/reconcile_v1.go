package production

import (
	"testing"

	"github.com/lychee-technology/forma/internal/manifest"
	"github.com/lychee-technology/forma/internal/reconcile"
)

// reconcileSkipReason is why every manifest-reconcile scenario skips: the V1
// manifest-reconcile tool repairs and collects the mutable V1 manifest, which
// a Protocol V2 schema does not have, so a cut-over deployment keeps it
// stopped (docs/publication-protocol-v2.md, "Upgrade" step 1) until V2
// garbage collection replaces it (#891).
const reconcileSkipReason = "the V1 manifest-reconcile tool does no work against a Protocol V2 schema; " +
	"reconcile scenarios run again on V2 garbage collection (#891)"

// skipV1Reconcile skips a manifest-reconcile scenario until #891.
func skipV1Reconcile(t *testing.T) {
	t.Helper()
	if v1ReconcileStopped {
		t.Skip(reconcileSkipReason)
	}
}

// v1ReconcileStopped is true until V2 garbage collection (#891) replaces the
// reconcile scenarios. It is a variable, not a constant, so that static
// analysis does not treat their code after skipV1Reconcile as unreachable.
var v1ReconcileStopped = true

// reconcileStores answers the V1 manifest and GC-state stores the
// manifest-reconcile tool wires, over the V1 manifest layout. Only the
// reconcile scenarios call it, and they skip before they do until #891
// (skipV1Reconcile).
func (e *Env) reconcileStores() (*reconcile.ResolverManifestStore, *reconcile.ManifestGCStateStore) {
	store := &manifest.S3Store{Client: e.Cluster.S3, Bucket: e.Cluster.Bucket}
	resolver := manifest.PathResolver{Prefix: e.S3Prefix, PathTemplate: "manifest/{{.SchemaID}}.json"}
	return &reconcile.ResolverManifestStore{Store: store, Resolver: resolver},
		&reconcile.ManifestGCStateStore{Store: store, Resolver: resolver}
}
