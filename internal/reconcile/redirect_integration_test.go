package reconcile

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/publication"
)

// TestARedirectedCollectionDeletesAndForgetsNothingElsewhere is R10-F2
// through a whole collection: the pass lists, reads and deletes through the
// store manifest-reconcile builds (NewS3Objects over the SDK's own client),
// bound to the URL that store tells. Two generations are past their grace,
// so the plan deletes their objects and manifests and forgets the lower
// one. The bound server answers the LIST and the reads, and redirects every
// DELETE to another server. The pass fails at its first step: no DELETE
// reaches the other route, no object of the bucket is deleted, and no
// history row is forgotten. With the redirect gone, the next pass collects
// exactly what the first one planned.
func TestARedirectedCollectionDeletesAndForgetsNothingElsewhere(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	other := newOtherRoute(t)
	s, srv := newS3Server(t, f.store, f.target().Bucket)
	objects, err := NewS3Objects(sdkClient(srv.URL), f.target().Bucket)
	require.NoError(t, err)
	endpoint, err := objects.Endpoint(ctx)
	require.NoError(t, err)
	target := publication.StorageTarget{Endpoint: endpoint, Bucket: f.target().Bucket, Prefix: f.layout.Prefix}
	f.exec(t, "DELETE FROM %s", f.tables.Storage)
	_, err = publication.BindStorage(ctx, publication.DB(f.db), f.tables, target)
	require.NoError(t, err)
	run := func() SchemaReport {
		r := f.reconciler(testOptions(true))
		r.Objects, r.Storage = objects, target
		report, err := r.Run(ctx)
		require.NoError(t, err)
		require.Len(t, report.Schemas, 1)
		return report.Schemas[0]
	}

	g1 := f.publishInit(t, f.initialized(t), 1)
	g2 := f.publishCompact(t, g1)
	g3 := f.publishCompact(t, g2)
	rep := run()
	require.NoError(t, rep.Err)
	require.Equal(t, []int64{g1.rec.Generation, g2.rec.Generation}, rep.Collected.Retired)
	waitOutGrace()

	before := f.store.keys()
	s.setRedirect(redirectTo(http.MethodDelete, other))
	rep = run()
	require.ErrorIs(t, rep.Err, ErrRedirectRefused)
	require.NotEmpty(t, rep.Plan.Objects, "the pass planned deletes")
	require.Equal(t, []int64{g1.rec.Generation}, rep.Plan.Forget)
	require.Empty(t, rep.Collected.Objects)
	require.Empty(t, rep.Collected.Manifests)
	require.Empty(t, rep.Collected.Forgotten, "no history row is forgotten after a refused delete")
	require.Empty(t, other.requests(), "no DELETE reached the other route")
	require.Equal(t, before, f.store.keys())
	expired, err := publication.Expired(ctx, publication.DB(f.db), f.tables, testSchema, testOptions(true).RetentionGrace)
	require.NoError(t, err)
	require.Len(t, expired, 2, "both expired rows are still in the history")

	s.setRedirect(nil)
	rep = run()
	require.NoError(t, rep.Err)
	require.Equal(t, []int64{g1.rec.Generation}, rep.Collected.Forgotten)
	require.Equal(t, f.keysOf(g3), f.store.keys(), fmt.Sprintf("the retry collected the plan: %v", rep.Collected))
}
