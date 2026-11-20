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
	s, run := boundS3Server(t, f)
	g1, g3 := publishTwoExpired(t, f, run)

	before := f.store.keys()
	s.setRedirect(redirectTo(http.MethodDelete, other))
	rep := run(testOptions(true))
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
	rep = run(testOptions(true))
	require.NoError(t, rep.Err)
	require.Equal(t, []int64{g1.rec.Generation}, rep.Collected.Forgotten)
	require.Equal(t, f.keysOf(g3), f.store.keys(), fmt.Sprintf("the retry collected the plan: %v", rep.Collected))
}

// TestARedirectedChecksumReadStopsTheCollection is R14-F1: under --gc
// --verify-checksums, the bound server redirects the GET that hashes one of
// the current generation's objects, and answers a HEAD of it with a 404.
// The refused GET proves nothing about the object, and the probe after it
// does not make it a loss: a lost_object finding is not blocking, so the
// pass would go on to delete the expired generations and forget a history
// row. The pass fails with ErrRedirectRefused instead, reports no loss,
// deletes nothing, sends nothing to the redirect's target, and forgets no
// row. With the redirect and the 404 gone, the next pass collects the plan.
func TestARedirectedChecksumReadStopsTheCollection(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	other := newOtherRoute(t)
	s, run := boundS3Server(t, f)
	g1, g3 := publishTwoExpired(t, f, run)
	key := "/" + f.target().Bucket + "/" + g3.manifest.Files[0].Key

	before := f.store.keys()
	s.setRedirect(func(r *http.Request) string {
		if r.Method == http.MethodGet && r.URL.Path == key {
			return other.URL + r.URL.RequestURI()
		}
		return ""
	})
	s.setMissing(func(r *http.Request) bool { return r.Method == http.MethodHead && r.URL.Path == key })
	opts := testOptions(true)
	opts.VerifyChecksums = true
	rep := run(opts)
	require.ErrorIs(t, rep.Err, ErrRedirectRefused, "the refused hash is the pass's failure")
	for _, finding := range rep.Findings {
		require.NotEqual(t, LostObject, finding.Kind, "a refused read is never a loss: %v", finding)
	}
	require.Empty(t, rep.Collected.Objects)
	require.Empty(t, rep.Collected.Manifests)
	require.Empty(t, rep.Collected.Forgotten, "no history row is forgotten")
	require.Empty(t, other.requests(), "nothing reached the redirect's target")
	require.Equal(t, before, f.store.keys(), "nothing is deleted")
	expired, err := publication.Expired(ctx, publication.DB(f.db), f.tables, testSchema, opts.RetentionGrace)
	require.NoError(t, err)
	require.Len(t, expired, 2, "both expired rows are still in the history")

	s.setRedirect(nil)
	s.setMissing(nil)
	rep = run(opts)
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Findings)
	require.Equal(t, []int64{g1.rec.Generation}, rep.Collected.Forgotten)
	require.Equal(t, f.keysOf(g3), f.store.keys(), fmt.Sprintf("the retry collected the plan: %v", rep.Collected))
}

// boundS3Server serves the fixture's bucket over S3 and binds the
// deployment to the URL the store manifest-reconcile builds over it
// (NewS3Objects over the SDK's own client) tells. It answers the server and
// a run of the reconciler through that store.
func boundS3Server(t *testing.T, f *fixture) (*s3Server, func(Options) SchemaReport) {
	ctx := context.Background()
	s, srv := newS3Server(t, f.store, f.target().Bucket)
	objects, err := NewS3Objects(sdkClient(srv.URL), f.target().Bucket)
	require.NoError(t, err)
	endpoint, err := objects.Endpoint(ctx)
	require.NoError(t, err)
	target := publication.StorageTarget{Endpoint: endpoint, Bucket: f.target().Bucket, Prefix: f.layout.Prefix}
	f.exec(t, "DELETE FROM %s", f.tables.Storage)
	_, err = publication.BindStorage(ctx, publication.DB(f.db), f.tables, target)
	require.NoError(t, err)
	return s, func(opts Options) SchemaReport {
		r := f.reconciler(opts)
		r.Objects, r.Storage = objects, target
		report, err := r.Run(ctx)
		require.NoError(t, err)
		require.Len(t, report.Schemas, 1)
		return report.Schemas[0]
	}
}

// publishTwoExpired publishes three generations and has a pass retire the
// first two and their grace pass, so the next pass plans to delete their
// objects and manifests and to forget the lower one. It answers the first
// and the current generation.
func publishTwoExpired(t *testing.T, f *fixture, run func(Options) SchemaReport) (generation, generation) {
	g1 := f.publishInit(t, f.initialized(t), 1)
	g2 := f.publishCompact(t, g1)
	g3 := f.publishCompact(t, g2)
	rep := run(testOptions(true))
	require.NoError(t, rep.Err)
	require.Equal(t, []int64{g1.rec.Generation, g2.rec.Generation}, rep.Collected.Retired)
	waitOutGrace()
	return g1, g3
}
