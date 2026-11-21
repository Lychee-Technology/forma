package reconcile

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// TestARedirectedCollectionDeletesAndForgetsNothingElsewhere verifies that a
// redirected DELETE sends 0 requests to the redirect target, deletes 0 keys,
// and forgets 0 history rows.
func TestARedirectedCollectionDeletesAndForgetsNothingElsewhere(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	other := newOtherRoute(t)
	s, run := boundS3Server(t, f)
	g1, g2, g3 := publishTwoExpired(t, f, run)

	before := f.store.keys()
	s.setRedirect(redirectTo(http.MethodDelete, other))
	rep := run(testOptions(true))
	require.ErrorIs(t, rep.Err, ErrRedirectRefused)
	require.NotEmpty(t, rep.Plan.Objects)
	require.Equal(t, []int64{g1.rec.Generation, g2.rec.Generation}, rep.Plan.Forget)
	require.Empty(t, rep.Collected.Objects)
	require.Empty(t, rep.Collected.Manifests)
	require.Empty(t, rep.Collected.Forgotten)
	require.Empty(t, other.requests())
	require.Equal(t, before, f.store.keys())
	expired, err := publication.Expired(ctx, publication.DB(f.db), f.tables, testSchema, testOptions(true).RetentionGrace)
	require.NoError(t, err)
	require.Len(t, expired, 2)

	s.setRedirect(nil)
	rep = run(testOptions(true))
	require.NoError(t, rep.Err)
	require.Equal(t, []int64{g1.rec.Generation, g2.rec.Generation}, rep.Collected.Forgotten)
	require.Equal(t, f.keysOf(g3), f.store.keys(), fmt.Sprintf("retry collected: %v", rep.Collected))
}

// TestARedirectedChecksumReadStopsTheCollection verifies R14-F1: a redirected
// checksum GET accompanied by a 404 HEAD fails with ErrRedirectRefused and
// never classifies the object as LostObject or proceeds to GC.
func TestARedirectedChecksumReadStopsTheCollection(t *testing.T) {
	f := newFixture(t)
	other := newOtherRoute(t)
	s, run := boundS3Server(t, f)
	g1, g2, g3 := publishTwoExpired(t, f, run)
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
	require.ErrorIs(t, rep.Err, ErrRedirectRefused)
	for _, finding := range rep.Findings {
		require.NotEqual(t, LostObject, finding.Kind)
	}
	require.Empty(t, rep.Collected.Objects)
	require.Empty(t, rep.Collected.Manifests)
	require.Empty(t, rep.Collected.Forgotten)
	require.Empty(t, other.requests())
	require.Equal(t, before, f.store.keys())

	s.setRedirect(nil)
	s.setMissing(nil)
	rep = run(opts)
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Findings)
	require.Equal(t, []int64{g1.rec.Generation, g2.rec.Generation}, rep.Collected.Forgotten)
	require.Equal(t, f.keysOf(g3), f.store.keys())
}

// TestAConfirmingHeadRedirectDuringChecksumScrubFailsWithRedirectRefused
// verifies R15-F2 end-to-end: when GET returns 404 and the confirming HEAD is
// redirected, the pass fails with ErrRedirectRefused (not ErrObjectNotFound
// and not LostObject) and deletes/forgets nothing.
func TestAConfirmingHeadRedirectDuringChecksumScrubFailsWithRedirectRefused(t *testing.T) {
	f := newFixture(t)
	other := newOtherRoute(t)
	s, run := boundS3Server(t, f)
	_, _, g3 := publishTwoExpired(t, f, run)
	key := "/" + f.target().Bucket + "/" + g3.manifest.Files[0].Key

	before := f.store.keys()
	s.setMissing(func(r *http.Request) bool { return r.Method == http.MethodGet && r.URL.Path == key })
	s.setRedirect(func(r *http.Request) string {
		if r.Method == http.MethodHead && r.URL.Path == key {
			return other.URL + r.URL.RequestURI()
		}
		return ""
	})
	opts := testOptions(true)
	opts.VerifyChecksums = true
	rep := run(opts)
	require.ErrorIs(t, rep.Err, ErrRedirectRefused, "confirming HEAD redirect must not be masked by GET's 404")
	require.NotErrorIs(t, rep.Err, manifestv2.ErrObjectNotFound)
	for _, finding := range rep.Findings {
		require.NotEqual(t, LostObject, finding.Kind)
	}
	require.Empty(t, rep.Collected.Objects)
	require.Empty(t, rep.Collected.Manifests)
	require.Empty(t, rep.Collected.Forgotten)
	require.Empty(t, other.requests())
	require.Equal(t, before, f.store.keys())
}

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

func publishTwoExpired(t *testing.T, f *fixture, run func(Options) SchemaReport) (generation, generation, generation) {
	g1 := f.publishInit(t, f.initialized(t), 1)
	g2 := f.publishCompact(t, g1)
	g3 := f.publishCompact(t, g2)
	rep := run(testOptions(true))
	require.NoError(t, rep.Err)
	require.Equal(t, []int64{g1.rec.Generation, g2.rec.Generation}, rep.Collected.Retired)
	waitOutGrace()
	return g1, g2, g3
}
