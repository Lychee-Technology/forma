package reconcile

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/manifestv2"
)

// TestTheSDKClientFollowsARedirectedDelete is R10-F2's premise: the AWS
// SDK's default HTTP client follows a 307 with the request's method, so a
// DELETE the bound server redirects is sent to the redirect's target, and
// the SDK reports that target's answer as the delete's success. The storage
// binding names only the URL a request is built for.
func TestTheSDKClientFollowsARedirectedDelete(t *testing.T) {
	store := newMemStore(func() int64 { return 1_700_000_000_000 })
	store.set("data/7/k", []byte("x"), 1_700_000_000_000)
	other := newOtherRoute(t)
	s, srv := newS3Server(t, store, "lake")
	s.setRedirect(redirectTo(http.MethodDelete, other))

	raw := &S3Objects{Client: sdkClient(srv.URL), Bucket: "lake"}
	require.NoError(t, raw.Delete(context.Background(), "data/7/k"), "the SDK reports the other route's answer")
	require.Equal(t, []string{"DELETE /lake/data/7/k"}, other.requests(), "the DELETE reached the other route")
	_, err := raw.Endpoint(context.Background())
	require.ErrorIs(t, err, errUnknownDestination, "a store whose client follows redirects cannot tell where its requests land")
}

// TestAPassFollowsNoRedirect is R10-F2: the store a pass lists, reads and
// deletes through (NewS3Objects) refuses every redirect before the
// redirected request is sent. A redirected DELETE fails, and nothing
// reaches the other route; so does a redirected GET or HEAD, which is a
// failure and never a missing object. The store still tells the bucket URL
// its requests are built for.
func TestAPassFollowsNoRedirect(t *testing.T) {
	ctx := context.Background()
	store := newMemStore(func() int64 { return 1_700_000_000_000 })
	store.set("data/7/k", []byte("x"), 1_700_000_000_000)
	other := newOtherRoute(t)
	s, srv := newS3Server(t, store, "lake")
	objects, err := NewS3Objects(sdkClient(srv.URL), "lake")
	require.NoError(t, err)
	endpoint, err := objects.Endpoint(ctx)
	require.NoError(t, err)
	require.Equal(t, srv.URL+"/lake", endpoint)

	listed, err := objects.List(ctx, "data/7/")
	require.NoError(t, err, "an unredirected request is served")
	require.Len(t, listed, 1)

	s.setRedirect(redirectTo(http.MethodDelete, other))
	err = objects.Delete(ctx, "data/7/k")
	require.ErrorIs(t, err, ErrRedirectRefused)
	require.Empty(t, other.requests(), "no DELETE reached the other route")
	require.Equal(t, []string{"data/7/k"}, store.keys())

	for _, method := range []string{http.MethodGet, http.MethodHead} {
		s.setRedirect(redirectTo(method, other))
		var err error
		if method == http.MethodGet {
			_, err = objects.Get(ctx, "data/7/k")
		} else {
			_, err = objects.Exists(ctx, "data/7/k")
		}
		require.Error(t, err, method)
		require.False(t, errors.Is(err, manifestv2.ErrObjectNotFound), "a redirected %s is no proof the object is missing", method)
	}
	require.Empty(t, other.requests())

	s.setRedirect(nil)
	raw, err := objects.Get(ctx, "data/7/k")
	require.NoError(t, err)
	require.True(t, bytes.Equal([]byte("x"), raw))
}
