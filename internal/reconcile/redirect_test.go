package reconcile

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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

// TestARefusedRedirectNamesNoLocation is R13-F2: a redirect's Location can
// carry a credential, a signed URL's query or a user-info password, which
// URL.Redacted does not mask, and http.Client would put it whole into the
// *url.Error it answers. The error a refused redirect surfaces names the
// request's method and its URL without query, and none of the Location,
// whichever method was redirected and whether the Location is absolute or
// relative. It is still ErrRedirectRefused, the request is sent once (the
// SDK's retryer does not resend it), and no redirected request is sent.
func TestARefusedRedirectNamesNoLocation(t *testing.T) {
	ctx := context.Background()
	const secret = "dummy-signature-R13F2"
	var reached, served atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(other.Close)
	locations := []string{
		strings.Replace(other.URL, "http://", "http://user:"+secret+"@", 1) + "/lake/data/7/k?X-Amz-Signature=" + secret,
		"/elsewhere/data/7/k?X-Amz-Credential=" + secret + "#" + secret,
	}
	for _, location := range locations {
		bound := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			served.Add(1)
			w.Header().Set("Location", location)
			w.WriteHeader(http.StatusTemporaryRedirect)
		}))
		t.Cleanup(bound.Close)
		objects, err := NewS3Objects(sdkClient(bound.URL), "lake")
		require.NoError(t, err)
		calls := map[string]func() error{
			http.MethodDelete: func() error { return objects.Delete(ctx, "data/7/k") },
			http.MethodGet:    func() error { _, err := objects.Get(ctx, "data/7/k"); return err },
			http.MethodHead:   func() error { _, err := objects.Exists(ctx, "data/7/k"); return err },
			"LIST":            func() error { _, err := objects.List(ctx, "data/7/"); return err },
		}
		for name, call := range calls {
			served.Store(0)
			err := call()
			require.ErrorIs(t, err, ErrRedirectRefused, name)
			require.NotContains(t, err.Error(), secret, "%s: the surfaced error names no part of the Location", name)
			require.NotContains(t, err.Error(), "elsewhere", name)
			require.NotContains(t, err.Error(), other.URL[len("http://"):], name)
			require.Contains(t, err.Error(), bound.URL+"/lake", name)
			require.EqualValues(t, 1, served.Load(), "%s: a refused redirect is not retried", name)
			var uerr *url.Error
			require.False(t, errors.As(err, &uerr), "%s: no *url.Error carries the Location", name)
		}
	}
	require.Zero(t, reached.Load(), "no redirected request was sent")
}

// TestARedirectWithAStalledBodyIsRefusedPromptly is R14-F2: the bound
// server sends a redirect's headers, and a few bytes of a body it says is
// longer, flushes them and sends no more. A refusal that waited for body
// bytes, even a bounded number, would hold the pass, and the schema's lock,
// until its context ended, since the HTTP client's timeout is unset by
// default. Every call is refused at once, with ErrRedirectRefused, the
// connection is closed rather than left waiting, and no redirected request
// is sent.
func TestARedirectWithAStalledBodyIsRefusedPromptly(t *testing.T) {
	var reached, closed atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(other.Close)
	release := make(chan struct{})
	bound := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", other.URL+r.URL.RequestURI())
		w.Header().Set("Content-Length", "65536")
		w.WriteHeader(http.StatusTemporaryRedirect)
		_, _ = io.WriteString(w, "<Error>")
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
			closed.Add(1)
		case <-release:
		}
	}))
	t.Cleanup(bound.Close)
	t.Cleanup(func() { close(release) }) // runs first: no handler outlives the test
	objects, err := NewS3Objects(sdkClient(bound.URL), "lake")
	require.NoError(t, err)

	calls := map[string]func(context.Context) error{
		http.MethodDelete: func(ctx context.Context) error { return objects.Delete(ctx, "data/7/k") },
		http.MethodGet:    func(ctx context.Context) error { _, err := objects.Get(ctx, "data/7/k"); return err },
		"hash":            func(ctx context.Context) error { _, _, err := objects.Hash(ctx, "data/7/k"); return err },
		"LIST":            func(ctx context.Context) error { _, err := objects.List(ctx, "data/7/"); return err },
	}
	for name, call := range calls {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		start := time.Now()
		err := call(ctx)
		elapsed := time.Since(start)
		cancel()
		require.ErrorIs(t, err, ErrRedirectRefused, name)
		require.NotErrorIs(t, err, context.DeadlineExceeded, name)
		require.Less(t, elapsed, 5*time.Second, "%s: the refusal waits for no body byte", name)
	}
	require.Eventually(t, func() bool { return closed.Load() == int32(len(calls)) }, 5*time.Second, 10*time.Millisecond,
		"each stalled response's connection is closed by the refusal")
	require.Zero(t, reached.Load(), "no redirected request was sent")
}

// TestARedirectReachingTheClientIsRefusedToo: should a redirect reach the
// http.Client behind the transport, its CheckRedirect refuses it with the
// same error, which names no part of the Location either.
func TestARedirectReachingTheClientIsRefusedToo(t *testing.T) {
	via, err := http.NewRequest(http.MethodDelete, "https://s3.example/lake/data/7/k?x-id=DeleteObject", nil)
	require.NoError(t, err)
	next, err := http.NewRequest(http.MethodDelete, "https://u:pw-secret@other.example/k?sig=secret", nil)
	require.NoError(t, err)
	next.Response = &http.Response{StatusCode: http.StatusPermanentRedirect}
	err = refuseRedirect(next, []*http.Request{via})
	require.ErrorIs(t, err, ErrRedirectRefused)
	require.Equal(t, "DELETE https://s3.example/lake/data/7/k answered 308 with a redirect (its Location is not shown, since it may carry a credential): "+
		ErrRedirectRefused.Error(), err.Error())
}
