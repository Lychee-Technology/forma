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

// TestTheSDKClientFollowsARedirectedDelete demonstrates the premise: the AWS
// SDK's default HTTP client follows a 307 with the request's method, so a
// DELETE the bound server redirects reaches the redirect target.
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

// TestAPassFollowsNoRedirect verifies that NewS3Objects refuses every redirect
// before any redirected request is sent across LIST, GET, HEAD, and DELETE.
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
		require.ErrorIs(t, err, ErrRedirectRefused, method)
		require.False(t, errors.Is(err, manifestv2.ErrObjectNotFound), "a redirected %s is no proof the object is missing", method)
	}
	require.Empty(t, other.requests())

	s.setRedirect(nil)
	raw, err := objects.Get(ctx, "data/7/k")
	require.NoError(t, err)
	require.True(t, bytes.Equal([]byte("x"), raw))
}

// TestARefusedRedirectNamesNoLocation verifies that redirect refusal errors
// never leak Location header values (which can carry credentials or signatures)
// and are not retried by the SDK.
func TestARefusedRedirectNamesNoLocation(t *testing.T) {
	ctx := context.Background()
	const secret = "dummy-signature-redacted"
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
			require.NotContains(t, err.Error(), secret, "%s: surfaced error must not contain Location", name)
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

// TestARedirectWithAStalledBodyIsRefusedPromptly verifies that a redirect with
// a stalled body is refused immediately without draining the body, closing the
// connection so the pass and schema lock do not hang.
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
	t.Cleanup(func() { close(release) })
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
		require.Less(t, elapsed, 5*time.Second, "%s: refusal must not wait for body bytes", name)
	}
	require.Eventually(t, func() bool { return closed.Load() == int32(len(calls)) }, 5*time.Second, 10*time.Millisecond,
		"each stalled response's connection is closed by the refusal")
	require.Zero(t, reached.Load(), "no redirected request was sent")
}
