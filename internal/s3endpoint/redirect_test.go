package s3endpoint

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// route is an HTTP server that records the requests it is sent.
type route struct {
	*httptest.Server
	mu   sync.Mutex
	seen []string
}

func newRoute(t *testing.T, h func(w http.ResponseWriter, r *http.Request)) *route {
	t.Helper()
	rt := &route{}
	rt.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rt.mu.Lock()
		rt.seen = append(rt.seen, r.Method+" "+r.URL.Path)
		rt.mu.Unlock()
		h(w, r)
	}))
	t.Cleanup(rt.Close)
	return rt
}

func (rt *route) requests() []string {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return append([]string(nil), rt.seen...)
}

// redirecting answers every request with a 307 to other, and other answers
// every request with an empty 200.
func redirecting(t *testing.T) (bound, other *route) {
	other = newRoute(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	bound = newRoute(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", other.URL+r.URL.RequestURI())
		w.WriteHeader(http.StatusTemporaryRedirect)
	})
	return bound, other
}

func pathClient(endpoint string, h s3.HTTPClient) *s3.Client {
	return newClient(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(endpoint), UsePathStyle: true, HTTPClient: h, Retryer: aws.NopRetryer{}})
}

// TestTheSDKClientFollowsARedirectedGet demonstrates the premise: the SDK's
// default HTTP client follows a 307, so a GET the bound route redirects is
// answered by another route.
func TestTheSDKClientFollowsARedirectedGet(t *testing.T) {
	bound, other := redirecting(t)
	c := pathClient(bound.URL, awshttp.NewBuildableClient())
	_, err := c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("lake"), Key: aws.String("k")})
	require.NoError(t, err, "the other route's 200 is taken for the bound route's")
	require.Equal(t, []string{"GET /lake/k"}, other.requests())
	require.False(t, FollowsNoRedirect(c))
}

// TestAClientWithoutRedirectsSendsNothingElsewhere: a writer's PUT and GET
// and a collector's DELETE and LIST, redirected, fail with
// ErrRedirectRefused, and nothing reaches the other route. A PUT's failure is
// what keeps a writer from publishing a generation whose object the bound
// route never stored.
func TestAClientWithoutRedirectsSendsNothingElsewhere(t *testing.T) {
	ctx := context.Background()
	bound, other := redirecting(t)
	for name, h := range map[string]s3.HTTPClient{
		"default":            nil,
		"buildable":          awshttp.NewBuildableClient(),
		"http.Client":        &http.Client{},
		"already unfollowed": nil,
	} {
		c, err := WithoutRedirects(pathClient(bound.URL, h))
		require.NoError(t, err, name)
		if name == "already unfollowed" {
			c, err = WithoutRedirects(c)
			require.NoError(t, err, name)
		}
		require.True(t, FollowsNoRedirect(c), name)
		b, k := aws.String("lake"), aws.String("data/7/k")
		calls := map[string]func() error{
			"PUT": func() error {
				_, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: b, Key: k, Body: strings.NewReader("x"), IfNoneMatch: aws.String("*")})
				return err
			},
			"GET":    func() error { _, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: b, Key: k}); return err },
			"DELETE": func() error { _, err := c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: b, Key: k}); return err },
			"LIST":   func() error { _, err := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: b}); return err },
		}
		for method, call := range calls {
			require.ErrorIs(t, call(), ErrRedirectRefused, "%s: %s", name, method)
		}
	}
	require.Empty(t, other.requests(), "no request reached the other route")
}

// TestWithoutRedirectsRefusesAnHTTPClientItCannotControl: a caller's own
// HTTP client may follow redirects however it likes.
func TestWithoutRedirectsRefusesAnHTTPClientItCannotControl(t *testing.T) {
	_, err := WithoutRedirects(pathClient("http://s3.example", ownHTTPClient{}))
	require.ErrorIs(t, err, ErrUnknownDestination)
}

type ownHTTPClient struct{}

func (ownHTTPClient) Do(*http.Request) (*http.Response, error) { return nil, http.ErrNotSupported }

// TestARedirectReachingTheClientIsRefusedToo verifies the CheckRedirect
// backstop formats the same sanitized error without Location details.
func TestARedirectReachingTheClientIsRefusedToo(t *testing.T) {
	via, err := http.NewRequest(http.MethodDelete, "https://s3.example/lake/data/7/k?x-id=DeleteObject", nil)
	require.NoError(t, err)
	next, err := http.NewRequest(http.MethodDelete, "https://u:pw-secret@other.example/k?sig=secret", nil)
	require.NoError(t, err)
	next.Response = &http.Response{StatusCode: http.StatusPermanentRedirect}
	err = refuseRedirect(next, []*http.Request{via})
	require.ErrorIs(t, err, ErrRedirectRefused)
	require.Equal(t, "DELETE https://s3.example/lake/data/7/k answered 308 with a redirect (Location omitted): "+
		ErrRedirectRefused.Error(), err.Error())
}
