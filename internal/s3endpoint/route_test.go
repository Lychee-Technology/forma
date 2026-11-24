package s3endpoint

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// recorder is an HTTP client that records the URL of every request it is
// handed and answers none, standing in for the network under a client
// configured exactly as a writer's or a collector's is.
type recorder struct{ urls []url.URL }

var errRecorded = errors.New("recorded")

func (r *recorder) Do(req *http.Request) (*http.Response, error) {
	r.urls = append(r.urls, *req.URL)
	return nil, errRecorded
}

// sent answers the URL the client's own request of send went to.
func sent(t *testing.T, rec *recorder, send func() error) url.URL {
	t.Helper()
	rec.urls = nil
	require.ErrorIs(t, send(), errRecorded)
	require.Len(t, rec.urls, 1)
	return rec.urls[0]
}

// TestClientEndpointIsWhereTheClientSendsItsRequests pins R1-F1 of #911: the
// bucket URL ClientEndpoint reports is the one the client itself sends a PUT,
// a GET, a DELETE and a LIST to under every routing option it supports, FIPS,
// dual-stack and transfer acceleration included, so the same binding cannot
// accept two clients that send to different hosts.
func TestClientEndpointIsWhereTheClientSendsItsRequests(t *testing.T) {
	ctx := context.Background()
	const bucket, key = "lake", "data/7/e1/delta/k.parquet"
	cases := map[string]struct {
		opts func(*s3.Options)
		want string
	}{
		"regional":   {func(*s3.Options) {}, "https://lake.s3.us-west-2.amazonaws.com"},
		"path-style": {func(o *s3.Options) { o.UsePathStyle = true }, "https://s3.us-west-2.amazonaws.com/lake"},
		"dual-stack": {func(o *s3.Options) { o.EndpointOptions.UseDualStackEndpoint = aws.DualStackEndpointStateEnabled }, "https://lake.s3.dualstack.us-west-2.amazonaws.com"},
		"fips":       {func(o *s3.Options) { o.EndpointOptions.UseFIPSEndpoint = aws.FIPSEndpointStateEnabled }, "https://lake.s3-fips.us-west-2.amazonaws.com"},
		"fips and dual-stack": {func(o *s3.Options) {
			o.EndpointOptions.UseFIPSEndpoint, o.EndpointOptions.UseDualStackEndpoint = aws.FIPSEndpointStateEnabled, aws.DualStackEndpointStateEnabled
		}, "https://lake.s3-fips.dualstack.us-west-2.amazonaws.com"},
		"accelerate": {func(o *s3.Options) { o.UseAccelerate = true }, "https://lake.s3-accelerate.amazonaws.com"},
		"accelerate and dual-stack": {func(o *s3.Options) {
			o.UseAccelerate, o.EndpointOptions.UseDualStackEndpoint = true, aws.DualStackEndpointStateEnabled
		}, "https://lake.s3-accelerate.dualstack.amazonaws.com"},
		"custom endpoint":            {func(o *s3.Options) { o.BaseEndpoint = aws.String("https://s3.storage.example/tenant") }, "https://lake.s3.storage.example/tenant"},
		"custom endpoint path-style": {func(o *s3.Options) { o.BaseEndpoint, o.UsePathStyle = aws.String("http://127.0.0.1:9000"), true }, "http://127.0.0.1:9000/lake"},
	}
	seen := map[string]string{}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			rec := &recorder{}
			c := s3.New(s3.Options{
				Region:      "us-west-2",
				Credentials: credentials.NewStaticCredentialsProvider("id", "secret", ""),
				Retryer:     aws.NopRetryer{},
				HTTPClient:  rec,
			}, tc.opts)
			endpoint, err := ClientEndpoint(ctx, c, bucket)
			require.NoError(t, err)
			require.Equal(t, tc.want, endpoint)

			object := func(u url.URL) string {
				require.True(t, strings.HasSuffix(u.Path, "/"+key), "%s", u.String())
				return (&url.URL{Scheme: u.Scheme, Host: u.Host, Path: strings.TrimSuffix(u.Path, "/"+key)}).String()
			}
			put := sent(t, rec, func() error {
				_, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: strings.NewReader("x"), IfNoneMatch: aws.String("*")})
				return err
			})
			get := sent(t, rec, func() error {
				_, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
				return err
			})
			del := sent(t, rec, func() error {
				_, err := c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
				return err
			})
			list := sent(t, rec, func() error {
				_, err := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String("data/7/")})
				return err
			})
			require.Equal(t, endpoint, object(put), "PUT")
			require.Equal(t, endpoint, object(get), "GET")
			require.Equal(t, endpoint, object(del), "DELETE")
			require.Equal(t, endpoint, (&url.URL{Scheme: list.Scheme, Host: list.Host, Path: strings.TrimSuffix(list.Path, "/")}).String(), "LIST")
			seen[name] = endpoint
		})
	}
	distinct := map[string]bool{}
	for _, endpoint := range seen {
		distinct[endpoint] = true
	}
	require.Len(t, distinct, len(cases), "each routing option names its own bucket URL: %v", seen)
}

// TestClientEndpointRefusesARouteItCannotTell: a configuration the SDK
// cannot route (FIPS over a custom endpoint) builds no request, and is
// refused rather than reported as the plain endpoint.
func TestClientEndpointRefusesARouteItCannotTell(t *testing.T) {
	c := s3.New(s3.Options{
		Region:       "us-west-2",
		BaseEndpoint: aws.String("https://s3.storage.example"),
		Credentials:  credentials.NewStaticCredentialsProvider("id", "secret", ""),
	}, func(o *s3.Options) { o.EndpointOptions.UseFIPSEndpoint = aws.FIPSEndpointStateEnabled })
	_, err := ClientEndpoint(context.Background(), c, "lake")
	require.ErrorIs(t, err, ErrUnknownDestination)
}

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

// TestWithoutRedirectsKeepsTheRouteAndRefusesUnknownTransports: the copy
// sends where the original does, and a client whose HTTP client type may
// follow redirects on its own is refused.
func TestWithoutRedirectsKeepsTheRouteAndRefusesUnknownTransports(t *testing.T) {
	ctx := context.Background()
	plain := s3.New(s3.Options{Region: "us-west-2", EndpointOptions: s3.EndpointResolverOptions{UseDualStackEndpoint: aws.DualStackEndpointStateEnabled}})
	require.False(t, FollowsNoRedirect(plain))
	bound, err := WithoutRedirects(plain)
	require.NoError(t, err)
	require.True(t, FollowsNoRedirect(bound))
	want, err := ClientEndpoint(ctx, plain, "lake")
	require.NoError(t, err)
	got, err := ClientEndpoint(ctx, bound, "lake")
	require.NoError(t, err)
	require.Equal(t, want, got)

	_, err = WithoutRedirects(s3.New(s3.Options{Region: "us-west-2", HTTPClient: &recorder{}}))
	require.ErrorIs(t, err, ErrUnknownDestination)
}

// TestARedirectedPutIsRefused demonstrates R1-F3's premise and its remedy:
// an SDK client over an ordinary Go HTTP client follows a 303 to another
// route as a GET, and PutObject takes that GET's 200 for its own success
// though nothing was stored. A client built by WithoutRedirects sends nothing to the other
// route and fails the PUT with ErrRedirectRefused.
func TestARedirectedPutIsRefused(t *testing.T) {
	var elsewhere []string
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		elsewhere = append(elsewhere, r.Method)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(other.Close)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", other.URL+r.URL.RequestURI())
		w.WriteHeader(http.StatusSeeOther)
	}))
	t.Cleanup(origin.Close)
	plain := s3.New(s3.Options{
		Region: "us-east-1", BaseEndpoint: aws.String(origin.URL), UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider("id", "secret", ""), Retryer: aws.NopRetryer{},
		HTTPClient: &http.Client{},
	})
	put := func(c *s3.Client) error {
		_, err := c.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String("lake"), Key: aws.String("data/k"), Body: strings.NewReader("x"), IfNoneMatch: aws.String("*"),
		})
		return err
	}
	require.NoError(t, put(plain), "the Go HTTP client reports the other route's GET as the PUT's success")
	require.Equal(t, []string{http.MethodGet}, elsewhere)

	elsewhere = nil
	bound, err := WithoutRedirects(plain)
	require.NoError(t, err)
	require.ErrorIs(t, put(bound), ErrRedirectRefused)
	require.Empty(t, elsewhere, "no request reached the other route")
}
