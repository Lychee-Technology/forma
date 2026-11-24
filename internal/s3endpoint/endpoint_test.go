package s3endpoint

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyendpoints "github.com/aws/smithy-go/endpoints"
	"github.com/stretchr/testify/require"
)

// recorder answers every request with an empty 200 and records its URL: what
// a client really sends, as opposed to what ClientEndpoint's probe captures.
type recorder struct{ urls []url.URL }

func (r *recorder) Do(req *http.Request) (*http.Response, error) {
	r.urls = append(r.urls, *req.URL)
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: http.NoBody, Request: req}, nil
}

func newClient(o s3.Options) *s3.Client {
	o.Credentials = aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
	})
	return s3.New(o)
}

// sent answers the URLs c really sends a PUT, a GET, a DELETE and a LIST of
// bucket to.
func sent(t *testing.T, c *s3.Client, bucket string) []url.URL {
	t.Helper()
	ctx := context.Background()
	rec := &recorder{}
	opt := func(o *s3.Options) { o.HTTPClient = rec; o.Retryer = aws.NopRetryer{} }
	b, k := aws.String(bucket), aws.String("data/7/object.parquet")
	_, _ = c.PutObject(ctx, &s3.PutObjectInput{Bucket: b, Key: k, Body: strings.NewReader("x"), IfNoneMatch: aws.String("*")}, opt)
	_, _ = c.GetObject(ctx, &s3.GetObjectInput{Bucket: b, Key: k}, opt)
	_, _ = c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: b, Key: k}, opt)
	_, _ = c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: b, Prefix: aws.String("data/7/")}, opt)
	require.Len(t, rec.urls, 4, "one request per call")
	return rec.urls
}

// TestClientEndpointIsWhereTheClientSends compares ClientEndpoint with the
// requests the same client sends under every routing option a deployment
// may set, the endpoint-changing ones included: the endpoint it reports is
// the scheme, host and bucket path every PUT, GET, DELETE and LIST goes to.
func TestClientEndpointIsWhereTheClientSends(t *testing.T) {
	cases := map[string]struct {
		opts s3.Options
		want string
	}{
		"region":           {s3.Options{Region: "us-east-1"}, "https://lake.s3.us-east-1.amazonaws.com"},
		"region, path":     {s3.Options{Region: "eu-west-1", UsePathStyle: true}, "https://s3.eu-west-1.amazonaws.com/lake"},
		"other partition":  {s3.Options{Region: "cn-north-1"}, "https://lake.s3.cn-north-1.amazonaws.com.cn"},
		"FIPS":             {s3.Options{Region: "us-east-1", EndpointOptions: s3.EndpointResolverOptions{UseFIPSEndpoint: aws.FIPSEndpointStateEnabled}}, "https://lake.s3-fips.us-east-1.amazonaws.com"},
		"dual-stack":       {s3.Options{Region: "us-east-1", EndpointOptions: s3.EndpointResolverOptions{UseDualStackEndpoint: aws.DualStackEndpointStateEnabled}}, "https://lake.s3.dualstack.us-east-1.amazonaws.com"},
		"FIPS, dual-stack": {s3.Options{Region: "us-east-1", EndpointOptions: s3.EndpointResolverOptions{UseFIPSEndpoint: aws.FIPSEndpointStateEnabled, UseDualStackEndpoint: aws.DualStackEndpointStateEnabled}}, "https://lake.s3-fips.dualstack.us-east-1.amazonaws.com"},
		"accelerate":       {s3.Options{Region: "us-east-1", UseAccelerate: true}, "https://lake.s3-accelerate.amazonaws.com"},
		"base endpoint, path": {s3.Options{Region: "us-east-1", BaseEndpoint: aws.String("http://rustfs:9000"), UsePathStyle: true},
			"http://rustfs:9000/lake"},
		"base endpoint, virtual": {s3.Options{Region: "us-east-1", BaseEndpoint: aws.String("https://s3.storage.example")},
			"https://lake.s3.storage.example"},
		"base endpoint with a path": {s3.Options{Region: "us-east-1", BaseEndpoint: aws.String("https://gw.example/tenant"), UsePathStyle: true},
			"https://gw.example/tenant/lake"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			c := newClient(tc.opts)
			endpoint, err := ClientEndpoint(context.Background(), c, "lake")
			require.NoError(t, err)
			require.Equal(t, tc.want, endpoint)
			for _, u := range sent(t, c, "lake") {
				got := (&url.URL{Scheme: u.Scheme, Host: u.Host, Path: u.Path}).String()
				require.True(t, got == endpoint || strings.HasPrefix(got, endpoint+"/"),
					"a request to %s is not under the reported endpoint %s", got, endpoint)
			}
		})
	}
}

// TestClientEndpointRefusesResolversItCannotFollow: a custom endpoint
// resolver may route each request anywhere, so no endpoint is reported.
func TestClientEndpointRefusesResolversItCannotFollow(t *testing.T) {
	custom := newClient(s3.Options{Region: "us-east-1", EndpointResolverV2: customResolver{}})
	_, err := ClientEndpoint(context.Background(), custom, "lake")
	require.ErrorIs(t, err, ErrUnknownDestination)
	_, err = ClientEndpoint(context.Background(), newClient(s3.Options{}), "lake")
	require.ErrorIs(t, err, ErrUnknownDestination, "neither a base endpoint nor a region")
	_, err = ClientEndpoint(context.Background(), newClient(s3.Options{Region: "us-east-1"}), "")
	require.ErrorIs(t, err, ErrUnknownDestination, "no bucket")
}

type customResolver struct{}

func (customResolver) ResolveEndpoint(ctx context.Context, p s3.EndpointParameters) (smithyendpoints.Endpoint, error) {
	return s3.NewDefaultEndpointResolverV2().ResolveEndpoint(ctx, p)
}

// TestClientEndpointRejectsAmbiguousPaths verifies R15-F1 directly on
// ClientEndpoint: custom endpoints with repeated or ambiguous path slashes
// never normalize to the same binding as a clean path.
func TestClientEndpointRejectsAmbiguousPaths(t *testing.T) {
	ctx := context.Background()
	cleanClient := func(base string, pathStyle bool) *s3.Client {
		return s3.New(s3.Options{
			Region:       "us-east-1",
			BaseEndpoint: aws.String(base),
			UsePathStyle: pathStyle,
		})
	}
	epTenant, err := ClientEndpoint(ctx, cleanClient("https://s3.storage.example/tenant", false), "lake")
	require.NoError(t, err)
	require.Equal(t, "https://lake.s3.storage.example/tenant", epTenant)

	epPath, err := ClientEndpoint(ctx, cleanClient("https://s3.storage.example/tenant", true), "lake")
	require.NoError(t, err)
	require.Equal(t, "https://s3.storage.example/tenant/lake", epPath)
	require.NotEqual(t, epTenant, epPath)

	for _, bad := range []string{
		"https://s3.storage.example/tenant//",
		"https://s3.storage.example//",
		"https://s3.storage.example/tenant/..",
		"https://s3.storage.example/tenant%2Fa",
	} {
		for _, pathStyle := range []bool{false, true} {
			_, err := ClientEndpoint(ctx, cleanClient(bad, pathStyle), "lake")
			require.ErrorIs(t, err, ErrUnknownDestination, "base=%q pathStyle=%v", bad, pathStyle)
		}
	}
}
