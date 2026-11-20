package reconcile

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// errUnknownDestination reports an S3 client whose destination the storage
// binding cannot name, so a pass through it could list and delete anywhere.
var errUnknownDestination = errors.New("the S3 client's destination cannot be told")

// Destination is implemented by an ObjectStore that can tell where its
// requests go. A pass whose store implements it runs only when that is the
// endpoint of its storage target (checkStorage): the binding is compared
// with what the client that lists and deletes uses, not with a flag.
type Destination interface {
	// Endpoint answers the URL the store sends its bucket's requests to,
	// the bucket's addressing included (ClientEndpoint).
	Endpoint(ctx context.Context) (string, error)
}

// Endpoint answers the URL the store's client sends the requests for its
// bucket to (ClientEndpoint). A client that is not an *s3.Client cannot
// tell, and fails. So does one that may follow a redirect (R10-F2): the URL
// answered here is only the first hop of a request, and the binding would
// not constrain where a redirected DELETE lands. Build the store with
// NewS3Objects.
func (s *S3Objects) Endpoint(ctx context.Context) (string, error) {
	c, ok := s.Client.(*s3.Client)
	if !ok {
		return "", fmt.Errorf("an S3 client of type %T: %w", s.Client, errUnknownDestination)
	}
	if !followsNoRedirect(c) {
		return "", fmt.Errorf("an S3 client that may follow a redirect to a route the binding does not name (build it with NewS3Objects): %w",
			errUnknownDestination)
	}
	return ClientEndpoint(ctx, c, s.Bucket)
}

// endpointProbeKey is the key of the DELETE ClientEndpoint builds and never
// sends.
const endpointProbeKey = "forma-endpoint-probe"

// ClientEndpoint answers the URL c sends the requests for bucket to, as the
// storage binding names it: the bucket's URL, without a key or a query, of
// a LIST and a DELETE that c builds through its own middleware and that a
// transport of ClientEndpoint's records instead of sending. Both must name
// the same bucket URL, or it fails. Three things decide that URL:
//
//   - The base endpoint, when c has one: an option the caller set
//     (--s3-endpoint), or what the SDK took from the configuration without
//     one (AWS_ENDPOINT_URL_S3, AWS_ENDPOINT_URL, a profile's endpoint_url
//     or services section). A flag is therefore not what a run is bound
//     by: an environment that redirects a client whose flag is unchanged
//     redirects the URL answered here too.
//   - The bucket's addressing (UsePathStyle, --s3-use-path). Path-style
//     addressing sends the requests to the endpoint's host with the bucket
//     as the path's first segment, virtual-hosted addressing to a host
//     that starts with the bucket's name. A gateway at a custom endpoint
//     can route the two to different backends, so they are different
//     targets.
//   - Without a base endpoint, the AWS endpoint of c's region, which names
//     the partition a bucket name is unique in. FIPS, dual-stack and
//     acceleration reach the same bucket there, and are left out of the
//     probe. At a base endpoint they stay as c has them.
//
// The probe sends nothing: its requests carry placeholder credentials, no
// retries and no S3 Express session, so building them reads no credential
// and makes no call. A client whose endpoint a resolver of the caller's
// chooses, a legacy EndpointResolver or an EndpointResolverV2 other than
// the SDK's, can send each request anywhere, and fails.
func ClientEndpoint(ctx context.Context, c *s3.Client, bucket string) (string, error) {
	o := c.Options()
	if o.EndpointResolver != nil { //nolint:staticcheck // SA1019: a deprecated resolver can still be set, and must be refused
		return "", fmt.Errorf("a client with a custom EndpointResolver: %w", errUnknownDestination)
	}
	if reflect.TypeOf(o.EndpointResolverV2) != reflect.TypeOf(s3.NewDefaultEndpointResolverV2()) {
		return "", fmt.Errorf("a client with an EndpointResolverV2 of type %T: %w", o.EndpointResolverV2, errUnknownDestination)
	}
	if bucket == "" {
		return "", fmt.Errorf("a client for no bucket: %w", errUnknownDestination)
	}
	atAWS := aws.ToString(o.BaseEndpoint) == ""
	if atAWS && o.Region == "" {
		return "", fmt.Errorf("a client with neither a base endpoint nor a region: %w", errUnknownDestination)
	}
	list, err := probeRequest(atAWS, func(opt func(*s3.Options)) error {
		_, err := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)}, opt)
		return err
	})
	if err != nil {
		return "", fmt.Errorf("build a LIST of bucket %q: %w", bucket, err)
	}
	del, err := probeRequest(atAWS, func(opt func(*s3.Options)) error {
		_, err := c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(endpointProbeKey)}, opt)
		return err
	})
	if err != nil {
		return "", fmt.Errorf("build a DELETE in bucket %q: %w", bucket, err)
	}
	at := url.URL{Scheme: list.Scheme, Host: list.Host, Path: strings.TrimSuffix(list.Path, "/")}
	if del.Scheme != at.Scheme || del.Host != at.Host || del.Path != at.Path+"/"+endpointProbeKey {
		return "", fmt.Errorf("a LIST of bucket %q goes to %s and a DELETE in it to %s: %w",
			bucket, at.String(), (&url.URL{Scheme: del.Scheme, Host: del.Host, Path: del.Path}).String(), errUnknownDestination)
	}
	return at.String(), nil
}

// probeCredentials sign a probe's requests without reading the client's
// credentials, which may take a call to fetch.
var probeCredentials = aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
	return aws.Credentials{AccessKeyID: "endpoint-probe", SecretAccessKey: "endpoint-probe"}, nil
})

// errProbed is what probeTransport answers every request with.
var errProbed = errors.New("the request was recorded, not sent")

// probeTransport records the URL of every request and sends none.
type probeTransport struct{ urls []url.URL }

func (p *probeTransport) Do(r *http.Request) (*http.Response, error) {
	p.urls = append(p.urls, *r.URL)
	return nil, errProbed
}

// probeRequest runs send with the options of a probe and answers the URL of
// the one request it built. atAWS leaves FIPS, dual-stack and acceleration
// out (ClientEndpoint).
func probeRequest(atAWS bool, send func(func(*s3.Options)) error) (url.URL, error) {
	t := &probeTransport{}
	err := send(func(o *s3.Options) {
		o.HTTPClient = t
		o.Credentials = probeCredentials
		o.Retryer = aws.NopRetryer{}
		o.DisableS3ExpressSessionAuth = aws.Bool(true)
		if atAWS {
			o.EndpointOptions.UseFIPSEndpoint = aws.FIPSEndpointStateDisabled
			o.EndpointOptions.UseDualStackEndpoint = aws.DualStackEndpointStateDisabled
			o.UseAccelerate = false
		}
	})
	if len(t.urls) != 1 || !errors.Is(err, errProbed) {
		return url.URL{}, fmt.Errorf("the client built %d request(s), and answered %v: %w", len(t.urls), err, errUnknownDestination)
	}
	return t.urls[0], nil
}

var _ Destination = (*S3Objects)(nil)
