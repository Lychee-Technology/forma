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

	"github.com/lychee-technology/forma/internal/publication"
)

// errUnknownDestination reports an S3 client whose effective destination cannot
// be established unambiguously for the storage binding.
var errUnknownDestination = errors.New("the S3 client's destination cannot be told")

// Destination is implemented by an ObjectStore that can report the exact bucket
// URL its client sends requests to.
type Destination interface {
	Endpoint(ctx context.Context) (string, error)
}

// Endpoint answers the exact bucket URL the store's client sends LIST and
// DELETE requests to (ClientEndpoint), refusing clients that may follow HTTP
// redirects.
func (s *S3Objects) Endpoint(ctx context.Context) (string, error) {
	c, ok := s.Client.(*s3.Client)
	if !ok {
		return "", fmt.Errorf("an S3 client of type %T: %w", s.Client, errUnknownDestination)
	}
	if !followsNoRedirect(c) {
		return "", fmt.Errorf("an S3 client that may follow redirects (build it with NewS3Objects): %w", errUnknownDestination)
	}
	return ClientEndpoint(ctx, c, s.Bucket)
}

const endpointProbeKey = "forma-endpoint-probe"

// ClientEndpoint determines the exact bucket URL c sends LIST and DELETE
// requests for bucket to, using a no-network transport through c's own SDK
// middleware. Both requests must target the same scheme, host and bucket path
// prefix, and the resulting bucket URL must pass publication.NormalizeEndpoint
// (rejecting ambiguous paths such as "/tenant//" where virtual-hosted deletes
// would route to "/tenant//key" rather than "/tenant/key").
func ClientEndpoint(ctx context.Context, c *s3.Client, bucket string) (string, error) {
	o := c.Options()
	if o.EndpointResolver != nil { //nolint:staticcheck // SA1019: legacy custom resolver must be refused
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
	if rawBase := aws.ToString(o.BaseEndpoint); rawBase != "" {
		u, err := url.Parse(rawBase)
		if err != nil {
			return "", fmt.Errorf("parse base endpoint %q: %w: %w", rawBase, err, errUnknownDestination)
		}
		if u.RawPath != "" || strings.Contains(rawBase, "%") {
			return "", fmt.Errorf("base endpoint %q uses an escaped path: %w", rawBase, errUnknownDestination)
		}
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
	suffix := "/" + endpointProbeKey
	if del.Scheme != list.Scheme || del.Host != list.Host || !strings.HasSuffix(del.Path, suffix) {
		return "", fmt.Errorf("a LIST of bucket %q goes to %s and a DELETE in it to %s: %w",
			bucket, safeURL(&list), safeURL(&del), errUnknownDestination)
	}
	basePath := strings.TrimSuffix(del.Path, suffix)
	if strings.HasSuffix(basePath, "/") || (list.Path != basePath && list.Path != basePath+"/") {
		return "", fmt.Errorf("a LIST of bucket %q goes to %s and a DELETE in it to %s: %w",
			bucket, safeURL(&list), safeURL(&del), errUnknownDestination)
	}
	if list.EscapedPath() != list.Path || del.EscapedPath() != del.Path {
		return "", fmt.Errorf("bucket %q endpoint uses an escaped path (%s / %s): %w",
			bucket, safeURL(&list), safeURL(&del), errUnknownDestination)
	}
	raw := (&url.URL{Scheme: list.Scheme, Host: list.Host, Path: basePath}).String()
	norm, err := publication.NormalizeEndpoint(raw)
	if err != nil {
		return "", fmt.Errorf("bucket %q endpoint %q is ambiguous: %w: %w", bucket, raw, err, errUnknownDestination)
	}
	return norm, nil
}

var probeCredentials = aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
	return aws.Credentials{AccessKeyID: "endpoint-probe", SecretAccessKey: "endpoint-probe"}, nil
})

var errProbed = errors.New("the request was recorded, not sent")

type probeTransport struct{ urls []url.URL }

func (p *probeTransport) Do(r *http.Request) (*http.Response, error) {
	p.urls = append(p.urls, *r.URL)
	return nil, errProbed
}

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
