// Package s3endpoint tells the exact bucket URL an AWS SDK S3 client sends a
// bucket's requests to. It is the one derivation of a deployment's storage
// target (publication.StorageTarget.Endpoint): the V2 writers bind the
// deployment with it before they stage (internal/cdcv2), and
// manifest-reconcile checks the target its collector lists and deletes in
// against the same binding with it (internal/reconcile). Deriving both from
// one function keeps virtual-hosted and path-style addressing,
// AWS_ENDPOINT_URL_S3 and custom endpoint paths from diverging between the
// writers and garbage collection.
package s3endpoint

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

// ErrUnknownDestination reports an S3 client whose effective destination
// cannot be established unambiguously for the storage binding.
var ErrUnknownDestination = errors.New("the S3 client's destination cannot be told")

const endpointProbeKey = "forma-endpoint-probe"

// ClientEndpoint determines the exact bucket URL c sends LIST and DELETE
// requests for bucket to, using a no-network transport through c's own SDK
// middleware. Both requests must target the same scheme, host and bucket path
// prefix, and the resulting bucket URL must pass publication.NormalizeEndpoint
// (rejecting ambiguous paths such as "/tenant//" where virtual-hosted deletes
// would route to "/tenant//key" rather than "/tenant/key").
//
// A PUT or GET of an object goes to the same URL as the DELETE of one: the
// SDK resolves every object operation of a bucket through the same endpoint
// rules, so the URL is also where a writer's objects land.
func ClientEndpoint(ctx context.Context, c *s3.Client, bucket string) (string, error) {
	o := c.Options()
	if o.EndpointResolver != nil { //nolint:staticcheck // SA1019: legacy custom resolver must be refused
		return "", fmt.Errorf("a client with a custom EndpointResolver: %w", ErrUnknownDestination)
	}
	if reflect.TypeOf(o.EndpointResolverV2) != reflect.TypeOf(s3.NewDefaultEndpointResolverV2()) {
		return "", fmt.Errorf("a client with an EndpointResolverV2 of type %T: %w", o.EndpointResolverV2, ErrUnknownDestination)
	}
	if bucket == "" {
		return "", fmt.Errorf("a client for no bucket: %w", ErrUnknownDestination)
	}
	atAWS := aws.ToString(o.BaseEndpoint) == ""
	if atAWS && o.Region == "" {
		return "", fmt.Errorf("a client with neither a base endpoint nor a region: %w", ErrUnknownDestination)
	}
	if rawBase := aws.ToString(o.BaseEndpoint); rawBase != "" {
		u, err := url.Parse(rawBase)
		if err != nil {
			return "", fmt.Errorf("parse base endpoint %q: %w: %w", rawBase, err, ErrUnknownDestination)
		}
		if u.RawPath != "" || strings.Contains(rawBase, "%") {
			return "", fmt.Errorf("base endpoint %q uses an escaped path: %w", rawBase, ErrUnknownDestination)
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
	return bucketURL(bucket, list, del)
}

// bucketURL answers the bucket URL a LIST of bucket sent to list and a
// DELETE in it sent to del share, refusing requests that disagree on it.
func bucketURL(bucket string, list, del url.URL) (string, error) {
	suffix := "/" + endpointProbeKey
	if del.Scheme != list.Scheme || del.Host != list.Host || !strings.HasSuffix(del.Path, suffix) {
		return "", fmt.Errorf("a LIST of bucket %q goes to %s and a DELETE in it to %s: %w",
			bucket, SafeURL(&list), SafeURL(&del), ErrUnknownDestination)
	}
	basePath := strings.TrimSuffix(del.Path, suffix)
	if strings.HasSuffix(basePath, "/") || (list.Path != basePath && list.Path != basePath+"/") {
		return "", fmt.Errorf("a LIST of bucket %q goes to %s and a DELETE in it to %s: %w",
			bucket, SafeURL(&list), SafeURL(&del), ErrUnknownDestination)
	}
	if list.EscapedPath() != list.Path || del.EscapedPath() != del.Path {
		return "", fmt.Errorf("bucket %q endpoint uses an escaped path (%s / %s): %w",
			bucket, SafeURL(&list), SafeURL(&del), ErrUnknownDestination)
	}
	raw := (&url.URL{Scheme: list.Scheme, Host: list.Host, Path: basePath}).String()
	norm, err := publication.NormalizeEndpoint(raw)
	if err != nil {
		return "", fmt.Errorf("bucket %q endpoint %q is ambiguous: %w: %w", bucket, raw, err, ErrUnknownDestination)
	}
	return norm, nil
}

// SafeURL renders u without its userinfo, query or fragment, so that no
// presigned or credential-bearing part of a request URL reaches a message.
func SafeURL(u *url.URL) string {
	if u == nil {
		return ""
	}
	return (&url.URL{Scheme: u.Scheme, Host: u.Host, Path: u.Path, RawPath: u.RawPath}).String()
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
		return url.URL{}, fmt.Errorf("the client built %d request(s), and answered %v: %w", len(t.urls), err, ErrUnknownDestination)
	}
	return t.urls[0], nil
}
