// Package s3endpoint tells the exact bucket URL an AWS SDK S3 client sends a
// bucket's requests to, and keeps the client's requests there. ClientEndpoint
// is the one derivation of a deployment's storage target
// (publication.StorageTarget.Endpoint): the V2 writers check the binding with
// it before they stage and bind the deployment with it in their first
// publication transaction (internal/cdcv2), and manifest-reconcile checks the target its collector
// lists and deletes in against the same binding with it (internal/reconcile).
// Deriving both from one function keeps virtual-hosted and path-style
// addressing, AWS_ENDPOINT_URL_S3, custom endpoint paths, FIPS, dual-stack and
// acceleration from diverging between the writers and garbage collection.
// WithoutRedirects makes both sides' clients follow no HTTP redirect, so no
// request leaves the URL ClientEndpoint reports.
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

// ClientEndpoint determines the exact bucket URL c sends a bucket's requests
// to, using a no-network transport through c's own SDK middleware and c's own
// routing options (region, base endpoint, path-style addressing, FIPS,
// dual-stack and transfer acceleration): the probe replaces only the
// transport, the credentials and the retryer, none of which picks a route.
// It builds a LIST of bucket and a DELETE, a PUT and a GET of one key in it.
// The three object requests must go to one URL, and the LIST to its bucket
// path, on one scheme and host; the resulting bucket URL must pass
// publication.NormalizeEndpoint (rejecting ambiguous paths such as "/tenant//"
// where virtual-hosted deletes would route to "/tenant//key" rather than
// "/tenant/key"). So the URL is where a writer's objects land and where a
// collector lists and deletes them.
//
// It tells where the first request goes, not where a redirect would take it:
// a client must also follow no redirect (WithoutRedirects) for its requests
// to stay at the URL.
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
	if aws.ToString(o.BaseEndpoint) == "" && o.Region == "" {
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
	list, err := probeRequest(func(opt func(*s3.Options)) error {
		_, err := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)}, opt)
		return err
	})
	if err != nil {
		return "", fmt.Errorf("build a LIST of bucket %q: %w", bucket, err)
	}
	del, err := probeObjectRoute(ctx, c, bucket)
	if err != nil {
		return "", err
	}
	return bucketURL(bucket, list, del)
}

// probeObjectRoute answers the URL c sends a DELETE of a key of bucket to,
// once it has checked that a PUT and a GET of the key go to the same URL.
func probeObjectRoute(ctx context.Context, c *s3.Client, bucket string) (url.URL, error) {
	key := aws.String(endpointProbeKey)
	routes := map[string]func(func(*s3.Options)) error{
		"DELETE": func(opt func(*s3.Options)) error {
			_, err := c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: key}, opt)
			return err
		},
		"PUT": func(opt func(*s3.Options)) error {
			_, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: key}, opt)
			return err
		},
		"GET": func(opt func(*s3.Options)) error {
			_, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: key}, opt)
			return err
		},
	}
	urls := map[string]url.URL{}
	for _, method := range []string{"DELETE", "PUT", "GET"} {
		u, err := probeRequest(routes[method])
		if err != nil {
			return url.URL{}, fmt.Errorf("build a %s in bucket %q: %w", method, bucket, err)
		}
		urls[method] = u
	}
	del := urls["DELETE"]
	for _, method := range []string{"PUT", "GET"} {
		u := urls[method]
		if u.Scheme != del.Scheme || u.Host != del.Host || u.Path != del.Path {
			return url.URL{}, fmt.Errorf("a DELETE in bucket %q goes to %s and a %s to %s: %w",
				bucket, SafeURL(&del), method, SafeURL(&u), ErrUnknownDestination)
		}
	}
	return del, nil
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

// probeRequest builds the one request send makes through the client's own
// middleware and routing options, records its URL and sends nothing.
func probeRequest(send func(func(*s3.Options)) error) (url.URL, error) {
	t := &probeTransport{}
	err := send(func(o *s3.Options) {
		o.HTTPClient = t
		o.Credentials = probeCredentials
		o.Retryer = aws.NopRetryer{}
		o.DisableS3ExpressSessionAuth = aws.Bool(true)
	})
	if len(t.urls) != 1 || !errors.Is(err, errProbed) {
		return url.URL{}, fmt.Errorf("the client built %d request(s), and answered %v: %w", len(t.urls), err, ErrUnknownDestination)
	}
	return t.urls[0], nil
}
