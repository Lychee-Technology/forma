// Package s3endpoint tells where an AWS SDK S3 client sends a bucket's
// requests, and builds clients that follow no HTTP redirect, so that the
// writers (internal/cdcv2) and the garbage collector (internal/reconcile)
// derive a deployment's storage target the same way
// (publication.StorageTarget). It is a package of its own because the
// collector's package imports the writers'.
package s3endpoint

import (
	"bytes"
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

// ErrUnknownDestination reports an S3 client whose destination cannot be
// established unambiguously: a custom endpoint resolver, an ambiguous or
// escaped endpoint path, requests that disagree on where the bucket is, or
// an HTTP client that may follow redirects.
var ErrUnknownDestination = errors.New("the S3 client's destination cannot be told")

const probeKey = "forma-endpoint-probe"

// ClientEndpoint answers the bucket URL c sends bucket's requests to: the
// scheme, host and path prefix of a LIST of the bucket, and of a GET, a PUT
// and a DELETE of an object in it, which must all agree. The requests are
// built by c's own SDK middleware, under c's own routing options (base
// endpoint, path-style addressing, FIPS, dual-stack, transfer acceleration,
// region), and captured before anything is sent: nothing reaches the network
// and no credential is used. The URL passes publication.NormalizeEndpoint,
// which refuses ambiguous paths such as "/tenant//" rather than collapsing
// distinct destinations into one.
func ClientEndpoint(ctx context.Context, c *s3.Client, bucket string) (string, error) {
	if err := checkResolvable(c, bucket); err != nil {
		return "", fmt.Errorf("tell where the S3 client sends bucket %q's requests: %w", bucket, err)
	}
	list, err := probe(func(opt func(*s3.Options)) error {
		_, err := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)}, opt)
		return err
	})
	if err != nil {
		return "", fmt.Errorf("build a LIST of bucket %q: %w", bucket, err)
	}
	basePath, err := objectBase(ctx, c, bucket, list)
	if err != nil {
		return "", fmt.Errorf("tell where the S3 client sends bucket %q's object requests: %w", bucket, err)
	}
	if list.EscapedPath() != list.Path {
		return "", fmt.Errorf("bucket %q endpoint uses an escaped path (%s): %w", bucket, SafeURL(&list), ErrUnknownDestination)
	}
	raw := (&url.URL{Scheme: list.Scheme, Host: list.Host, Path: basePath}).String()
	norm, err := publication.NormalizeEndpoint(raw)
	if err != nil {
		return "", fmt.Errorf("bucket %q endpoint %q is ambiguous: %w: %w", bucket, raw, err, ErrUnknownDestination)
	}
	return norm, nil
}

// checkResolvable refuses a client whose endpoint the SDK's default
// resolver does not derive, or whose base endpoint carries an escaped path.
func checkResolvable(c *s3.Client, bucket string) error {
	o := c.Options()
	if o.EndpointResolver != nil { //nolint:staticcheck // SA1019: a legacy custom resolver must be refused
		return fmt.Errorf("a client with a custom EndpointResolver: %w", ErrUnknownDestination)
	}
	if reflect.TypeOf(o.EndpointResolverV2) != reflect.TypeOf(s3.NewDefaultEndpointResolverV2()) {
		return fmt.Errorf("a client with an EndpointResolverV2 of type %T: %w", o.EndpointResolverV2, ErrUnknownDestination)
	}
	if bucket == "" {
		return fmt.Errorf("a client for no bucket: %w", ErrUnknownDestination)
	}
	rawBase := aws.ToString(o.BaseEndpoint)
	if rawBase == "" && o.Region == "" {
		return fmt.Errorf("a client with neither a base endpoint nor a region: %w", ErrUnknownDestination)
	}
	if rawBase != "" {
		u, err := url.Parse(rawBase)
		if err != nil {
			return fmt.Errorf("parse base endpoint %q: %w: %w", rawBase, err, ErrUnknownDestination)
		}
		if u.RawPath != "" || strings.Contains(rawBase, "%") {
			return fmt.Errorf("base endpoint %q uses an escaped path: %w", rawBase, ErrUnknownDestination)
		}
	}
	return nil
}

// objectBase answers the bucket path prefix of c's object requests (a
// DELETE, a GET and a PUT of one key), refusing any that disagrees with the
// others or with the LIST.
func objectBase(ctx context.Context, c *s3.Client, bucket string, list url.URL) (string, error) {
	b, k := aws.String(bucket), aws.String(probeKey)
	requests := []struct {
		method string
		send   func(func(*s3.Options)) error
	}{
		{"DELETE", func(opt func(*s3.Options)) error {
			_, err := c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: b, Key: k}, opt)
			return err
		}},
		{"GET", func(opt func(*s3.Options)) error {
			_, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: b, Key: k}, opt)
			return err
		}},
		{"PUT", func(opt func(*s3.Options)) error {
			_, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: b, Key: k, Body: bytes.NewReader(nil), IfNoneMatch: aws.String("*")}, opt)
			return err
		}},
	}
	suffix := "/" + probeKey
	basePath := ""
	for i, r := range requests {
		u, err := probe(r.send)
		if err != nil {
			return "", fmt.Errorf("build a %s in bucket %q: %w", r.method, bucket, err)
		}
		p := strings.TrimSuffix(u.Path, suffix)
		if i == 0 {
			basePath = p
		}
		if u.Scheme != list.Scheme || u.Host != list.Host || !strings.HasSuffix(u.Path, suffix) || p != basePath ||
			strings.HasSuffix(p, "/") || (list.Path != p && list.Path != p+"/") || u.EscapedPath() != u.Path {
			return "", fmt.Errorf("a LIST of bucket %q goes to %s and a %s in it to %s: %w",
				bucket, SafeURL(&list), r.method, SafeURL(&u), ErrUnknownDestination)
		}
	}
	return basePath, nil
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

// probe builds one request through send and answers its URL. It replaces
// only what sends and signs the request (the HTTP client, the credentials,
// the retryer, the S3 Express session), never what routes it.
func probe(send func(func(*s3.Options)) error) (url.URL, error) {
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
