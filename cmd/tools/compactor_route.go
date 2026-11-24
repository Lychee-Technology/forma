package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/lychee-technology/forma/internal/s3endpoint"
)

// ErrMergeRouteMismatch reports a compactor whose DuckDB merge engine would
// read the bucket through another route than its S3 client. The client is
// what the writer checks and binds the deployment's storage target with and
// what hashes every merge source against its manifest digest (#347); the
// merge engine reads the same keys again, configured from the flags and,
// when they name no endpoint, the client's endpoint for the region
// (openMergeEngine). A route the SDK takes from the environment or shared
// config, which the merge engine never sees, would make the merge read other
// bytes than the ones checked.
var ErrMergeRouteMismatch = errors.New("the compactor's merge engine would read the bucket through another route than its S3 client")

// checkMergeRoute refuses a run whose S3 client and merge engine are not
// configured with the same endpoint, region, scheme and routing settings, or
// do not read the bucket from the same URL, and returns the endpoint to
// configure the merge engine with (mergeEngineEndpoint). The merge engine
// takes --s3-endpoint (its scheme dropped), --s3-region, --s3-use-ssl and
// --s3-use-path, and has no FIPS, dual-stack or acceleration setting; the
// client takes the same flags, but the SDK fills what they leave unset from
// AWS_ENDPOINT_URL_S3, AWS_ENDPOINT_URL, a shared config endpoint_url,
// AWS_REGION, AWS_USE_FIPS_ENDPOINT and AWS_USE_DUALSTACK_ENDPOINT. Equal
// flags are not yet equal routes: the merge engine addresses the bucket
// exactly as --s3-use-path says, while the SDK, with --s3-use-path=false,
// falls back to path-style addressing for a bucket it cannot put in the host
// name (a dotted bucket, over HTTPS or plain HTTP alike, a name that is not a
// DNS label, an IP endpoint), so the addressing compared is the one the SDK's
// own endpoint rules resolve for the bucket (clientPathStyle). Last, the
// bucket URL the merge engine builds is compared with the one the SDK
// resolves (checkBucketURL).
func checkMergeRoute(c *s3.Client, s s3Flags) (string, error) {
	o := c.Options()
	if got := aws.ToString(o.BaseEndpoint); got != s.endpoint {
		return "", fmt.Errorf("the S3 client sends to %s, which it took from AWS_ENDPOINT_URL_S3, AWS_ENDPOINT_URL or a shared "+
			"config endpoint_url, and the merge engine reads through %s (--s3-endpoint): pass that endpoint as --s3-endpoint: %w",
			describeEndpoint(got, o.Region), describeEndpoint(s.endpoint, s.region), ErrMergeRouteMismatch)
	}
	if o.Region != s.region {
		return "", fmt.Errorf("the S3 client is in region %q and the merge engine in region %q (--s3-region): pass the region as --s3-region: %w",
			o.Region, s.region, ErrMergeRouteMismatch)
	}
	if o.EndpointOptions.UseFIPSEndpoint == aws.FIPSEndpointStateEnabled ||
		o.EndpointOptions.UseDualStackEndpoint == aws.DualStackEndpointStateEnabled ||
		o.UseDualstack || //nolint:staticcheck // SA1019: the legacy setting still routes the client
		o.UseAccelerate {
		return "", fmt.Errorf("the S3 client uses a FIPS, dual-stack or accelerated endpoint (AWS_USE_FIPS_ENDPOINT, "+
			"AWS_USE_DUALSTACK_ENDPOINT or shared config), which the merge engine has no setting for: pass that endpoint "+
			"as --s3-endpoint and turn the setting off: %w", ErrMergeRouteMismatch)
	}
	scheme, shown := clientScheme(o, s.endpoint)
	if scheme == "" {
		return s.endpoint, nil // the client's endpoint check refuses a malformed one (s3endpoint.ClientEndpoint)
	}
	if (scheme == "https") != s.useSSL {
		return "", fmt.Errorf("the S3 client sends to %s over %s and the merge engine uses %s (--s3-use-ssl=%t): "+
			"make --s3-use-ssl agree with the client's scheme: %w",
			shown, scheme, map[bool]string{true: "https", false: "http"}[s.useSSL], s.useSSL, ErrMergeRouteMismatch)
	}
	pathStyle, err := clientPathStyle(o, s.bucket)
	if err != nil {
		return "", fmt.Errorf("resolve the S3 client's route to bucket %q: %w", s.bucket, err)
	}
	if pathStyle != s.usePath {
		style := map[bool]string{true: "path-style", false: "virtual-hosted"}
		return "", fmt.Errorf("the S3 client addresses bucket %q %s on %s and the merge engine %s (--s3-use-path=%t); "+
			"the SDK falls back to path-style addressing for a bucket it cannot name in the host, such as a dotted "+
			"bucket, a name that is not a DNS label or one behind an IP endpoint: pass --s3-use-path=%t: %w",
			s.bucket, style[pathStyle], shown, style[s.usePath], s.usePath, pathStyle, ErrMergeRouteMismatch)
	}
	return checkBucketURL(o, s, scheme)
}

// mergeEngineEndpoint returns the endpoint to configure the merge engine
// with: --s3-endpoint when one is given, and otherwise the endpoint the SDK
// resolves for the region, scheme and host only. Left without an endpoint,
// DuckDB httpfs builds s3.<region>.amazonaws.com in every region, which is
// not the S3 endpoint of the AWS partitions with another DNS suffix: in
// cn-north-1 the SDK reads bkt.s3.cn-north-1.amazonaws.com.cn and httpfs
// would read bkt.s3.cn-north-1.amazonaws.com (R4-F1).
func mergeEngineEndpoint(o s3.Options, s s3Flags) (string, error) {
	if s.endpoint != "" {
		return s.endpoint, nil
	}
	u, err := resolveClientURL(o, nil, false)
	if err != nil {
		return "", fmt.Errorf("resolve the S3 client's endpoint in region %q: %w", o.Region, err)
	}
	if strings.Trim(u.Path, "/") != "" || u.RawQuery != "" {
		return "", fmt.Errorf("the S3 client's endpoint in region %q, %s, has a path the merge engine cannot take: %w",
			o.Region, s3endpoint.SafeURL(u), ErrMergeRouteMismatch)
	}
	return (&url.URL{Scheme: u.Scheme, Host: u.Host}).String(), nil
}

// mergeEngineBucketURL is the bucket URL DuckDB httpfs reads an object of the
// bucket under, configured with endpoint (the scheme dropped), --s3-use-ssl
// and --s3-use-path: the endpoint's host with the bucket as the first path
// segment, or as the first label of the host.
func mergeEngineBucketURL(endpoint string, s s3Flags) string {
	host := strings.TrimPrefix(strings.TrimPrefix(endpoint, "https://"), "http://")
	scheme := map[bool]string{true: "https", false: "http"}[s.useSSL]
	if s.usePath {
		return scheme + "://" + strings.TrimSuffix(host, "/") + "/" + s.bucket
	}
	return scheme + "://" + s.bucket + "." + strings.TrimSuffix(host, "/")
}

// checkBucketURL refuses a merge engine that would read the bucket from
// another URL than the S3 client resolves for it, and returns the endpoint
// the merge engine is configured with. scheme is the one the client sends
// with (clientScheme): the SDK applies DisableHTTPS after it resolves the
// endpoint.
func checkBucketURL(o s3.Options, s s3Flags, scheme string) (string, error) {
	endpoint, err := mergeEngineEndpoint(o, s)
	if err != nil {
		return "", err
	}
	client, err := resolveClientURL(o, aws.String(s.bucket), false)
	if err != nil {
		return "", fmt.Errorf("resolve the S3 client's route to bucket %q: %w", s.bucket, err)
	}
	want := strings.TrimSuffix((&url.URL{Scheme: scheme, Host: client.Host, Path: client.Path}).String(), "/")
	if got := mergeEngineBucketURL(endpoint, s); got != want {
		u, _ := url.Parse(got)
		return "", fmt.Errorf("the S3 client reads bucket %q from %s and the merge engine would read it from %s: "+
			"pass the client's endpoint as --s3-endpoint: %w",
			s.bucket, s3endpoint.SafeURL(client), s3endpoint.SafeURL(u), ErrMergeRouteMismatch)
	}
	return endpoint, nil
}

// clientPathStyle reports whether the S3 client addresses bucket path-style,
// as the SDK's own endpoint rules resolve it for an object request: forced by
// UsePathStyle, or a fallback the rules take when the bucket cannot be a
// virtual host. It resolves the bucket's endpoint once with the client's
// settings and once with path style forced: the client addresses the bucket
// path-style exactly when both resolve to the same URL.
func clientPathStyle(o s3.Options, bucket string) (bool, error) {
	if o.UsePathStyle {
		return true, nil
	}
	actual, err := resolveClientURL(o, aws.String(bucket), false)
	if err != nil {
		return false, err
	}
	forced, err := resolveClientURL(o, aws.String(bucket), true)
	if err != nil {
		return false, err
	}
	return actual.String() == forced.String(), nil
}

// resolveClientURL resolves, with the S3 client's settings and its endpoint
// resolver, the endpoint of bucket (or the service's, with no bucket), path
// style forced or as the client's UsePathStyle says.
func resolveClientURL(o s3.Options, bucket *string, forcePath bool) (*url.URL, error) {
	resolver := o.EndpointResolverV2
	if resolver == nil {
		resolver = s3.NewDefaultEndpointResolverV2()
	}
	ep, err := resolver.ResolveEndpoint(context.Background(), s3.EndpointParameters{
		Bucket:                         bucket,
		Region:                         aws.String(o.Region),
		Endpoint:                       o.BaseEndpoint,
		UseFIPS:                        aws.Bool(o.EndpointOptions.UseFIPSEndpoint == aws.FIPSEndpointStateEnabled),
		UseDualStack:                   aws.Bool(o.EndpointOptions.UseDualStackEndpoint == aws.DualStackEndpointStateEnabled),
		Accelerate:                     aws.Bool(o.UseAccelerate),
		DisableMultiRegionAccessPoints: aws.Bool(o.DisableMultiRegionAccessPoints),
		UseArnRegion:                   aws.Bool(o.UseARNRegion),
		ForcePathStyle:                 aws.Bool(forcePath || o.UsePathStyle),
	})
	if err != nil {
		return nil, fmt.Errorf("resolve the S3 endpoint: %w", err)
	}
	return &ep.URI, nil
}

// clientScheme returns the scheme the S3 client sends with, and the endpoint
// to name in an error. With --s3-endpoint it is that endpoint's scheme; with
// none the SDK resolves the region's AWS default endpoint, over HTTPS unless
// the client disables it, whatever --s3-use-ssl says. It returns no scheme
// for an endpoint that is not a URL.
func clientScheme(o s3.Options, endpoint string) (scheme, shown string) {
	if endpoint == "" {
		if o.EndpointOptions.DisableHTTPS {
			return "http", describeEndpoint("", o.Region)
		}
		return "https", describeEndpoint("", o.Region)
	}
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return "", ""
	}
	return u.Scheme, s3endpoint.SafeURL(u)
}

// describeEndpoint renders a configured endpoint without what may carry a
// credential (s3endpoint.SafeURL), and none as the region's AWS default.
func describeEndpoint(raw, region string) string {
	if raw == "" {
		return fmt.Sprintf("the AWS default endpoint of region %q", region)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "an endpoint that is not a URL (not shown)"
	}
	return s3endpoint.SafeURL(u)
}
