package main

import (
	"errors"
	"fmt"
	"net/url"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/lychee-technology/forma/internal/s3endpoint"
)

// ErrMergeRouteMismatch reports a compactor whose DuckDB merge engine would
// read the bucket through another route than its S3 client. The client is
// what the writer checks and binds the deployment's storage target with and
// what hashes every merge source against its manifest digest (#347); the
// merge engine reads the same keys again, configured from the flags alone
// (openMergeEngine). A route the SDK takes from the environment or shared
// config, which the merge engine never sees, would make the merge read other
// bytes than the ones checked.
var ErrMergeRouteMismatch = errors.New("the compactor's merge engine would read the bucket through another route than its S3 client")

// checkMergeRoute refuses a run whose S3 client and merge engine are not
// configured with the same endpoint, region, scheme and routing settings. The
// merge engine takes --s3-endpoint (its scheme dropped), --s3-region,
// --s3-use-ssl and --s3-use-path, and has no FIPS, dual-stack or
// acceleration setting; the client takes the same flags, but the SDK fills
// what they leave unset from AWS_ENDPOINT_URL_S3, AWS_ENDPOINT_URL, a shared
// config endpoint_url, AWS_REGION, AWS_USE_FIPS_ENDPOINT and
// AWS_USE_DUALSTACK_ENDPOINT.
func checkMergeRoute(c *s3.Client, s s3Flags) error {
	o := c.Options()
	if got := aws.ToString(o.BaseEndpoint); got != s.endpoint {
		return fmt.Errorf("the S3 client sends to %s, which it took from AWS_ENDPOINT_URL_S3, AWS_ENDPOINT_URL or a shared "+
			"config endpoint_url, and the merge engine reads through %s (--s3-endpoint): pass that endpoint as --s3-endpoint: %w",
			describeEndpoint(got, o.Region), describeEndpoint(s.endpoint, s.region), ErrMergeRouteMismatch)
	}
	if o.Region != s.region {
		return fmt.Errorf("the S3 client is in region %q and the merge engine in region %q (--s3-region): pass the region as --s3-region: %w",
			o.Region, s.region, ErrMergeRouteMismatch)
	}
	if o.EndpointOptions.UseFIPSEndpoint == aws.FIPSEndpointStateEnabled ||
		o.EndpointOptions.UseDualStackEndpoint == aws.DualStackEndpointStateEnabled ||
		o.UseDualstack || //nolint:staticcheck // SA1019: the legacy setting still routes the client
		o.UseAccelerate {
		return fmt.Errorf("the S3 client uses a FIPS, dual-stack or accelerated endpoint (AWS_USE_FIPS_ENDPOINT, "+
			"AWS_USE_DUALSTACK_ENDPOINT or shared config), which the merge engine has no setting for: pass that endpoint "+
			"as --s3-endpoint and turn the setting off: %w", ErrMergeRouteMismatch)
	}
	scheme, shown := clientScheme(o, s.endpoint)
	if scheme == "" {
		return nil // the client's endpoint check refuses a malformed one (s3endpoint.ClientEndpoint)
	}
	if (scheme == "https") != s.useSSL {
		return fmt.Errorf("the S3 client sends to %s over %s and the merge engine uses %s (--s3-use-ssl=%t): "+
			"make --s3-use-ssl agree with the client's scheme: %w",
			shown, scheme, map[bool]string{true: "https", false: "http"}[s.useSSL], s.useSSL, ErrMergeRouteMismatch)
	}
	return nil
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
