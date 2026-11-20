package reconcile

import (
	"context"
	"errors"
	"fmt"
	"reflect"

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
	// Endpoint answers the S3 endpoint the store sends its requests to.
	Endpoint(ctx context.Context) (string, error)
}

// Endpoint answers the endpoint the store's client sends its requests to
// (ClientEndpoint). A client that is not an *s3.Client cannot tell, and
// fails.
func (s *S3Objects) Endpoint(ctx context.Context) (string, error) {
	c, ok := s.Client.(*s3.Client)
	if !ok {
		return "", fmt.Errorf("an S3 client of type %T: %w", s.Client, errUnknownDestination)
	}
	return ClientEndpoint(ctx, c)
}

// ClientEndpoint answers the S3 endpoint c sends its requests to, as the
// storage binding names it. That is c's base endpoint when it has one: an
// option the caller set (--s3-endpoint), or what the SDK took from the
// configuration without one (AWS_ENDPOINT_URL_S3, AWS_ENDPOINT_URL, a
// profile's endpoint_url or services section). A flag is therefore not
// what a run is bound by: an environment that redirects a client whose flag
// is unchanged redirects the endpoint answered here too. Without a base
// endpoint it answers the AWS endpoint of c's region, which names the
// partition a bucket name is unique in; FIPS, dual-stack and acceleration
// reach the same bucket and are left out. A client whose endpoint a
// resolver of the caller's chooses, a legacy EndpointResolver or an
// EndpointResolverV2 other than the SDK's, can send its requests anywhere,
// and fails.
func ClientEndpoint(ctx context.Context, c *s3.Client) (string, error) {
	o := c.Options()
	if o.EndpointResolver != nil {
		return "", fmt.Errorf("a client with a custom EndpointResolver: %w", errUnknownDestination)
	}
	if reflect.TypeOf(o.EndpointResolverV2) != reflect.TypeOf(s3.NewDefaultEndpointResolverV2()) {
		return "", fmt.Errorf("a client with an EndpointResolverV2 of type %T: %w", o.EndpointResolverV2, errUnknownDestination)
	}
	if e := aws.ToString(o.BaseEndpoint); e != "" {
		return e, nil
	}
	if o.Region == "" {
		return "", fmt.Errorf("a client with neither a base endpoint nor a region: %w", errUnknownDestination)
	}
	ep, err := s3.NewDefaultEndpointResolverV2().ResolveEndpoint(ctx, s3.EndpointParameters{
		Region: aws.String(o.Region), UseFIPS: aws.Bool(false), UseDualStack: aws.Bool(false), Accelerate: aws.Bool(false),
	})
	if err != nil {
		return "", fmt.Errorf("resolve the AWS S3 endpoint of region %q: %w", o.Region, err)
	}
	return ep.URI.String(), nil
}

var _ Destination = (*S3Objects)(nil)
