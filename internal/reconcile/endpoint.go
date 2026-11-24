package reconcile

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/lychee-technology/forma/internal/s3endpoint"
)

// ErrRedirectRefused reports an S3 request answered with a redirect. The
// collector follows no redirects so a destructive request can never be routed
// beyond the bound destination (s3endpoint.ErrRedirectRefused).
var ErrRedirectRefused = s3endpoint.ErrRedirectRefused

// Destination is implemented by an ObjectStore that can report the exact bucket
// URL its client sends requests to.
type Destination interface {
	Endpoint(ctx context.Context) (string, error)
}

// Endpoint answers the exact bucket URL the store's client sends its requests
// to (s3endpoint.ClientEndpoint), the derivation the writers bind with,
// refusing clients that may follow HTTP redirects.
func (s *S3Objects) Endpoint(ctx context.Context) (string, error) {
	c, ok := s.Client.(*s3.Client)
	if !ok {
		return "", fmt.Errorf("an S3 client of type %T: %w", s.Client, s3endpoint.ErrUnknownDestination)
	}
	if !s3endpoint.FollowsNoRedirect(c) {
		return "", fmt.Errorf("an S3 client that may follow redirects (build it with NewS3Objects): %w", s3endpoint.ErrUnknownDestination)
	}
	endpoint, err := s3endpoint.ClientEndpoint(ctx, c, s.Bucket)
	if err != nil {
		return "", fmt.Errorf("tell where the collector's S3 client sends bucket %q's requests: %w", s.Bucket, err)
	}
	return endpoint, nil
}

// NewS3Objects returns an S3Objects over a copy of c that refuses every HTTP
// redirect before following it (s3endpoint.WithoutRedirects). Only stores
// constructed here report a bound Destination.Endpoint.
func NewS3Objects(c *s3.Client, bucket string) (*S3Objects, error) {
	bound, err := s3endpoint.WithoutRedirects(c)
	if err != nil {
		return nil, fmt.Errorf("build the collector's S3 client: %w", err)
	}
	return &S3Objects{Client: bound, Bucket: bucket}, nil
}

var _ Destination = (*S3Objects)(nil)
