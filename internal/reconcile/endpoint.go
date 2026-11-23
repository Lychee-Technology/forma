package reconcile

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/lychee-technology/forma/internal/s3endpoint"
)

// errUnknownDestination reports an S3 client whose effective destination cannot
// be established unambiguously for the storage binding.
var errUnknownDestination = s3endpoint.ErrUnknownDestination

// Destination is implemented by an ObjectStore that can report the exact bucket
// URL its client sends requests to.
type Destination interface {
	Endpoint(ctx context.Context) (string, error)
}

// Endpoint answers the exact bucket URL the store's client sends LIST and
// DELETE requests to (s3endpoint.ClientEndpoint, the derivation the writers
// bind the deployment with), refusing clients that may follow HTTP redirects.
func (s *S3Objects) Endpoint(ctx context.Context) (string, error) {
	c, ok := s.Client.(*s3.Client)
	if !ok {
		return "", fmt.Errorf("an S3 client of type %T: %w", s.Client, errUnknownDestination)
	}
	if !followsNoRedirect(c) {
		return "", fmt.Errorf("an S3 client that may follow redirects (build it with NewS3Objects): %w", errUnknownDestination)
	}
	endpoint, err := s3endpoint.ClientEndpoint(ctx, c, s.Bucket)
	if err != nil {
		return "", fmt.Errorf("tell the bucket URL of %q: %w", s.Bucket, err)
	}
	return endpoint, nil
}

var _ Destination = (*S3Objects)(nil)
