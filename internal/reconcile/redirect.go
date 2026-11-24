package reconcile

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/lychee-technology/forma/internal/s3endpoint"
)

// ErrRedirectRefused reports an S3 request answered with a redirect. The
// collector follows no redirects so a destructive request can never be routed
// beyond the bound destination (s3endpoint.WithoutRedirects).
var ErrRedirectRefused = s3endpoint.ErrRedirectRefused

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
