package manifestv2

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3HeadClient is the HeadObject surface an existence probe needs.
type S3HeadClient interface {
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}

// S3Exists answers an existence probe of bucket's keys backed by HeadObject:
// the federated reader's check, after a failed read, of whether a listed
// object is lost. Only a confirmed 404 answers (false, nil): the SDK's
// *types.NotFound, or an HTTP response error with status 404 for stores whose
// bodyless HEAD reply the SDK cannot model as NotFound. Every other failure
// propagates wrapped: a probe that merely failed does not prove the object is
// gone, and misreading it would fabricate a data-loss report. A 403 wraps
// ErrAccessDenied: without s3:ListBucket on the bucket S3 answers it for a
// missing key too, so the caller can tell the probe proved nothing either way.
func S3Exists(client S3HeadClient, bucket string) func(ctx context.Context, key string) (bool, error) {
	return func(ctx context.Context, key string) (bool, error) {
		_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err == nil {
			return true, nil
		}
		if headNotFound(err) {
			return false, nil
		}
		if accessDenied(err) {
			return false, fmt.Errorf("head object %s/%s: %w: %w", bucket, key, ErrAccessDenied, err)
		}
		return false, fmt.Errorf("head object %s/%s: %w", bucket, key, err)
	}
}

// headNotFound reports whether err is a confirmed "object absent" answer.
// Strictly 404: any other status (403, 5xx) or transport failure leaves the
// object's existence unknown.
func headNotFound(err error) bool {
	var notFound *types.NotFound
	if errors.As(err, &notFound) {
		return true
	}
	var respErr *awshttp.ResponseError
	return errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusNotFound
}
