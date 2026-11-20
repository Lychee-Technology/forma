package manifestv2

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// S3Client is the part of the S3 API an S3Store uses.
type S3Client interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// S3Store is a write-once Store over an S3-compatible bucket. Every put is
// conditional (If-None-Match: *), so S3 itself refuses a second write to a
// key, with 412 Precondition Failed, however many writers race for it.
type S3Store struct {
	Client S3Client
	Bucket string
}

// Put writes body under key once. A 412 is ErrKeyExists. Any other failure,
// a 409 ConditionalRequestConflict from a concurrent conditional put
// included, is a plain error whose outcome the caller cannot know: the
// object may or may not exist.
func (s *S3Store) Put(ctx context.Context, key string, body io.ReadSeeker) error {
	if s == nil || s.Client == nil {
		return errors.New("s3 store is not configured")
	}
	_, err := s.Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.Bucket),
		Key:         aws.String(key),
		Body:        body,
		IfNoneMatch: aws.String("*"),
	})
	if err != nil && preconditionFailed(err) {
		return fmt.Errorf("put object %s/%s: %w: %w", s.Bucket, key, ErrKeyExists, err)
	}
	if err != nil {
		return fmt.Errorf("put object %s/%s: %w", s.Bucket, key, err)
	}
	return nil
}

// Get answers the object's bytes. Only a NoSuchKey answer is
// ErrObjectNotFound: a missing bucket is a 404 too, and is a failed store
// call, not a missing object.
func (s *S3Store) Get(ctx context.Context, key string) ([]byte, error) {
	if s == nil || s.Client == nil {
		return nil, errors.New("s3 store is not configured")
	}
	out, err := s.Client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.Bucket), Key: aws.String(key)})
	if err != nil && noSuchKey(err) {
		return nil, fmt.Errorf("get object %s/%s: %w: %w", s.Bucket, key, ErrObjectNotFound, err)
	}
	if err != nil && accessDenied(err) {
		return nil, fmt.Errorf("get object %s/%s: %w: %w", s.Bucket, key, ErrAccessDenied, err)
	}
	if err != nil {
		return nil, fmt.Errorf("get object %s/%s: %w", s.Bucket, key, err)
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("read object %s/%s: %w", s.Bucket, key, err)
	}
	return data, nil
}

func noSuchKey(err error) bool {
	var missing *types.NoSuchKey
	if errors.As(err, &missing) {
		return true
	}
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchKey"
}

// accessDenied reports a 403: an AccessDenied (or Forbidden) API error, or an
// HTTP response error with status 403 for a bodyless answer (a HEAD) the SDK
// cannot model. It is ambiguous about existence (ErrAccessDenied).
func accessDenied(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && (apiErr.ErrorCode() == "AccessDenied" || apiErr.ErrorCode() == "Forbidden") {
		return true
	}
	var respErr *awshttp.ResponseError
	return errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusForbidden
}

// preconditionFailed reports a confirmed 412, the one answer that proves a
// conditional put did not write.
func preconditionFailed(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "PreconditionFailed" {
		return true
	}
	var respErr *awshttp.ResponseError
	return errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusPreconditionFailed
}
