package manifest

import (
	"bytes"
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

type S3Client interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// S3Store implements Store using AWS S3-compatible APIs.
type S3Store struct {
	Client S3Client
	Bucket string
}

func (s *S3Store) Load(ctx context.Context, path string) ([]byte, string, error) {
	if s == nil || s.Client == nil {
		return nil, "", fmt.Errorf("s3 client is nil")
	}
	out, err := s.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		if isNoSuchKey(err) {
			return nil, "", fmt.Errorf("get manifest object %s/%s: %w: %w", s.Bucket, path, ErrObjectNotFound, err)
		}
		return nil, "", fmt.Errorf("get manifest object %s/%s: %w", s.Bucket, path, err)
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, "", fmt.Errorf("read manifest object %s/%s: %w", s.Bucket, path, err)
	}
	etag := ""
	if out.ETag != nil {
		etag = *out.ETag
	}
	return data, etag, nil
}

func (s *S3Store) Save(ctx context.Context, path string, data []byte, etag string) (string, error) {
	if s == nil || s.Client == nil {
		return "", fmt.Errorf("s3 client is nil")
	}
	input := &s3.PutObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(path),
		Body:   bytes.NewReader(data),
	}
	if etag != "" {
		input.IfMatch = aws.String(etag)
	} else {
		// An empty etag means the caller loaded no existing manifest. An
		// unconditional PUT here would silently clobber a manifest another
		// writer created in the meantime (#203 review); If-None-Match makes
		// the create fail with 412 instead, so callers can reload and retry
		// against the real object.
		input.IfNoneMatch = aws.String("*")
	}
	out, err := s.Client.PutObject(ctx, input)
	if err != nil {
		return "", fmt.Errorf("put manifest object %s/%s: %w", s.Bucket, path, err)
	}
	newETag := ""
	if out.ETag != nil {
		newETag = *out.ETag
	}
	return newETag, nil
}

// isNoSuchKey reports whether a GetObject error is a CONFIRMED missing key:
// the SDK's modeled *types.NoSuchKey, or any API error carrying the
// NoSuchKey code. The HTTP status alone is deliberately not consulted — a
// NoSuchBucket is also a 404, and GetObject always returns an error body
// from which the SDK derives the code, so the code is the only signal that
// separates "the key is absent" from "the store call failed" (#464).
func isNoSuchKey(err error) bool {
	var noSuchKey *types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return true
	}
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchKey"
}

// IsPreconditionFailed reports whether err is a CONFIRMED HTTP 412
// conditional-put rejection from the object store — the only Save failure
// that proves the write did not commit, and therefore the only one a caller
// may answer by reloading and retrying. Transport errors, timeouts and other
// API failures are deliberately excluded: after those the put may have
// landed, and callers must treat the outcome as ambiguous. Shared by the
// compactor's CAS swap, manifest-reconcile's retried saves, and cdc-init's
// final publish (#416).
func IsPreconditionFailed(err error) bool {
	if err == nil {
		return false
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "PreconditionFailed" {
		return true
	}
	var respErr *awshttp.ResponseError
	return errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusPreconditionFailed
}
