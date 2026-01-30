package manifest

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Store implements Store using AWS S3-compatible APIs.
type S3Store struct {
	Client *s3.Client
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
		return nil, "", err
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, "", err
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
	}
	out, err := s.Client.PutObject(ctx, input)
	if err != nil {
		return "", err
	}
	newETag := ""
	if out.ETag != nil {
		newETag = *out.ETag
	}
	return newETag, nil
}
