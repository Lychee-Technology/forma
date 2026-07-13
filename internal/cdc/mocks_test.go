package cdc

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/lychee-technology/forma"
)

type errorSchemaRegistry struct{ err error }

func (r errorSchemaRegistry) GetSchemaAttributeCacheByName(string) (int16, forma.SchemaAttributeCache, error) {
	return 0, nil, r.err
}

func (r errorSchemaRegistry) GetSchemaAttributeCacheByID(int16) (string, forma.SchemaAttributeCache, error) {
	return "", nil, r.err
}

func (r errorSchemaRegistry) GetSchemaByName(string) (int16, forma.JSONSchema, error) {
	return 0, forma.JSONSchema{}, r.err
}

func (r errorSchemaRegistry) GetSchemaByID(int16) (string, forma.JSONSchema, error) {
	return "", forma.JSONSchema{}, r.err
}

func (r errorSchemaRegistry) ListSchemas() []string {
	return nil
}

type inMemoryManifestStore struct {
	data    map[string][]byte
	etags   map[string]string
	loadErr error
	saveErr error
	saved   int
}

type objectOnlyS3Client struct{}

func (c *objectOnlyS3Client) CopyObject(_ context.Context, _ *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	return &s3.CopyObjectOutput{}, nil
}

func (c *objectOnlyS3Client) DeleteObject(_ context.Context, _ *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return &s3.DeleteObjectOutput{}, nil
}

type copyFailingS3Client struct{}

func (c *copyFailingS3Client) CopyObject(_ context.Context, _ *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	return nil, errors.New("copy failed")
}

func (c *copyFailingS3Client) DeleteObject(_ context.Context, _ *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return &s3.DeleteObjectOutput{}, nil
}

type fullS3ClientMock struct {
	objectOnlyS3Client
}

func (c *fullS3ClientMock) GetObject(_ context.Context, _ *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return &s3.GetObjectOutput{}, nil
}

func (c *fullS3ClientMock) PutObject(_ context.Context, _ *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return &s3.PutObjectOutput{}, nil
}

func newInMemoryManifestStore() *inMemoryManifestStore {
	return &inMemoryManifestStore{
		data:  make(map[string][]byte),
		etags: make(map[string]string),
	}
}

func (s *inMemoryManifestStore) Load(_ context.Context, path string) ([]byte, string, error) {
	if s.loadErr != nil {
		return nil, "", s.loadErr
	}
	b, ok := s.data[path]
	if !ok {
		return nil, "", fmt.Errorf("not found")
	}
	return append([]byte(nil), b...), s.etags[path], nil
}

func (s *inMemoryManifestStore) Save(_ context.Context, path string, data []byte, _ string) (string, error) {
	if s.saveErr != nil {
		return "", s.saveErr
	}
	s.saved++
	etag := fmt.Sprintf("etag-%d", s.saved)
	s.data[path] = append([]byte(nil), data...)
	s.etags[path] = etag
	return etag, nil
}
