package cdc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/lychee-technology/forma"
)

// mockObjectSizeBytes is the ContentLength every mock HeadObject reports, so
// tests can assert SizeBytes propagation into manifest entries.
const mockObjectSizeBytes int64 = 42

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

// stubSchemaRegistry returns a fixed attribute cache with no error. A nil/empty
// cache field models a schema whose lookup succeeds but carries no attributes.
type stubSchemaRegistry struct{ cache forma.SchemaAttributeCache }

func (r stubSchemaRegistry) GetSchemaAttributeCacheByName(string) (int16, forma.SchemaAttributeCache, error) {
	return 0, r.cache, nil
}

func (r stubSchemaRegistry) GetSchemaAttributeCacheByID(int16) (string, forma.SchemaAttributeCache, error) {
	return "", r.cache, nil
}

func (r stubSchemaRegistry) GetSchemaByName(string) (int16, forma.JSONSchema, error) {
	return 0, forma.JSONSchema{}, nil
}

func (r stubSchemaRegistry) GetSchemaByID(int16) (string, forma.JSONSchema, error) {
	return "", forma.JSONSchema{}, nil
}

func (r stubSchemaRegistry) ListSchemas() []string { return nil }

// testAttrCache is a minimal populated cache (one column-bound text attr, one
// EAV bool attr) shared by tests that need a resolvable schema.
func testAttrCache() forma.SchemaAttributeCache {
	return forma.SchemaAttributeCache{
		"name": {
			AttributeName: "name",
			AttributeID:   10,
			ValueType:     forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText01},
		},
		"flag": {
			AttributeName: "flag",
			AttributeID:   11,
			ValueType:     forma.ValueTypeBool,
		},
	}
}

type inMemoryManifestStore struct {
	data    map[string][]byte
	etags   map[string]string
	loadErr error
	saveErr error
	saved   int
	// saveDelay slows Save so tests can order the manifest append against
	// timestamps sampled around it at millisecond resolution (#252 review P1);
	// lastSaveDoneMs records when the slowed Save completed.
	saveDelay      time.Duration
	lastSaveDoneMs int64
}

type objectOnlyS3Client struct{}

func (c *objectOnlyS3Client) CopyObject(_ context.Context, _ *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	return &s3.CopyObjectOutput{}, nil
}

func (c *objectOnlyS3Client) DeleteObject(_ context.Context, _ *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return &s3.DeleteObjectOutput{}, nil
}

func (c *objectOnlyS3Client) HeadObject(_ context.Context, _ *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return &s3.HeadObjectOutput{ContentLength: aws.Int64(mockObjectSizeBytes)}, nil
}

type copyFailingS3Client struct{}

func (c *copyFailingS3Client) CopyObject(_ context.Context, _ *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	return nil, errors.New("copy failed")
}

func (c *copyFailingS3Client) DeleteObject(_ context.Context, _ *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return &s3.DeleteObjectOutput{}, nil
}

func (c *copyFailingS3Client) HeadObject(_ context.Context, _ *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return &s3.HeadObjectOutput{ContentLength: aws.Int64(mockObjectSizeBytes)}, nil
}

// recordingS3Client records every DeleteObject key so tests can assert the
// #226 in-band tmp cleanup fired; copy/delete failures are injectable.
type recordingS3Client struct {
	copyErr     error
	deleteErr   error
	deletedKeys []string
}

func (c *recordingS3Client) CopyObject(_ context.Context, in *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	if c.copyErr != nil {
		return nil, fmt.Errorf("mock copy object to %s: %w", aws.ToString(in.Key), c.copyErr)
	}
	return &s3.CopyObjectOutput{}, nil
}

func (c *recordingS3Client) DeleteObject(_ context.Context, in *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	c.deletedKeys = append(c.deletedKeys, aws.ToString(in.Key))
	if c.deleteErr != nil {
		return nil, fmt.Errorf("mock delete object %s: %w", aws.ToString(in.Key), c.deleteErr)
	}
	return &s3.DeleteObjectOutput{}, nil
}

func (c *recordingS3Client) HeadObject(_ context.Context, _ *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return &s3.HeadObjectOutput{ContentLength: aws.Int64(mockObjectSizeBytes)}, nil
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
	if s.saveDelay > 0 {
		time.Sleep(s.saveDelay)
	}
	s.lastSaveDoneMs = time.Now().UnixMilli()
	s.saved++
	etag := fmt.Sprintf("etag-%d", s.saved)
	s.data[path] = append([]byte(nil), data...)
	s.etags[path] = etag
	return etag, nil
}
