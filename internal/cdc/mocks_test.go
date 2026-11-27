package cdc

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/lychee-technology/forma"
)

// mockObjectSizeBytes is the ContentLength every mock HeadObject reports.
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
	return derivedAncestry(forma.SchemaAttributeCache{
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
	})
}

type objectOnlyS3Client struct{}

func (c *objectOnlyS3Client) DeleteObject(_ context.Context, _ *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return &s3.DeleteObjectOutput{}, nil
}

func (c *objectOnlyS3Client) HeadObject(_ context.Context, _ *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
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

// derivedAncestry gives every attribute of cache without ArrayAncestors an
// empty one, as the schema registry does for a schema whose array ancestry it
// derived and that holds no array of objects: the parquet guard exports only
// such a schema (#704, #937).
func derivedAncestry(cache forma.SchemaAttributeCache) forma.SchemaAttributeCache {
	for name, meta := range cache {
		if meta.ArrayAncestors == nil {
			meta.ArrayAncestors = []string{}
			cache[name] = meta
		}
	}
	return cache
}
