package manifest

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type capturingS3Client struct {
	lastPut *s3.PutObjectInput
}

func (c *capturingS3Client) GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return nil, nil
}

func (c *capturingS3Client) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	c.lastPut = in
	return &s3.PutObjectOutput{ETag: aws.String("new-etag")}, nil
}

func TestS3StoreSave_EmptyETagUsesConditionalCreate(t *testing.T) {
	client := &capturingS3Client{}
	store := &S3Store{Client: client, Bucket: "bkt"}

	if _, err := store.Save(context.Background(), "manifest/7.json", []byte("{}"), ""); err != nil {
		t.Fatalf("save: %v", err)
	}
	if client.lastPut.IfMatch != nil {
		t.Fatal("empty etag must not set If-Match")
	}
	if client.lastPut.IfNoneMatch == nil || *client.lastPut.IfNoneMatch != "*" {
		t.Fatalf("empty etag must set If-None-Match:* so a create cannot clobber a concurrently created manifest, got %v", client.lastPut.IfNoneMatch)
	}
}

func TestS3StoreSave_ETagUsesIfMatch(t *testing.T) {
	client := &capturingS3Client{}
	store := &S3Store{Client: client, Bucket: "bkt"}

	if _, err := store.Save(context.Background(), "manifest/7.json", []byte("{}"), "abc"); err != nil {
		t.Fatalf("save: %v", err)
	}
	if client.lastPut.IfMatch == nil || *client.lastPut.IfMatch != "abc" {
		t.Fatalf("etag save must set If-Match, got %v", client.lastPut.IfMatch)
	}
	if client.lastPut.IfNoneMatch != nil {
		t.Fatal("etag save must not set If-None-Match")
	}
}
