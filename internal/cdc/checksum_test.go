package cdc

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type fakeGetClient struct {
	body []byte
	err  error
	key  string
}

func (f *fakeGetClient) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.key = *params.Key
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(f.body))}, nil
}

func TestObjectSHA256HashesBytes(t *testing.T) {
	client := &fakeGetClient{body: []byte("hello parquet")}
	got, err := ObjectSHA256(context.Background(), client, "b", "k/file.parquet")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// sha256("hello parquet")
	want := "sha256:950423965d5b936670f1549c58ce0594b58e1027c2b5e1e2a4f1515b1bc2f1b0"
	if got != want {
		t.Fatalf("checksum = %q, want %q", got, want)
	}
	if client.key != "k/file.parquet" {
		t.Fatalf("requested key %q", client.key)
	}
}

func TestObjectSHA256WrapsGetFailure(t *testing.T) {
	client := &fakeGetClient{err: fmt.Errorf("boom")}
	_, err := ObjectSHA256(context.Background(), client, "b", "k")
	if err == nil || !strings.Contains(err.Error(), "k") {
		t.Fatalf("want wrapped error naming the key, got %v", err)
	}
}

func TestObjectSHA256NilClient(t *testing.T) {
	if _, err := ObjectSHA256(context.Background(), nil, "b", "k"); err == nil {
		t.Fatal("want error for nil client")
	}
}
