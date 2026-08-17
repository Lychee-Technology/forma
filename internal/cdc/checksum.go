package cdc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ChecksumSHA256Prefix tags manifest FileEntry.Checksum values produced by
// ObjectSHA256. Empty Checksum means the entry predates stamping (#347);
// field presence is the format version signal, matching the Columns rule.
const ChecksumSHA256Prefix = "sha256:"

// S3GetClient is the minimal byte-read surface content hashing needs.
type S3GetClient interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// ObjectSHA256 streams an object and returns "sha256:<hex>" over its bytes.
// The hash is of what the store returns, not what the writer sent: it blesses
// the published state, so it detects later mutation, not upload mangling.
func ObjectSHA256(ctx context.Context, client S3GetClient, bucket, key string) (string, error) {
	if client == nil {
		return "", fmt.Errorf("s3 client is nil")
	}
	out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return "", fmt.Errorf("get object %s for checksum: %w", key, err)
	}
	if out == nil || out.Body == nil {
		return "", fmt.Errorf("empty response body for object %s: no bytes to checksum", key)
	}
	defer func() { _ = out.Body.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, out.Body); err != nil {
		return "", fmt.Errorf("read object %s for checksum: %w", key, err)
	}
	return ChecksumSHA256Prefix + hex.EncodeToString(h.Sum(nil)), nil
}
