package reconcile

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// ErrUnknownAge reports an object listed without a usable LastModified. A pass
// that meets one fails because the horizon cannot prove its publication fenced.
var ErrUnknownAge = errors.New("object age unknown")

// ObjectStore is what a pass reads and deletes in the bucket. Only a confirmed
// absent key returns manifestv2.ErrObjectNotFound; any access error, network
// error or redirect refusal returns a plain error so the pass fails closed.
type ObjectStore interface {
	List(ctx context.Context, prefix string) ([]publication.Object, error)
	Get(ctx context.Context, key string) ([]byte, error)
	Exists(ctx context.Context, key string) (bool, error)
	Hash(ctx context.Context, key string) (string, int64, error)
	Delete(ctx context.Context, key string) error
}

// S3API is the subset of the S3 client API S3Objects uses.
type S3API interface {
	cdc.S3ListClient
	manifestv2.S3Client
	manifestv2.S3HeadClient
	DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, opts ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

// S3Objects is an ObjectStore over an S3 bucket.
type S3Objects struct {
	Client S3API
	Bucket string
}

// List pages through cdc.ForEachObject, failing on truncated listings (#521)
// and on any object missing a usable positive LastModified.
func (s *S3Objects) List(ctx context.Context, prefix string) ([]publication.Object, error) {
	var out []publication.Object
	err := cdc.ForEachObject(ctx, s.Client, s.Bucket, prefix, func(o types.Object) error {
		key := aws.ToString(o.Key)
		if o.LastModified == nil || o.LastModified.UnixMilli() <= 0 {
			return fmt.Errorf("object %s was listed without a usable LastModified (%v): %w",
				key, o.LastModified, ErrUnknownAge)
		}
		out = append(out, publication.Object{Key: key, LastModified: o.LastModified.UnixMilli()})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list bucket %s: %w", s.Bucket, err)
	}
	return out, nil
}

// Get reads through manifestv2.S3Store, separating ErrObjectNotFound from
// ErrAccessDenied and transport failures.
func (s *S3Objects) Get(ctx context.Context, key string) ([]byte, error) {
	raw, err := (&manifestv2.S3Store{Client: s.Client, Bucket: s.Bucket}).Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("read the object: %w", err)
	}
	return raw, nil
}

// Exists probes with HeadObject (manifestv2.S3Exists): only a confirmed 404
// answers false.
func (s *S3Objects) Exists(ctx context.Context, key string) (bool, error) {
	ok, err := manifestv2.S3Exists(s.Client, s.Bucket)(ctx, key)
	if err != nil {
		return false, fmt.Errorf("probe the object: %w", err)
	}
	return ok, nil
}

// Hash streams an object through SHA-256. Only a NoSuchKey answer wraps
// manifestv2.ErrObjectNotFound; redirects, 403s and transport errors stay
// operational failures.
func (s *S3Objects) Hash(ctx context.Context, key string) (string, int64, error) {
	out, err := s.Client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.Bucket), Key: aws.String(key)})
	if err != nil && noSuchKey(err) {
		return "", 0, fmt.Errorf("get object %s/%s to hash it: %w: %w", s.Bucket, key, manifestv2.ErrObjectNotFound, err)
	}
	if err != nil {
		return "", 0, fmt.Errorf("get object %s/%s to hash it: %w", s.Bucket, key, err)
	}
	if out == nil || out.Body == nil {
		return "", 0, errors.New("get object " + s.Bucket + "/" + key + " answered no body to hash")
	}
	defer func() { _ = out.Body.Close() }()
	h := sha256.New()
	n, err := io.Copy(h, out.Body)
	if err != nil {
		return "", 0, fmt.Errorf("hash object %s/%s: %w", s.Bucket, key, err)
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

// Delete deletes key from the bucket; S3 answers success for an absent key.
func (s *S3Objects) Delete(ctx context.Context, key string) error {
	_, err := s.Client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.Bucket), Key: aws.String(key)})
	if err != nil {
		return fmt.Errorf("delete object %s/%s: %w", s.Bucket, key, err)
	}
	return nil
}

func noSuchKey(err error) bool {
	var missing *types.NoSuchKey
	if errors.As(err, &missing) {
		return true
	}
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchKey"
}

var _ ObjectStore = (*S3Objects)(nil)
