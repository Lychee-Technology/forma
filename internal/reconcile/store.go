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

	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// ErrUnknownAge reports an object listed without a usable LastModified. A
// pass that meets one fails: the horizon cannot prove its publication fenced.
var ErrUnknownAge = errors.New("object age unknown")

// ObjectStore is what a pass reads and deletes in the bucket. A key it is
// asked for and that holds no object is manifestv2.ErrObjectNotFound, and
// only then: a refused or failed call (manifestv2.ErrAccessDenied among
// them) proves nothing about the object, and a pass that meets one fails
// rather than call the object missing.
type ObjectStore interface {
	// List answers every object under prefix, with its LastModified as
	// epoch milliseconds. A listing it cannot follow to the end fails, and
	// so does one that answers an object without a LastModified
	// (ErrUnknownAge).
	List(ctx context.Context, prefix string) ([]publication.Object, error)
	// Get answers an object's bytes.
	Get(ctx context.Context, key string) ([]byte, error)
	// Exists answers whether an object is there; false only on a
	// confirmed not-found.
	Exists(ctx context.Context, key string) (bool, error)
	// Hash answers the SHA-256 of an object's bytes, as 64 lowercase hex
	// digits, and how many bytes it read.
	Hash(ctx context.Context, key string) (string, int64, error)
	// Delete deletes an object. Deleting a key that holds none succeeds,
	// so a pass repeated after a crash deletes again harmlessly.
	Delete(ctx context.Context, key string) error
}

// S3API is the part of the S3 API an S3Objects uses.
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

// List pages through cdc.ForEachObject, which fails a listing it cannot
// follow to the end instead of answering the part it saw (#521): a short
// listing would hide objects a retained generation lists and report them
// lost. It also fails on an object listed without a usable LastModified:
// aws.ToTime of a missing one is the zero time, which would make any key,
// fresh staging included, look older than every horizon.
func (s *S3Objects) List(ctx context.Context, prefix string) ([]publication.Object, error) {
	var out []publication.Object
	err := cdc.ForEachObject(ctx, s.Client, s.Bucket, prefix, func(o types.Object) error {
		key := aws.ToString(o.Key)
		if o.LastModified == nil || o.LastModified.UnixMilli() <= 0 {
			return fmt.Errorf("object %s was listed without a usable LastModified (%v): its age, and so whether its publication can still commit, is unknown: %w",
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

// Get reads through manifestv2.S3Store, which tells a missing key
// (ErrObjectNotFound) from a refused read (ErrAccessDenied).
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

// Hash streams the object through SHA-256.
func (s *S3Objects) Hash(ctx context.Context, key string) (string, int64, error) {
	out, err := s.Client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.Bucket), Key: aws.String(key)})
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

// Delete deletes the object; S3 answers success for a key that holds none.
func (s *S3Objects) Delete(ctx context.Context, key string) error {
	_, err := s.Client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.Bucket), Key: aws.String(key)})
	if err != nil {
		return fmt.Errorf("delete object %s/%s: %w", s.Bucket, key, err)
	}
	return nil
}

var _ ObjectStore = (*S3Objects)(nil)
