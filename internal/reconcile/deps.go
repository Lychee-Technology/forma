package reconcile

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/lychee-technology/forma/internal/compaction"
)

// ObjectInfo is one listed S3 object with the metadata reconcile needs:
// Size feeds repaired FileEntry.SizeBytes, LastModified feeds the GC grace
// check.
type ObjectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
}

// ObjectLister lists every object under a prefix. Implementations own
// pagination — callers always receive the complete listing.
type ObjectLister interface {
	ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error)
}

// ObjectDeleter deletes a single object by bucket-relative key.
type ObjectDeleter interface {
	DeleteObject(ctx context.Context, key string) error
}

// s3ObjectAPI is the slice of *s3.Client the reconciler consumes. None of
// the production S3 interfaces (cdc.S3ObjectClient, manifest.S3Client)
// expose ListObjectsV2, so the tool declares its own.
type s3ObjectAPI interface {
	ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, opts ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

// S3ObjectStore implements ObjectLister and ObjectDeleter over an
// *s3.Client (or any compatible API subset).
type S3ObjectStore struct {
	Client s3ObjectAPI
	Bucket string
}

// ListObjects lists all objects under prefix, following continuation tokens
// until the listing is exhausted.
func (s *S3ObjectStore) ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	var objs []ObjectInfo
	var token *string
	for {
		out, err := s.Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.Bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("list s3 objects under %s/%s: %w", s.Bucket, prefix, err)
		}
		for _, obj := range out.Contents {
			objs = append(objs, ObjectInfo{
				Key:          aws.ToString(obj.Key),
				Size:         aws.ToInt64(obj.Size),
				LastModified: aws.ToTime(obj.LastModified),
			})
		}
		if !aws.ToBool(out.IsTruncated) || out.NextContinuationToken == nil {
			return objs, nil
		}
		token = out.NextContinuationToken
	}
}

// DeleteObject deletes one object by key.
func (s *S3ObjectStore) DeleteObject(ctx context.Context, key string) error {
	_, err := s.Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("delete s3 object %s/%s: %w", s.Bucket, key, err)
	}
	return nil
}

// DuckStatsReader recomputes parquet stats over a DuckDB session with
// httpfs and S3 credentials already configured (cdc.NewDuckExporter's DB).
// Keep the pool at one connection: the exporter's S3 SETs are
// session-scoped and only reach the connection they ran on (#285).
type DuckStatsReader struct {
	DB     *sql.DB
	Bucket string
}

func (d *DuckStatsReader) FileStats(ctx context.Context, key string) (compaction.MergeStats, error) {
	return compaction.SingleFileStats(ctx, d.DB, fmt.Sprintf("s3://%s/%s", d.Bucket, key))
}

var (
	_ ObjectLister  = (*S3ObjectStore)(nil)
	_ ObjectDeleter = (*S3ObjectStore)(nil)
	_ StatsReader   = (*DuckStatsReader)(nil)
)
