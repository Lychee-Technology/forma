package manifest

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3ProbeClient is the S3 surface a manifest-driven QuerySource needs: the
// manifest object IO of S3Client plus HeadObject for existence probes.
type S3ProbeClient interface {
	S3Client
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}

// S3QuerySourceConfig is the S3 wiring for a manifest-driven QuerySource.
// It mirrors the CDC/compaction write side: ManifestPrefix and
// ManifestTemplate must match the writers' manifest layout, and DataPrefix
// must match the writers' parquet prefix, or reads resolve a different
// object set than the one being written.
type S3QuerySourceConfig struct {
	// Bucket holds both the manifests and the parquet objects they list.
	Bucket string
	// ManifestPrefix is the root prefix for manifest objects.
	ManifestPrefix string
	// ManifestTemplate is the per-schema manifest path template (e.g.
	// "manifest/{{.SchemaID}}.json"). Callers gate on it being non-empty;
	// this package adds no default of its own.
	ManifestTemplate string
	// DataPrefix is the parquet prefix used to build the legacy fallback
	// glob for schemas with no manifest entries. Empty disables the
	// fallback entirely.
	DataPrefix string
}

// NewS3QuerySource assembles the manifest-driven QuerySource used by
// federated reads (#250), centralizing the wiring the production e2e
// harness has been carrying inline (internal/e2e_harness/production/
// engine.go parquetSource). Reads scan exactly the manifest-listed objects,
// listed-but-absent keys classify as an inconsistent parquet set, and
// never-flushed schemas fall back to the legacy per-schema glob.
func NewS3QuerySource(client S3ProbeClient, cfg S3QuerySourceConfig) *QuerySource {
	return &QuerySource{
		Store:    &S3Store{Client: client, Bucket: cfg.Bucket},
		Resolver: PathResolver{Prefix: cfg.ManifestPrefix, PathTemplate: cfg.ManifestTemplate},
		Bucket:   cfg.Bucket,
		Exists:   S3ExistsProbe(client, cfg.Bucket),
		Fallback: S3FallbackGlob(cfg.Bucket, cfg.DataPrefix),
	}
}

// S3ExistsProbe returns a QuerySource.Exists probe backed by HeadObject.
// Only a confirmed 404 answers (false, nil): the SDK's *types.NotFound, or
// an HTTP response error with status 404 for stores whose bodyless HEAD
// reply the SDK cannot model as NotFound. Every other failure propagates
// wrapped — a probe that merely failed does not prove the object is gone,
// and misreading it would fabricate a data-loss report (see
// QuerySource.MissingIn).
func S3ExistsProbe(client S3ProbeClient, bucket string) func(ctx context.Context, key string) (bool, error) {
	return func(ctx context.Context, key string) (bool, error) {
		_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err == nil {
			return true, nil
		}
		if isNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("head parquet object %s/%s: %w", bucket, key, err)
	}
}

// isNotFound reports whether err is a CONFIRMED "object absent" answer.
// Strictly 404: any other status (403, 5xx) or transport failure leaves the
// object's existence unknown and must surface to the caller.
func isNotFound(err error) bool {
	var notFound *types.NotFound
	if errors.As(err, &notFound) {
		return true
	}
	var respErr *awshttp.ResponseError
	return errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusNotFound
}

// S3FallbackGlob returns the legacy per-schema parquet glob used for
// schemas with no manifest entries yet, preserving pre-manifest read
// behavior. The single `*` does not cross `/`, so in-flight `_tmp/`
// objects under the schema prefix stay excluded — it must never be widened
// to `**`. An empty dataPrefix returns a nil func, which QuerySource.Paths
// reads as "no fallback" (a glob built from an empty prefix would scan the
// bucket root).
func S3FallbackGlob(bucket, dataPrefix string) func(schemaID int16) string {
	if dataPrefix == "" {
		return nil
	}
	return func(schemaID int16) string {
		return fmt.Sprintf("s3://%s/%s/%d/*.parquet", bucket, dataPrefix, schemaID)
	}
}
