package federated

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

func hintQuery(tmpl string) *model.FederatedAttributeQuery {
	return &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7},
		DuckDBHints:    &model.DuckDBRenderHints{S3ParquetPathTemplate: tmpl},
	}
}

// TestHintRejectedWhenFeatureOff pins #456 layer 1: a caller template is
// rejected as invalid input when the opt-in flag is off, regardless of bucket.
func TestHintRejectedWhenFeatureOff(t *testing.T) {
	cfg := forma.DuckDBConfig{AllowCallerParquetPaths: false, S3Bucket: "b"}
	paths, err := duckDBParquetPathsForQuery(hintQuery("s3://b/x.parquet"), cfg)
	require.Nil(t, paths)
	require.ErrorIs(t, err, forma.ErrInvalidInput)
}

// TestHintNoHintReturnsNil pins that an absent hint stays (nil, nil) even with
// the feature off — resolution falls through to the manifest source.
func TestHintNoHintReturnsNil(t *testing.T) {
	cfg := forma.DuckDBConfig{AllowCallerParquetPaths: false}
	paths, err := duckDBParquetPathsForQuery(&model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7},
	}, cfg)
	require.NoError(t, err)
	require.Nil(t, paths)
}

// TestHintInScopeAccepted pins that an in-bucket path (glob included) is
// honored when the feature is on.
func TestHintInScopeAccepted(t *testing.T) {
	cfg := forma.DuckDBConfig{AllowCallerParquetPaths: true, S3Bucket: "b"}
	paths, err := duckDBParquetPathsForQuery(hintQuery("s3://b/7/base/*.parquet"), cfg)
	require.NoError(t, err)
	require.Equal(t, []string{"s3://b/7/base/*.parquet"}, paths)
}

// TestHintOutOfScopeRejected pins #456 layer 2: a path outside the configured
// bucket is rejected, including the s3://<bucket>X/ prefix-collision bypass and
// non-s3 targets.
func TestHintOutOfScopeRejected(t *testing.T) {
	cfg := forma.DuckDBConfig{AllowCallerParquetPaths: true, S3Bucket: "b"}
	for _, tmpl := range []string{
		"s3://other/x.parquet",
		"s3://bx/x.parquet", // prefix collision: must NOT match "s3://b/"
		"/etc/passwd",       // local file
		"https://attacker/x.parquet",
	} {
		paths, err := duckDBParquetPathsForQuery(hintQuery(tmpl), cfg)
		require.Nil(t, paths, "template %q", tmpl)
		require.ErrorIs(t, err, forma.ErrInvalidInput, "template %q", tmpl)
	}
}

// TestHintQuoteCharRejected pins the defense-in-depth char check: a quote in a
// rendered path is rejected at the gate (escaping still guards the render).
func TestHintQuoteCharRejected(t *testing.T) {
	cfg := forma.DuckDBConfig{AllowCallerParquetPaths: true, S3Bucket: "b"}
	paths, err := duckDBParquetPathsForQuery(hintQuery("s3://b/x.parquet') UNION ALL SELECT 1 --"), cfg)
	require.Nil(t, paths)
	require.ErrorIs(t, err, forma.ErrInvalidInput)
}

// TestHintRenderedOutOfScopeRejected pins that scope is checked AFTER template
// rendering, not on the raw template: a template whose {{.SchemaID}} expansion
// lands outside the bucket is rejected on its rendered value.
func TestHintRenderedOutOfScopeRejected(t *testing.T) {
	cfg := forma.DuckDBConfig{AllowCallerParquetPaths: true, S3Bucket: "b"}
	paths, err := duckDBParquetPathsForQuery(hintQuery("s3://other/{{.SchemaID}}/x.parquet"), cfg)
	require.Nil(t, paths)
	require.ErrorIs(t, err, forma.ErrInvalidInput)
}

// TestHintMixedScopeRejected pins that a comma-separated multi-path template is
// validated per segment: one out-of-scope segment rejects the whole hint, so an
// in-scope segment cannot smuggle an out-of-scope sibling into the scan set.
func TestHintMixedScopeRejected(t *testing.T) {
	cfg := forma.DuckDBConfig{AllowCallerParquetPaths: true, S3Bucket: "b"}
	paths, err := duckDBParquetPathsForQuery(
		hintQuery("s3://b/{{.SchemaID}}/base/*.parquet, s3://other/{{.SchemaID}}/x.parquet"), cfg)
	require.Nil(t, paths)
	require.ErrorIs(t, err, forma.ErrInvalidInput)
}

// TestHintEmptyBucketRejected pins that the feature cannot be honored without a
// bucket to scope against, even with the flag on.
func TestHintEmptyBucketRejected(t *testing.T) {
	cfg := forma.DuckDBConfig{AllowCallerParquetPaths: true, S3Bucket: ""}
	paths, err := duckDBParquetPathsForQuery(hintQuery("s3://b/x.parquet"), cfg)
	require.Nil(t, paths)
	require.ErrorIs(t, err, forma.ErrInvalidInput)
}
