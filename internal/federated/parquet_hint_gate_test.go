package federated

import (
	"errors"
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

// TestHintRecursiveGlobRejected pins #477: `**` is refused in a hint path, the
// same rule §4.3.1 imposes on the fallback glob, so a hint cannot widen the
// scan across every prefix of the bucket.
func TestHintRecursiveGlobRejected(t *testing.T) {
	cfg := forma.DuckDBConfig{AllowCallerParquetPaths: true, S3Bucket: "b"}
	for _, tmpl := range []string{
		"s3://b/**/*.parquet",
		"s3://b/data/{{.SchemaID}}/**",
		"s3://b/data/**.parquet",
	} {
		paths, err := duckDBParquetPathsForQuery(hintQuery(tmpl), cfg)
		require.Nil(t, paths, "template %q", tmpl)
		require.ErrorIs(t, err, forma.ErrInvalidInput, "template %q", tmpl)
	}
}

// TestHintTmpSegmentRejected pins #477: in-flight `_tmp/` staging objects are
// not a scan target. A literal `_tmp` segment is refused wherever it sits.
func TestHintTmpSegmentRejected(t *testing.T) {
	cfg := forma.DuckDBConfig{AllowCallerParquetPaths: true, S3Bucket: "b"}
	for _, tmpl := range []string{
		"s3://b/data/{{.SchemaID}}/_tmp/x.parquet",
		"s3://b/data/{{.SchemaID}}/_tmp/*.parquet",
		"s3://b/_tmp/x.parquet",
	} {
		paths, err := duckDBParquetPathsForQuery(hintQuery(tmpl), cfg)
		require.Nil(t, paths, "template %q", tmpl)
		require.ErrorIs(t, err, forma.ErrInvalidInput, "template %q", tmpl)
	}
}

// TestHintWildcardOutsideFinalSegmentRejected pins #477: wildcards may appear
// only in the object-name segment. A wildcard directory segment
// (`.../<schema>/*/x.parquet`) would match `_tmp/` without ever spelling it,
// so the `_tmp` rule alone is not enough.
func TestHintWildcardOutsideFinalSegmentRejected(t *testing.T) {
	cfg := forma.DuckDBConfig{AllowCallerParquetPaths: true, S3Bucket: "b"}
	for _, tmpl := range []string{
		"s3://b/data/{{.SchemaID}}/*/x.parquet",
		"s3://b/data/*/{{.SchemaID}}/*.parquet",
		"s3://b/data/{{.SchemaID}}/?tmp/x.parquet",
		"s3://b/data/{{.SchemaID}}/[_]tmp/x.parquet",
		"s3://b/data/{{.SchemaID}}/_t*/x.parquet",
	} {
		paths, err := duckDBParquetPathsForQuery(hintQuery(tmpl), cfg)
		require.Nil(t, paths, "template %q", tmpl)
		require.ErrorIs(t, err, forma.ErrInvalidInput, "template %q", tmpl)
	}
}

// TestHintDirectoryPathRejected pins #477: a path ending in `/` names a
// directory, not an object or a glob, and is refused.
func TestHintDirectoryPathRejected(t *testing.T) {
	cfg := forma.DuckDBConfig{AllowCallerParquetPaths: true, S3Bucket: "b"}
	for _, tmpl := range []string{"s3://b/", "s3://b/data/{{.SchemaID}}/"} {
		paths, err := duckDBParquetPathsForQuery(hintQuery(tmpl), cfg)
		require.Nil(t, paths, "template %q", tmpl)
		require.ErrorIs(t, err, forma.ErrInvalidInput, "template %q", tmpl)
	}
}

// TestHintFinalSegmentGlobAccepted pins that the #477 shape rules keep every
// hint form the repo relies on: a single-`*` object glob, a multi-path
// base/delta template, a `?`/`[]` object pattern, and a literal object URI.
func TestHintFinalSegmentGlobAccepted(t *testing.T) {
	cfg := forma.DuckDBConfig{AllowCallerParquetPaths: true, S3Bucket: "b"}
	for tmpl, want := range map[string][]string{
		"s3://b/data/{{.SchemaID}}/*.parquet": {"s3://b/data/7/*.parquet"},
		"s3://b/data/{{.SchemaID}}/base/*.parquet, s3://b/data/{{.SchemaID}}/delta/*.parquet": {
			"s3://b/data/7/base/*.parquet", "s3://b/data/7/delta/*.parquet"},
		"s3://b/data/7/part-?[0-9].parquet":    {"s3://b/data/7/part-?[0-9].parquet"},
		"s3://b/data/7/019abc.parquet":         {"s3://b/data/7/019abc.parquet"},
		"s3://b/data/7/not_tmp/x.parquet":      {"s3://b/data/7/not_tmp/x.parquet"},
		"s3://b/data/7/_tmp_archive/x.parquet": {"s3://b/data/7/_tmp_archive/x.parquet"},
	} {
		paths, err := duckDBParquetPathsForQuery(hintQuery(tmpl), cfg)
		require.NoError(t, err, "template %q", tmpl)
		require.Equal(t, want, paths, "template %q", tmpl)
	}
}

// TestHintDataPrefixScope pins #477 prefix granularity: with S3DataPrefix set,
// a hint must sit under s3://<bucket>/<prefix>/ — exact segment boundary, so a
// sibling prefix sharing the same leading characters is out of scope — and the
// prefix is canonicalized with the writers' single TrimSuffix("/").
func TestHintDataPrefixScope(t *testing.T) {
	for _, prefix := range []string{"data", "data/"} {
		cfg := forma.DuckDBConfig{AllowCallerParquetPaths: true, S3Bucket: "b", S3DataPrefix: prefix}
		paths, err := duckDBParquetPathsForQuery(hintQuery("s3://b/data/{{.SchemaID}}/*.parquet"), cfg)
		require.NoError(t, err, "prefix %q", prefix)
		require.Equal(t, []string{"s3://b/data/7/*.parquet"}, paths, "prefix %q", prefix)

		for _, tmpl := range []string{
			"s3://b/other/{{.SchemaID}}/*.parquet",
			"s3://b/data2/{{.SchemaID}}/*.parquet", // prefix collision: must NOT match "data/"
			"s3://b/data",                          // the prefix itself, no key below it
			"s3://b/data/{{.SchemaID}}/*.parquet, s3://b/other/x.parquet",
		} {
			paths, err := duckDBParquetPathsForQuery(hintQuery(tmpl), cfg)
			require.Nil(t, paths, "prefix %q template %q", prefix, tmpl)
			require.ErrorIs(t, err, forma.ErrInvalidInput, "prefix %q template %q", prefix, tmpl)
		}
	}
}

// TestHintEmptyDataPrefixKeepsBucketScope pins that an empty (or bare "/")
// S3DataPrefix leaves the #456 bucket-granularity rule in force rather than
// rejecting everything or scoping to the bucket root literally.
func TestHintEmptyDataPrefixKeepsBucketScope(t *testing.T) {
	for _, prefix := range []string{"", "/"} {
		cfg := forma.DuckDBConfig{AllowCallerParquetPaths: true, S3Bucket: "b", S3DataPrefix: prefix}
		paths, err := duckDBParquetPathsForQuery(hintQuery("s3://b/anywhere/{{.SchemaID}}/*.parquet"), cfg)
		require.NoError(t, err, "prefix %q", prefix)
		require.Equal(t, []string{"s3://b/anywhere/7/*.parquet"}, paths, "prefix %q", prefix)
	}
}

// TestHintGlobMetacharDataPrefixRejected pins the #526 review finding: the
// configured S3DataPrefix is spliced verbatim into the permitted scope, and
// the shape rules inspect only the key below it, so a prefix carrying a glob
// metacharacter would let DuckDB expand a directory wildcard the caller never
// wrote (`s3://b/data/*/7/x.parquet`, or `**` reaching `_tmp/`). Such a prefix
// is rejected at startup by ValidateCallerParquetPaths; here the engine-level
// guard refuses every hint on a hand-built config that skipped it, keeps the
// prefix out of the published message, and leaves it in the operator copy.
func TestHintGlobMetacharDataPrefixRejected(t *testing.T) {
	for _, prefix := range []string{"data/*", "data/**", "data?", "data[1]", "*"} {
		cfg := forma.DuckDBConfig{AllowCallerParquetPaths: true, S3Bucket: "b", S3DataPrefix: prefix}
		tmpl := "s3://b/" + prefix + "/{{.SchemaID}}/x.parquet" // sits "inside" the scope string
		paths, err := duckDBParquetPathsForQuery(hintQuery(tmpl), cfg)
		require.Nil(t, paths, "prefix %q", prefix)
		require.ErrorIs(t, err, forma.ErrInvalidInput, "prefix %q", prefix)

		var pub forma.PublicError
		require.True(t, errors.As(err, &pub), "prefix %q: %v", prefix, err)
		require.NotContains(t, pub.PublicMessage(), prefix, "prefix %q must stay operator-only", prefix)
		require.True(t, forma.HasOperatorDetail(err), "prefix %q", prefix)
		require.Contains(t, err.Error(), prefix, "prefix %q: operator copy must name the prefix", prefix)
	}
}
