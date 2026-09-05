package federated

import (
	"fmt"
	"strings"

	"github.com/lychee-technology/forma"
)

// Scope rules for caller-supplied parquet path hints
// (federated.s3_parquet_path_template), applied after the opt-in gate and
// template rendering in duckDBParquetPathsForQuery (#456, #477).

// hintWildcardChars are the DuckDB glob metacharacters. A segment carrying any
// of them is a pattern, not a literal name.
const hintWildcardChars = "*?["

// validateHintPathScope fails a caller-supplied parquet path set unless every
// rendered path sits inside the permitted scope and has a safe shape.
//
// Scope (#456, #477): the path must start with s3://<bucket>/ and, when
// dataPrefix is non-empty, with s3://<bucket>/<dataPrefix>/. The trailing
// slash on both is load-bearing: it blocks the s3://<bucket>X/... and
// <prefix>X/... prefix-collision bypasses and a bare s3://<bucket> with no
// key. The prefix is canonicalized with the writers' single TrimSuffix("/")
// (internal/cdc BuildDeltaPath, manifest.S3FallbackGlob), so "delta/" and
// "delta" scope identically; a prefix that canonicalizes to empty leaves the
// bucket rule alone in force, exactly as it disables the fallback glob. The
// bucket and prefix are operator detail and stay out of the published
// message (#313); the caller's own path may be echoed.
//
// Shape (#477): the same rules the level-3 fallback glob obeys
// (docs/federated-query/design.md §4.3.1). `**` is refused outright. Wildcards
// may appear only in the final (object-name) segment: a wildcard directory
// segment such as .../<schema>/*/x.parquet matches the in-flight `_tmp/`
// staging directory without spelling it, so the literal-`_tmp` rule below
// would otherwise be a fig leaf. A literal `_tmp` segment is refused wherever
// it sits. The final segment must be non-empty — a path ending in `/` names a
// directory, not an object or a glob.
//
// The disallowed-character check is defense-in-depth, not the escaping
// boundary: every render site already routes the path through
// sqlutil.EscapeLiteral, and in a single-quoted DuckDB literal only a single
// quote can alter SQL structure once doubled — the double-quote, semicolon,
// and newlines cannot. Rejecting them outright keeps such a path from ever
// reaching a render site, at the cost of refusing the rare object key that
// legitimately contains one.
//
// The bucket is trimmed to match Config.ValidateCallerParquetPaths, which
// rejects a whitespace-only bucket at startup; trimming here keeps a
// hand-built engine (tests, embedders that skip Config.Validate) consistent
// rather than silently rejecting every hint against a padded bucket.
func validateHintPathScope(paths []string, bucket, dataPrefix string) error {
	bucket = strings.TrimSpace(bucket)
	if bucket == "" {
		return forma.WithOperatorDetail(
			forma.InvalidInputf("caller-supplied s3_parquet_path_template is not permitted on this deployment"),
			fmt.Errorf("duckdb.s3Bucket must be set to scope caller-supplied parquet paths"))
	}
	scope := "s3://" + bucket + "/"
	if trimmed := strings.TrimSuffix(dataPrefix, "/"); trimmed != "" {
		scope += trimmed + "/"
	}
	for _, path := range paths {
		if strings.ContainsAny(path, "'\";\n\r") {
			return forma.InvalidInputf("s3 parquet path %q is not permitted: it contains a disallowed character", path)
		}
		if !strings.HasPrefix(path, scope) || len(path) == len(scope) {
			return forma.WithOperatorDetail(
				forma.InvalidInputf("s3 parquet path %q is outside the permitted parquet scope", path),
				fmt.Errorf("path must be under %s", scope))
		}
		if err := validateHintPathShape(path, path[len(scope):]); err != nil {
			return err
		}
	}
	return nil
}

// validateHintPathShape applies the #477 glob-shape rules to the key portion
// of one in-scope hint path (the part after the permitted scope prefix). The
// full path is passed only for the published message.
func validateHintPathShape(path, key string) error {
	if strings.Contains(key, "**") {
		return forma.InvalidInputf(
			"s3 parquet path %q is not permitted: recursive globs (**) are not allowed", path)
	}
	segments := strings.Split(key, "/")
	last := len(segments) - 1
	if segments[last] == "" {
		return forma.InvalidInputf(
			"s3 parquet path %q is not permitted: it names a directory, not a parquet object or object glob", path)
	}
	for i, seg := range segments {
		if seg == "_tmp" {
			return forma.InvalidInputf(
				"s3 parquet path %q is not permitted: _tmp/ staging objects are not readable", path)
		}
		if i != last && strings.ContainsAny(seg, hintWildcardChars) {
			return forma.InvalidInputf(
				"s3 parquet path %q is not permitted: wildcards are allowed only in the final path segment", path)
		}
	}
	return nil
}
