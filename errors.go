package forma

import (
	"errors"
	"fmt"
)

// Sentinel errors returned by the entity manager layer.
// Callers should use errors.Is / errors.As for matching; never compare strings.

// ErrNotFound is returned when a requested entity does not exist.
var ErrNotFound = errors.New("not found")

// ErrConflict is returned when an operation would violate a uniqueness constraint
// or produce a duplicate record.
var ErrConflict = errors.New("conflict")

// ErrInvalidInput is returned when caller-supplied input fails validation
// (missing required fields, unsupported values, etc.).
var ErrInvalidInput = errors.New("invalid input")

// Read-path lakehouse errors. These live in the public package rather than in
// internal/federated deliberately: they exist to be discriminated
// programmatically, and an embedder reaching the engine through
// factory.NewEntityManagerWithConfig* cannot import an internal package, which
// would leave it comparing error text — exactly what the errors.Is/errors.As
// contract above forbids.
//
// Both are plain errors, not ErrInvalidInput: nothing about the request is
// wrong. They report that the configured read surface, or the manifest state
// it points at, cannot answer the query. See docs/error-handling.md.

// ErrNoParquetPaths marks a DuckDB-routed federated read whose parquet path set
// resolved empty: no per-request render hint, and either no configured parquet
// source or a source whose manifest lists no files while the fallback glob is
// disabled. Every query that reaches the DuckDB engine wants warm and/or cold
// data (hot-only requests short-circuit to Postgres first), so an empty set
// cannot be answered honestly. Not degradable — a Postgres-only fallback would
// be silently short precisely where the cold tier was requested (#299).
var ErrNoParquetPaths = errors.New("no parquet paths resolved")

// NoParquetPathsError names the schema whose path set came back empty and which
// resolution level was in play, because the two states have different remedies:
// a consulted-but-empty source needs its manifest repaired (or the fallback
// prefix set), while an absent source needs configuring at all.
type NoParquetPathsError struct {
	SchemaID int16
	// SourceConfigured reports whether a parquet source was consulted and
	// returned nothing, as opposed to no source existing to consult.
	SourceConfigured bool
}

func (e *NoParquetPathsError) Error() string {
	if e.SourceConfigured {
		return fmt.Sprintf("schema %d resolved no parquet paths: its manifest lists no files and the fallback glob is disabled; "+
			"repair the manifest (forma-tools manifest-reconcile) or set duckdb.s3DataPrefix", e.SchemaID)
	}
	return fmt.Sprintf("schema %d resolved no parquet paths: no per-request path hint and no manifest parquet source is configured; "+
		"set duckdb.manifestTemplate with duckdb.s3Bucket, or pass federated.s3_parquet_path_template", e.SchemaID)
}

func (e *NoParquetPathsError) Unwrap() error { return ErrNoParquetPaths }

// ErrManifestSchemaMismatch marks a manifest object whose recorded schema ID
// disagrees with the schema being read. A manifest addresses one schema by path
// convention alone; nothing downstream re-checks it — the parquet scan does not
// filter rows by schema (files are per-schema by path) and the projection stamps
// whatever it scans as the requested schema. So a path collision between two
// schemas would not merely under-read, it would serve another schema's rows
// under this schema's identity. Not degradable: cross-schema contamination is
// the opposite of a partial answer.
var ErrManifestSchemaMismatch = errors.New("manifest schema id does not match the schema being read")

// ManifestSchemaMismatchError names both schema IDs and the manifest object, so
// an operator can see which template or object is misaddressed.
type ManifestSchemaMismatchError struct {
	RequestedSchemaID int16
	ManifestSchemaID  int16
	Path              string
}

func (e *ManifestSchemaMismatchError) Error() string {
	return fmt.Sprintf("manifest %s is stamped for schema %d but was loaded for schema %d; "+
		"the manifest path template must resolve a distinct object per schema",
		e.Path, e.ManifestSchemaID, e.RequestedSchemaID)
}

func (e *ManifestSchemaMismatchError) Unwrap() error { return ErrManifestSchemaMismatch }
