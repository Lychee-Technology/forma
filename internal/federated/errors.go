package federated

import (
	"errors"
	"fmt"
	"strings"
)

// Sentinel errors classifying federated read-path failures by code branch —
// never by DuckDB driver message text, which is not stable across driver
// versions (#187). Tests and callers discriminate with errors.Is/errors.As;
// the wrap chains keep the raw driver error for logs.

// ErrDuckDBUnavailable marks queries rejected before reaching DuckDB: the
// client is not configured (or closed), or the circuit breaker is open.
// Transient infrastructure — degradable under AllowPartialDegradedMode.
var ErrDuckDBUnavailable = errors.New("duckdb unavailable")

// ErrFederatedReadFailed marks a DuckDB federated read that failed at
// execution or while streaming rows: unreadable parquet (corrupt bytes,
// truncation, schema mismatch), storage-layer rejections (credentials), or a
// mid-stream fault. The referenced objects exist — a manifest-listed object
// missing from storage classifies as ErrParquetSetInconsistent instead.
// Transient/object-scoped — degradable under AllowPartialDegradedMode.
var ErrFederatedReadFailed = errors.New("federated read failed")

// ErrPostgresReadFailed marks the Postgres side of a federated read failing:
// the dirty-ID consistency fetch or a Postgres-source page read. A Postgres
// outage is not degradable in practice — the degraded fallback is itself
// Postgres-only — so this classification is what #187 scenario 9 asserts on
// all its probes; degradability is still decided at the engine seam, not
// here.
var ErrPostgresReadFailed = errors.New("postgres read failed")

// ErrParquetSetInconsistent marks a federated read whose manifest lists
// parquet objects that do not exist in storage. This is a read-path
// consistency error (docs/error-handling.md): the cold tier has lost data,
// so it must surface to operators even under AllowPartialDegradedMode —
// degrading to Postgres-only would re-silence exactly the silent row loss
// this classification exists to make loud (#187 scenario 2).
var ErrParquetSetInconsistent = errors.New("parquet set inconsistent with manifest")

// ErrNoParquetPaths marks a DuckDB-routed federated read whose parquet path
// set resolved empty: no per-request render hint, and either no configured
// parquet source or a source whose manifest lists no files while the fallback
// glob is disabled. Every query reaching the DuckDB engine wants warm and/or
// cold data (the hot-only cases short-circuit to Postgres first), so an empty
// path set means the read surface cannot serve what was asked for.
//
// This is a read-path configuration error, not transient infrastructure, and
// it is NOT degradable (see degradableFederatedError): the pre-#299 behavior
// rendered read_parquet(<no value>), classified it as ErrFederatedReadFailed,
// and let AllowPartialDegradedMode answer a misconfigured deployment from
// Postgres alone — silently short exactly when the cold tier was requested.
var ErrNoParquetPaths = errors.New("no parquet paths resolved")

// NoParquetPathsError names the schema whose path set came back empty and
// which resolution level was in play, because the two states have different
// remedies: a consulted-but-empty source needs its manifest repaired (or the
// fallback prefix set), while an absent source needs configuring at all.
type NoParquetPathsError struct {
	SchemaID int16
	// SourceConfigured reports whether a ParquetSource was consulted and
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

// ParquetSetInconsistentError carries the schema and the missing object keys
// so the message names the offending state, per the read-path error style.
type ParquetSetInconsistentError struct {
	SchemaID    int16
	MissingKeys []string
}

func (e *ParquetSetInconsistentError) Error() string {
	return fmt.Sprintf("schema %d manifest lists %d parquet object(s) missing from storage: %s",
		e.SchemaID, len(e.MissingKeys), strings.Join(e.MissingKeys, ", "))
}

func (e *ParquetSetInconsistentError) Unwrap() error { return ErrParquetSetInconsistent }
