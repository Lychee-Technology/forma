package federated

import (
	"errors"

	"github.com/lychee-technology/forma"
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

// ErrKeysetUnsupportedOnPostgres marks a request that carries a keyset cursor
// and routed to the Postgres-only path. That path builds a
// model.PersistentRecordQuery, which has no cursor field, so it can neither
// apply nor reject the cursor itself: pre-#354 it silently answered an
// unfiltered first page and pagination never advanced. Not degradable — the
// degraded fallback IS the Postgres-only path.
var ErrKeysetUnsupportedOnPostgres = errors.New("keyset cursor unsupported on the postgres-only path")

// ErrNoParquetPaths and ErrManifestSchemaMismatch are defined in the public
// root package and re-exported here for internal call sites. They must be
// matchable by embedders that reach the engine through factory.NewEntityManager*
// and cannot import an internal package — the whole point of #299 was a
// discriminator callers can act on, which a package-private sentinel is not.
// These are aliases, not copies: errors.Is/errors.As behave identically whether
// a caller reaches for the forma.* or federated.* name.
var (
	ErrNoParquetPaths         = forma.ErrNoParquetPaths
	ErrManifestSchemaMismatch = forma.ErrManifestSchemaMismatch
	// ErrParquetSetInconsistent joined them for #301: internal/httpapi
	// classifies it to redact the object keys it carries out of public response
	// bodies, and cannot import this package without pulling DuckDB CGO into a
	// pure-Go test build. Redaction is gated on sentinel evidence rather than on
	// the status, so this holds on any status the error is classified as, not
	// only 5xx.
	ErrParquetSetInconsistent = forma.ErrParquetSetInconsistent
)

type (
	NoParquetPathsError         = forma.NoParquetPathsError
	ManifestSchemaMismatchError = forma.ManifestSchemaMismatchError
	ParquetSetInconsistentError = forma.ParquetSetInconsistentError
)
