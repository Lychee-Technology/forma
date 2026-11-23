package forma

import (
	"errors"
	"fmt"
	"strings"
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
// They are plain errors, not ErrInvalidInput: nothing about the request is
// wrong. They report that the configured read surface, or the publication
// state it reads, cannot answer the query. See docs/error-handling.md.

// ErrPublicationInconsistent marks a federated read of a schema whose
// publication state cannot be answered from (docs/publication-protocol-v2.md,
// "Readers"): no publication record, a record that is AWAITING_INIT or
// DROPPED or at an epoch other than the one the process pinned, a committed
// manifest that is missing or is not the one PostgreSQL committed, or a listed
// object the store does not hold. It is a read-path consistency error, never a
// 4xx. Not degradable: a PostgreSQL-only answer would drop every row the
// parquet tiers hold, which is the silently short answer this classification
// exists to make loud (#759).
//
// It lives here rather than in internal/federated, whose error type aliases
// this one, so that an embedder can match it, and so that internal/httpapi can
// classify it without importing internal/federated and its DuckDB CGO.
var ErrPublicationInconsistent = errors.New("publication state inconsistent")

// ErrPublishedObjectUnverifiable marks a federated read that could not tell
// whether an object its published generation names exists, the manifest or a
// listed parquet object: the object store answered access denied. S3 answers
// HTTP 403, not 404, for a missing key to a principal without s3:ListBucket on
// the bucket, so the answer is neither proof of loss nor a failed read to
// degrade around. A *PublicationConsistencyError carries it: the read fails
// closed (it matches ErrPublicationInconsistent, so it is not degradable and
// is never charged to the circuit breaker), but it names no missing key. Give
// the reader's principal s3:ListBucket on the bucket besides s3:GetObject, so
// that a missing object answers 404 and is reported as the loss it is.
var ErrPublishedObjectUnverifiable = errors.New("object store denied access to a published object, so whether it exists is unknown")

// PublicationConsistencyError names the schema, the epoch and the generation
// of a federated read's publication fault.
//
// MissingKeys holds the generation's object keys the store does not hold, when
// that is the fault: operator detail that must not cross a public transport.
// internal/httpapi keeps it, and the rest of the message, out of response
// bodies (#301); any new transport owes the same treatment.
type PublicationConsistencyError struct {
	SchemaID   int16
	Epoch      int64
	Generation int64
	// MissingKeys lists the generation's objects the store does not hold,
	// when that is the fault.
	MissingKeys []string
	// Err is the fault.
	Err error
}

func (e *PublicationConsistencyError) Error() string {
	msg := fmt.Sprintf("schema %d epoch %d generation %d: %v", e.SchemaID, e.Epoch, e.Generation, e.Err)
	if len(e.MissingKeys) > 0 {
		msg += fmt.Sprintf(": %d listed object(s) missing from storage: %s", len(e.MissingKeys), strings.Join(e.MissingKeys, ", "))
	}
	return msg
}

// Unwrap yields ErrPublicationInconsistent and the fault.
func (e *PublicationConsistencyError) Unwrap() []error {
	return []error{ErrPublicationInconsistent, e.Err}
}

// ErrParquetTypeIncompatible marks a federated read refused because a live
// parquet object stores an attribute's column in a form the attribute's
// current declaration cannot read without changing values: a type from
// another family, or a value outside what the declared type and its bound
// column can hold (#732). It is a read-path consistency error, never a 4xx:
// the declaration changed after the object was written, and reading on would
// round, truncate, substitute NULL or fail the statement. Not degradable: the
// refusal is deterministic, and a Postgres-only answer would hide it on every
// query until an operator acts.
var ErrParquetTypeIncompatible = errors.New("live parquet stores an attribute in a form its declared type cannot read losslessly")

// ParquetTypeIncompatibleError names the schema, the attribute and its current
// declaration, the offending object and the evidence, per the read-path error
// style. Path is a storage location — operator detail that internal/httpapi
// keeps out of response bodies, like PublicationConsistencyError's keys.
type ParquetTypeIncompatibleError struct {
	SchemaID  int16
	Attribute string
	// Declared is the attribute's declared type, "list<items>" for a list.
	Declared string
	// Column is the bound entity_main column, empty for an EAV-only attribute.
	Column MainColumn
	// Path is the live object; FileType the column's DuckDB type in it.
	Path     string
	FileType string
	// Evidence says why the stored column cannot be read as declared: the
	// type pair, or the first offending value a scan found.
	Evidence string
}

func (e *ParquetTypeIncompatibleError) Error() string {
	storage := "EAV-only"
	if e.Column != "" {
		storage = "bound to " + string(e.Column)
	}
	return fmt.Sprintf("schema %d attribute %s (declared %s, %s) cannot be read from live parquet %s, which stores it as %s: %s; "+
		"the declaration changed after the object was written. Restore the declaration the object was written under; "+
		"converting stored values is an explicit migration (#730). See docs/schema-evolution.md (#710)",
		e.SchemaID, e.Attribute, e.Declared, storage, e.Path, e.FileType, e.Evidence)
}

func (e *ParquetTypeIncompatibleError) Unwrap() error { return ErrParquetTypeIncompatible }
