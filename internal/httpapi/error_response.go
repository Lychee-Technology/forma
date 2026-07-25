package httpapi

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

// Public error-class tokens surfaced on redacted 5xx bodies (#301). They are the
// only thing a client can discriminate on, because the body carries no error
// text: operator-detail errors reach the HTTP boundary holding bucket-relative
// S3 object keys and, when DuckDB fails to attach postgres_scan, the Postgres
// password verbatim inside the driver's own message.
//
// errorClassInternal deliberately absorbs ErrFederatedReadFailed,
// ErrPostgresReadFailed, metadata drift, and transform failures. A client-facing
// retryability taxonomy is a separate design; the redacted body is safe for all
// of them.
const (
	errorClassParquetSetInconsistent = "parquet_set_inconsistent"
	errorClassNoParquetPaths         = "no_parquet_paths"
	errorClassManifestSchemaMismatch = "manifest_schema_mismatch"
	errorClassInternal               = "internal"
)

// errorClass maps an error to its public token using errors.Is, so wrapped
// chains classify identically to bare sentinels. Never match on message text:
// the read-path errors are typed carriers precisely so callers do not have to.
func errorClass(err error) string {
	switch {
	case errors.Is(err, forma.ErrParquetSetInconsistent):
		return errorClassParquetSetInconsistent
	case errors.Is(err, forma.ErrNoParquetPaths):
		return errorClassNoParquetPaths
	case errors.Is(err, forma.ErrManifestSchemaMismatch):
		return errorClassManifestSchemaMismatch
	default:
		return errorClassInternal
	}
}

// publicErrorMessage returns the fixed prose for a class. It must stay free of
// schema ids, object keys, configuration key names, and driver text — everything
// specific belongs on the operator log line instead.
func publicErrorMessage(class string) string {
	if class == errorClassInternal {
		return "internal error"
	}
	return "internal read error"
}

// writeError writes an error response.
func writeError(w http.ResponseWriter, statusCode int, message string) error {
	return writeJSON(w, statusCode, APIResponse{
		Success: false,
		Error:   message,
	})
}

// classifyManagerError maps a manager-layer error to an HTTP status code.
// It first checks for sentinel errors (preferred), then falls back to
// heuristic string matching for errors that do not wrap the sentinels.
func classifyManagerError(err error) int {
	if err == nil {
		return http.StatusInternalServerError
	}

	// Sentinel error checks — use errors.Is so wrapped errors are handled.
	if errors.Is(err, forma.ErrNotFound) {
		return http.StatusNotFound
	}
	if errors.Is(err, forma.ErrConflict) {
		return http.StatusConflict
	}
	if errors.Is(err, forma.ErrInvalidInput) {
		return http.StatusBadRequest
	}

	// Heuristic fallback for errors that do not wrap a sentinel.
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "not found") {
		return http.StatusNotFound
	}

	if strings.Contains(msg, "duplicate") ||
		strings.Contains(msg, "already exists") ||
		strings.Contains(msg, "conflict") {
		return http.StatusConflict
	}

	if strings.Contains(msg, "required") ||
		strings.Contains(msg, "invalid") ||
		strings.Contains(msg, "cannot sort") ||
		strings.Contains(msg, "unknown attribute") ||
		strings.Contains(msg, "must be") ||
		strings.Contains(msg, "unsupported") ||
		strings.Contains(msg, "empty") {
		return http.StatusBadRequest
	}

	return http.StatusInternalServerError
}

// respondError classifies err, records the full chain for operators, and writes
// a client-safe body.
//
// A 4xx that carries positive sentinel evidence of caller fault (isClientError)
// keeps the verbatim message: it describes caller-supplied input, the caller
// needs to know what to fix, and nothing on the write path touches S3 or the
// postgres_scan connection string.
//
// Everything else carries no error text at all (#301). Errors reaching this point hold
// bucket-relative S3 object keys and — when DuckDB fails to attach
// postgres_scan — the Postgres password verbatim inside the driver's own
// message. Redaction is an allowlist rather than a blocklist of known-sensitive
// types precisely because that password originates in driver text, not in a
// Forma error type. The detail is not discarded: it goes to the log under an
// error_id the client can quote back.
func respondError(w http.ResponseWriter, op string, err error, logFields ...any) {
	respondErrorWithStatus(w, classifyManagerError(err), op, err, logFields...)
}

// isClientError reports whether err is provably the caller's fault.
//
// Disclosure cannot key off the classified status: classifyManagerError falls
// back to substring matching over the whole chain, and driver text routinely
// contains its trigger words — DuckDB reports a missing S3 object as
// "404 (Not Found)" and an unresolvable column as "not found in FROM clause!".
// Trusting that heuristic would hand the verbatim branch exactly the errors #301
// exists to redact. Positive sentinel evidence is the only safe gate (#301).
func isClientError(err error) bool {
	return errors.Is(err, forma.ErrInvalidInput) ||
		errors.Is(err, forma.ErrNotFound) ||
		errors.Is(err, forma.ErrConflict)
}

// respondErrorWithStatus is respondError for callers that have already
// classified in order to choose their message (executeGet's 404 wording).
//
// The gate on disclosure is positive sentinel evidence — isClientError — not the
// status code. classifyManagerError reaches its 4xx verdicts partly by substring
// matching over the whole error chain, and driver prose trips those words
// routinely, so a status alone cannot distinguish "the caller mistyped an
// attribute" from "DuckDB could not fetch an S3 object and said 404 (Not Found)".
// The second must not be echoed back.
//
// The status is deliberately left as classified in both branches: a misclassified
// read-path error still answers 404, just with a redacted body. Disclosure and
// status are separate concerns and only the former is a leak.
//
// Every redacted response logs at Errorw whatever its status, because a redacted
// body is the operator's only remaining copy of the detail and the production
// logger runs at Info (cmd/server/main.go). Only the verbatim branch logs at
// Debugw: there, the caller already has the full message.
func respondErrorWithStatus(w http.ResponseWriter, status int, op string, err error, logFields ...any) {
	fields := make([]any, 0, len(logFields)+8)
	fields = append(fields, logFields...)
	fields = append(fields, "status", status)

	if status < http.StatusInternalServerError && isClientError(err) {
		fields = append(fields, "error", err.Error())
		zap.S().Debugw(op, fields...)
		_ = writeError(w, status, fmt.Sprintf("%s: %v", op, err))
		return
	}

	class := errorClass(err)
	errorID := uuid.NewString()
	fields = append(fields, "error_class", class, "error_id", errorID, "error", err.Error())
	zap.S().Errorw(op, fields...)

	_ = writeJSON(w, status, APIResponse{
		Success:    false,
		Error:      publicErrorMessage(class),
		ErrorClass: class,
		ErrorID:    errorID,
	})
}
