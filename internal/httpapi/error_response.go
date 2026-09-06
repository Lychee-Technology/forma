package httpapi

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/redact"
	"go.uber.org/zap"
)

// Public error-class tokens surfaced on redacted bodies (#301). A redacted body
// carries no error text at all — operator-detail errors reach the HTTP boundary
// holding bucket-relative S3 object keys and, when DuckDB fails to attach
// postgres_scan, the Postgres password verbatim inside the driver's own message —
// so this token is what a client discriminates on. The only other structured
// datum it may carry is the schema id (errorSchemaID); everything else stays on
// the operator log line.
//
// Classification keys off sentinel evidence alone; disclosure needs that *and*
// a deliberately published message (#313, resolvePublicMessage). A client
// error built without a forma.PublicError carrier keeps its 4xx status but
// takes the redacted branch — deny-by-default on the disclosure axis. Such a
// body carries error_class "internal" plus an error_id, like any other
// redacted response.
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

// errorSchemaID returns the schema the failed read was addressed to, or 0 when
// the chain carries no typed read-path error.
//
// This reverses a design decision. #301 originally asked for "error class +
// schema id"; the design settled on error_class + error_id alone and
// docs/error-handling.md recorded "no schema id" as a constraint on redacted
// bodies. The issue owner has since reinstated the schema id. The reasoning that
// justified excluding it is the same reasoning that permits it: a schema ID is a
// low-value opaque integer, and one already crosses verbatim on the ID-keyed 404s
// documented under "Accepted disclosures inside the allowlist". What it buys is a
// redacted body a client can correlate without an operator round-trip.
//
// errors.As, not a type assertion: these carriers reach the boundary wrapped
// several levels deep — the federated engine returns
// fmt.Errorf("execute duckdb query: %w: %w", carrier, driverErr) — so a bare
// assertion on err would resolve 0 for every real chain.
//
// A zero return is indistinguishable from "no carrier" by design, and safely so:
// schema IDs are always positive here (see APIResponse.SchemaID), which is what
// makes the field's omitempty encoding lossless.
func errorSchemaID(err error) int16 {
	var inconsistent *forma.ParquetSetInconsistentError
	if errors.As(err, &inconsistent) {
		return inconsistent.SchemaID
	}

	var noPaths *forma.NoParquetPathsError
	if errors.As(err, &noPaths) {
		return noPaths.SchemaID
	}

	// RequestedSchemaID, not ManifestSchemaID. The client asked to read the
	// requested schema; the manifest stamp is a foreign id found on the object,
	// belonging to whichever *other* schema misaddressed it. Returning that would
	// answer a request about schema A with schema B's identity — and disclose the
	// existence of an unrelated schema to a caller who never named it. The
	// operator gets both ids on the log line, which is where the collision is
	// diagnosed.
	var mismatch *forma.ManifestSchemaMismatchError
	if errors.As(err, &mismatch) {
		return mismatch.RequestedSchemaID
	}

	// The unstamped species (#522) carries only the requested id: the
	// object names no schema at all, so there is nothing foreign to leak.
	var unstamped *forma.ManifestUnstampedError
	if errors.As(err, &unstamped) {
		return unstamped.RequestedSchemaID
	}

	return 0
}

// publicErrorMessage returns the fixed prose for a class. It must stay free of
// schema ids, object keys, configuration key names, and driver text — everything
// specific belongs on the operator log line instead. The schema id travels as its
// own structured field (APIResponse.SchemaID), never inside this prose.
func publicErrorMessage(class string) string {
	if class == errorClassInternal {
		return "internal error"
	}
	return "internal read error"
}

// redactCredentials removes credential values from a string before it is written
// anywhere — response body or operator log.
//
// #301 redacted bodies but moved the full chain into zap.S().Errorw, and this
// package logged no errors at all beforehand. That made the log a *new* exposure
// surface for the Postgres password, because DuckDB quotes the whole postgres_scan
// connection string back inside its own prose when an attach fails:
//
//	IO Error: Unable to connect to Postgres at "host=… user=… password=… dbname=…"
//
// The secret is driver-authored text, so it has to be scrubbed at the boundary
// rather than at a Forma wrap site. Source-side scrubbing is tracked by #306; this
// is the boundary backstop that protects deployments in the meantime.
//
// The matcher is internal/redact's, shared with the CDC logger rather than
// reimplemented here. That is not tidiness: a naive `'[^']*'` branch mistakes
// libpq's escaped `\'` for a closing quote and emits the password tail past the
// placeholder, which is exactly the bug #290 fixed in the CDC copy.
//
// It is deliberately narrow. Everything else an operator needs — S3 object keys,
// schema ids, endpoint URLs, driver prose — survives verbatim in the log, because
// a blanket redaction would leave operators with an error_id and nothing to
// correlate it against.
func redactCredentials(s string) string {
	return redact.ConnStringPassword(s)
}

// writeError writes an error response.
func writeError(w http.ResponseWriter, statusCode int, message string) error {
	return writeJSON(w, statusCode, APIResponse{
		Success: false,
		Error:   message,
	})
}

// classifyManagerError maps a manager-layer error to an HTTP status code using
// sentinel evidence alone. An error that wraps none of the client sentinels is a
// 500.
//
// The substring heuristic this replaced (`not found` → 404, `duplicate` → 409,
// `invalid`/`required`/`must be`/… → 400) matched on the whole chain, and driver
// prose trips those words routinely: DuckDB renders a missing S3 object as
// `HTTP Error: … 404 (Not Found).`, so an S3 or credential failure answered HTTP
// 404. Redacting the body did not fix that — status is protocol semantics, and a
// client, cache, or alerting rule reads 404 as "the resource is absent", stops
// retrying, and may cache the negative result for what is really a storage
// failure. #301 asked for read-path consistency errors to answer a generic 5xx,
// and AGENTS.md classes them as operator-visible failures, not 4xx.
//
// Deleting the heuristic also removes the last site in the codebase that
// classified an error by string comparison, which AGENTS.md forbids outright.
//
// The cost is that a genuine client error only earns its 4xx by carrying a
// sentinel, and removing the heuristic therefore required a sweep. Six sites
// across four packages had been relying on it and now carry
// forma.ErrInvalidInput (since #313 via the forma.InvalidInputf carrier):
//
//   - internal/entity_query_sort.go — unknown sort attribute (#296)
//   - internal/transform/transformer.go — create or update body omitting a
//     required attribute (validateRequiredAttributesFromInput, the write-only
//     validator; without the sentinel this would answer an opaque 500). The
//     sweep also wrapped the same message in transform's shared
//     attribute_converter.go, and that was a mistake: FromEAVRecords also runs
//     on the read path, so the sentinel there turned persisted drift into a
//     verbatim 400. It has been removed; the write path keeps its 400 from the
//     validator above.
//   - internal/sqlgen/predicate_normalizer.go and dualpath_sql_helpers.go —
//     unknown filter attribute, unparseable filter value, unsupported operator
//   - internal/conditionexpr/parser.go — malformed "op:value", unknown operator,
//     unparseable date
//
// The table in docs/error-handling.md ("Public HTTP error surface") is the
// maintained list, with the caller mistake each one represents.
//
// IF YOU ADD A VALIDATOR that rejects caller input anywhere behind this
// boundary, build the error with forma.InvalidInputf (or NotFoundf/Conflictf)
// so it carries both the sentinel and a published message. There is no
// message-text fallback: an error with no sentinel is a 500 with an opaque
// body, and a bare fmt.Errorf wrap of a sentinel is a 4xx with an opaque body
// (#313) — either way the caller is told nothing about what they got wrong.
func classifyManagerError(err error) int {
	switch {
	case err == nil:
		return http.StatusInternalServerError
	case errors.Is(err, forma.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, forma.ErrConflict):
		return http.StatusConflict
	case errors.Is(err, forma.ErrInvalidInput):
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}

// respondError classifies err, records the chain for operators under
// redactCredentials, and writes a client-safe body.
//
// A 4xx that carries positive sentinel evidence of caller fault AND a
// deliberately published message (forma.PublicError) emits that message: it
// was authored at the wrap site for the caller, and nothing else from the
// chain rides along (#313).
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
// Positive sentinel evidence is the first conjunct of the disclosure decision
// (#301). It is also what classifyManagerError decides on, so for a
// respondError caller the two agree by construction. The check stays separate
// because respondErrorWithStatus also serves callers that pass their own
// status (executeGet), and a status alone must never be able to open the
// disclosed branch.
//
// It is necessary but not sufficient: disclosure additionally requires a
// deliberately published message resolved from the same branch
// (resolvePublicMessage). The whole-tree form here still gates the disclosed
// branch as a cheap precondition, but the load-bearing provenance check is
// per-node inside the resolver.
func isClientError(err error) bool {
	return errors.Is(err, forma.ErrInvalidInput) ||
		errors.Is(err, forma.ErrNotFound) ||
		errors.Is(err, forma.ErrConflict)
}

// resolvePublicMessage returns the message err deliberately published, if any.
//
// This replaces #307's chain-shape heuristic (canDiscloseVerbatim's
// "single-cause chains may disclose verbatim"): shape proved a weak proxy for
// provenance — a single-cause wrap could still embed driver prose in its own
// text, and the one live mixed chain (an unrenderable caller-supplied path
// template, internal/federated/duckdb_query_build.go) collapsed to an opaque
// body precisely when the caller needed guidance. Publication is decided where
// the error is authored.
//
// The resolution itself is forma.ResolvePublicMessage — branch-provenance
// bound (a node publishes only if a client sentinel is reachable from its own
// subtree) and node-matched the way errors.As matches (#362/#363 reviews).
// It lives in the forma package because the decorators' own PublicMessage
// delegation must apply the identical rule: after the #362 fix confined the
// rule to this boundary, wrapping a mixed tree in forma.WrapPublicf
// reconstructed the borrow one level up (#363 review, P1). One shared
// traversal means what a decorator carries is exactly what this boundary
// emits.
func resolvePublicMessage(err error) (string, bool) {
	return forma.ResolvePublicMessage(err)
}

// respondErrorWithStatus is respondError for callers that have already
// classified in order to choose their message (executeGet's 404 wording).
//
// The gate on disclosure is sentinel evidence plus a published message
// (isClientError + resolvePublicMessage) — not the status code, so a
// caller-supplied 4xx cannot promote an operator error to disclosure. The
// status conjunct only ever holds disclosure back.
//
// Every string that leaves this function passes through redactCredentials
// first, body and log alike. The log needs it because DuckDB puts the
// postgres_scan connection string, password included, into its own
// attach-failure prose, and this package's Errorw line is where that would
// otherwise enter log collection and retention. The published message is
// scrubbed too, as defence in depth: it is authored at a wrap site, and a wrap
// site can interpolate a DSN by accident.
//
// Log levels are contract. Every redacted response logs at Errorw whatever its
// status, because a redacted body is the operator's only remaining copy of the
// detail and the production logger runs at Info (cmd/server/main.go). A
// disclosed 4xx logs at Debugw when the chain holds nothing beyond its
// publication — the caller already has everything — but at Warnw when
// operator detail was withheld (forma.HasOperatorDetail): that line is the
// only copy of the detail, so it must clear the Info threshold, without
// inheriting Errorw's alerting weight for something a caller can trigger at
// will. The Warnw branch also mints an error_id shared by line and body (#361).
func respondErrorWithStatus(w http.ResponseWriter, status int, op string, err error, logFields ...any) {
	fields := make([]any, 0, len(logFields)+8)
	fields = append(fields, logFields...)
	fields = append(fields, "status", status)

	// classifyManagerError treats a nil error as 500; the two functions share this
	// file and must not disagree about the same contract. Unreachable from the
	// current call sites, all of which are inside `if err != nil`, but a nil here
	// must yield the redacted path rather than panic in err.Error() below.
	if err == nil {
		err = errors.New("nil error")
	}
	safe := redactCredentials(err.Error())

	if msg, ok := resolvePublicMessage(err); ok && status < http.StatusInternalServerError && isClientError(err) {
		resp := APIResponse{
			Success: false,
			Error:   fmt.Sprintf("%s: %s", op, redactCredentials(msg)),
		}
		if forma.HasOperatorDetail(err) {
			// #361: this Warnw line is the only remaining copy of the withheld
			// detail, so the body carries an error_id the caller can quote back.
			// The detail-less branch stays id-free: its Debugw line does not
			// survive the production Info threshold, and an error_id that
			// correlates to nothing is worse than none.
			resp.ErrorID = uuid.NewString()
			fields = append(fields, "error_id", resp.ErrorID, "error", safe)
			zap.S().Warnw(op, fields...)
		} else {
			fields = append(fields, "error", safe)
			zap.S().Debugw(op, fields...)
		}
		_ = writeJSON(w, status, resp)
		return
	}

	class := errorClass(err)
	errorID := uuid.NewString()
	schemaID := errorSchemaID(err)
	fields = append(fields, "error_class", class, "error_id", errorID)
	// Its own field, not interpolated into the message: operators filter log
	// queries on schema_id, and parsing it back out of prose is what that would
	// otherwise cost. Omitted when zero, matching the body, so a log line never
	// asserts a schema the error did not name.
	if schemaID != 0 {
		fields = append(fields, "schema_id", schemaID)
	}
	fields = append(fields, "error", safe)
	zap.S().Errorw(op, fields...)

	_ = writeJSON(w, status, APIResponse{
		Success:    false,
		Error:      publicErrorMessage(class),
		ErrorClass: class,
		ErrorID:    errorID,
		SchemaID:   schemaID,
	})
}
