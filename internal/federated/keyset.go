package federated

import (
	"fmt"

	"github.com/lychee-technology/forma/internal/model"
)

// This file holds the keyset *validation* seams used by the federated
// pagination path. Keyset SQL generation lives in internal/sqlgen
// (keyset_where.go): it renders positional `?` placeholders because DuckDB
// treats `$n` as an absolute parameter index and shifts every `?` after it
// (#161/#212). An older `$n`-emitting copy of that codegen used to live here
// with no production callers; it was deleted in #217 so the repo carries
// exactly one keyset codegen.

// isSupportedKeysetColumn returns true if the attribute is supported for keyset cursor extraction.
// Currently only system columns and known main table columns are supported.
// EAV-only attributes require schema cache integration and are not yet supported.
func isSupportedKeysetColumn(attribute string) bool {
	switch attribute {
	case "row_id", "created_at", "updated_at", "deleted_at", "ver_ts", "deleted_ts", "schema_id":
		return true
	default:
		// EAV attributes and main column attributes are not yet supported
		return false
	}
}

// validateKeysetColumns checks if all keyset cursor columns are supported.
// Returns an error if any column is unsupported.
func validateKeysetColumns(columns []model.KeysetColumn) error {
	for _, col := range columns {
		if !isSupportedKeysetColumn(col.Attribute) {
			return fmt.Errorf("keyset pagination on attribute %q is not supported (EAV attributes require schema cache)", col.Attribute)
		}
	}
	return nil
}

// validateKeysetTiebreak enforces the keyset caller contract: the final cursor
// column MUST be row_id. The continuation predicate for the last cursor key is
// a strict inequality, so a cursor ending on a non-unique key (created_at, a
// business attribute, ...) excludes every row sharing that key's value at the
// page boundary — an entire tie group is silently skipped (#183). row_id is the
// only version-invariant unique column, so ending the cursor there gives the
// composite key a total order and makes each boundary tie resolvable. Empty and
// nil cursors are a no-op (the open first page carries no tiebreak obligation).
// Mirrors validateKeysetColumns: a plain read-path error, not an
// ErrInvalidInput-wrapped write-path validation.
func validateKeysetTiebreak(cursor *model.KeysetCursor) error {
	if cursor == nil || len(cursor.Columns) == 0 {
		return nil
	}
	last := cursor.Columns[len(cursor.Columns)-1].Attribute
	if last != "row_id" {
		return fmt.Errorf("keyset cursor final column is %q, expected \"row_id\": a cursor not ending on the unique row_id tiebreak silently skips every row tied on the composite key at the page boundary", last)
	}
	return nil
}

// hasKeysetCursor reports whether the query carries an ACTIVE keyset cursor.
// A nil cursor or an empty column list is the open first page, which carries
// no continuation obligation — the same no-op contract validateKeysetTiebreak
// applies.
func hasKeysetCursor(fq *model.FederatedAttributeQuery) bool {
	return fq != nil && fq.KeysetCursor != nil && len(fq.KeysetCursor.Columns) > 0
}

// rejectKeysetOnPostgresOnly fails a cursor-bearing request that reached the
// Postgres-only path. Keyset SQL generation exists only for the DuckDB
// federated template (sqlgen/keyset_where.go); the Postgres-only path has no
// keyset support at all, so honouring the cursor there is impossible and
// dropping it answers an unfiltered first page the caller has already
// consumed (#354).
//
// The guard sits on queryPostgresOnly rather than on any one routing gate
// deliberately: that function is the single confluence of every route to the
// Postgres-only path — the hot-only gate, the routing decision, and the
// degraded fallback — so a Postgres-only route added later cannot bypass this
// check. It is NOT a guard on every route that reaches Postgres at all: the
// federated merge path also reads Postgres through RunOptimizedQuery
// (pagination.go) and is not covered here.
//
// Known consequence of guarding at the confluence rather than at each gate:
// both recordHotOnlyGatePlan (the hot-only gate) and recordRoutedPostgresSource
// (the routing decision) have already written a postgres source into
// opts.ExecutionPlan by the time this rejects, so a caller that inspects the
// plan after the error reads "postgres served this query" for a query that
// never ran. Inert on the response path — the page is nil on error, so
// attachExecutionPlan never stitches the plan onto anything — but it does dent
// the "the plan reflects actual access" contract those recorders exist for
// (#243/#185). Accepted over duplicating this guard at every gate, which would
// trade the single confluence for a set of checks that must be kept in sync.
//
// A plain read-path error mirroring validateKeysetTiebreak: the submitted
// cursor is well-formed, it is the route that cannot serve it.
func rejectKeysetOnPostgresOnly(fq *model.FederatedAttributeQuery) error {
	if !hasKeysetCursor(fq) {
		return nil
	}
	last := fq.KeysetCursor.Columns[len(fq.KeysetCursor.Columns)-1].Attribute
	return fmt.Errorf("keyset cursor over %d column(s) ending at %q cannot be served: this request routed to the postgres-only path, which applies no cursor predicate; reach the federated path instead (drop PreferHot and any hot-only PreferredTiers, and enable DuckDB) or paginate with limit/offset: %w",
		len(fq.KeysetCursor.Columns), last, ErrKeysetUnsupportedOnPostgres)
}

// mayDegradeToPostgres reports whether a DuckDB-path failure may be absorbed
// by the Postgres-only fallback. Beyond the error-class exemptions in
// degradableFederatedError, an active keyset cursor disqualifies the fallback
// outright: the fallback IS the Postgres-only path, which applies no cursor
// predicate (#354), so degrading would answer an unfiltered first page — the
// same silent-loss bargain those exemptions exist to refuse.
func mayDegradeToPostgres(fq *model.FederatedAttributeQuery, opts *model.FederatedQueryOptions, err error) bool {
	return opts != nil && opts.AllowPartialDegradedMode &&
		degradableFederatedError(err) && !hasKeysetCursor(fq)
}

// explainDeclinedDegradation annotates a failure the caller asked to have
// absorbed (AllowPartialDegradedMode) but which the keyset cursor
// disqualified. Without it, an operator sees an unexplained failure on a
// request they configured never to fail. The underlying cause is preserved in
// the wrap chain, so errors.Is on the original classification still holds.
//
// The guard is the exact complement of mayDegradeToPostgres, so the annotation
// fires only when the cursor is the SOLE disqualifier. When the error class
// itself is non-degradable (degradableFederatedError), dropping the cursor
// would not make the request degrade either, so blaming the cursor would hand
// the operator a remedy that cannot work — that failure surfaces unannotated,
// exactly as it did before #354.
func explainDeclinedDegradation(fq *model.FederatedAttributeQuery, opts *model.FederatedQueryOptions, err error) error {
	if opts == nil || !opts.AllowPartialDegradedMode || !hasKeysetCursor(fq) || !degradableFederatedError(err) {
		return err
	}
	return fmt.Errorf("degraded postgres-only fallback declined: the request carries a keyset cursor the postgres-only path cannot apply (#354); retry without a cursor to allow degradation: %w", err)
}
