package federated

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// This file holds the keyset *validation* seams used by the federated
// pagination path. Keyset SQL generation lives in internal/sqlgen
// (keyset_where.go): it renders positional `?` placeholders because DuckDB
// treats `$n` as an absolute parameter index and shifts every `?` after it
// (#161/#212). An older `$n`-emitting copy of that codegen used to live here
// with no production callers; it was deleted in #217 so the repo carries
// exactly one keyset codegen.

// keysetSystemColumns are the system columns the visible CTE actually
// projects. Both source legs agree on exactly these four names
// (sqlgen.SchemaProjection.buildS3Projection and buildPGProjection): row_id,
// ltbase_created_at AS created_at, changed_at AS ver_ts, deleted_at AS
// deleted_ts. The retired allowlist also admitted updated_at, deleted_at and
// schema_id, which are NOT columns of visible — a cursor on one could only
// ever reach DuckDB's binder. They are no longer blessed here; being
// well-formed identifiers they now pass as ordinary attribute names and fail
// at the binder, which is honest rather than falsely supported (#381).
//
// Looked up on the FOLDED name, LOWER-CASED, like every other map rule below.
// DuckDB resolves an unquoted identifier case-insensitively, so a cursor on
// "ROW_ID" or "Created_At" binds to exactly these columns; normalising the
// lookup key routes those spellings through the system-column branch instead
// of treating them as ordinary attribute names (#381). ParquetAttrColumn is
// the identity on all four, and schema registration already rejects an
// attribute whose fold collides with a reserved parquet column
// (sqlgen.ValidateParquetAttrColumns), so no real attribute can reach them.
var keysetSystemColumns = map[string]struct{}{
	"row_id":     {},
	"created_at": {},
	"ver_ts":     {},
	"deleted_ts": {},
}

// keysetRejectedColumns are columns of visible that are dedup machinery, not
// data. A cursor on either would bind successfully and paginate over the
// dedup rank, so it fails silently-wrong rather than loudly — the one case
// the identifier rule below cannot catch. Both are in scope where the keyset
// predicate renders: the visible CTE selects over ranked, which projects them.
//
// The lookup is on the FOLDED name, LOWER-CASED, because the raw name is not
// the name that reaches SQL: ParquetAttrColumn maps dots and spaces onto
// underscores and strips backticks and brackets, so "source_tier.priority" and
// "[rn]" land on these columns just as surely as the bare spellings do, and it
// preserves case, so "RN" and "Source_Tier_Priority" survive the fold intact.
// DuckDB resolves an unquoted identifier case-insensitively, so those spellings
// bind to the very rn and source_tier_priority the ranked CTE projects: without
// case normalisation the guard is defeated by the shift key, and the cursor
// paginates over the dedup rank — the silently-wrong outcome this set exists to
// prevent (#381). Checking the raw name, or the folded name verbatim, would
// leave the guard bypassable by punctuation or by case. No registered attribute is
// caught by mistake: schema registration rejects an attribute whose fold
// collides with a reserved parquet column, and rn and source_tier_priority are
// both in that reserved set (sqlgen.ValidateParquetAttrColumns).
var keysetRejectedColumns = map[string]struct{}{
	"rn":                   {},
	"source_tier_priority": {},
}

// safeSQLIdentifier matches a bare, unquoted DuckDB identifier. Cursor
// attributes are interpolated into SQL as identifiers, not bound as
// parameters, so this pattern IS the injection barrier (#381 item 2). It is
// applied to the FOLDED name: ParquetAttrColumn turns dots and spaces into
// underscores and strips backticks and brackets, so a legitimate nested
// attribute like "contact.annualIncome" passes, while a quote, semicolon,
// parenthesis or leading digit does not.
var safeSQLIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// parquetAttrFallbackColumn is the placeholder column name ParquetAttrColumn
// substitutes when the fold empties an attribute name. It is a legitimate
// attribute name in its own right, so the cursor rule can only reject a name
// that FOLDS ONTO it — a folded "attr" whose raw attribute was something else,
// whether the fold emptied the name (the empty string, "[]", a bare pair of
// backticks) or merely stripped it down to the literal placeholder, as
// "[attr]" and a backtick-wrapped attr do.
const parquetAttrFallbackColumn = "attr"

// validateKeysetCursor is THE keyset cursor contract, and the only validation
// entry point. Both seams call it — the engine gate (engine.go) and the
// paginated keyset branch (pagination.go) — so the two can no longer disagree
// about what a cursor may be (#381 item 1). It replaces the retired pair
// validateKeysetColumns (a system-column allowlist reachable only from
// ExecuteFederatedPaginatedQuery) and validateKeysetTiebreak (all that
// DBFederatedQueryEngine.Query applied, so that seam accepted arbitrary
// attribute columns the code generator could not correctly reference).
//
// It enforces, in order:
//
//   - the shape rules, shared with any package via model.KeysetCursor
//     (value/column alignment and the trailing row_id tiebreak);
//   - per column, all judged on the ParquetAttrColumn fold — the same fold the
//     code generator emits, so validation and codegen agree by construction:
//     never the writer's empty-name fallback, never the dedup machinery, and
//     otherwise a visible-CTE system column or a safe SQL identifier.
//
// It deliberately does NOT check that an attribute is registered in the
// schema: that needs the metadata cache, and an unregistered but well-formed
// name fails loudly at DuckDB's binder, never silently. The check belongs with
// the caller-facing cursor surface the Postgres-side keyset feature
// introduces.
//
// A plain read-path error mirroring the rest of this file: the route or the
// cursor is wrong for an operator to see, not a caller-facing 4xx.
func validateKeysetCursor(cursor *model.KeysetCursor) error {
	if err := cursor.ValidateShape(); err != nil {
		// Deliberately unwrapped: both call sites (engine.go, pagination.go)
		// already wrap this function's result with "validate keyset cursor",
		// so adding context here would double that prefix. The sqlgen copy of
		// the same call DOES wrap, because there it is the outermost frame.
		return err
	}
	if !cursor.IsActive() {
		return nil
	}
	for _, col := range cursor.Columns {
		// Fold ONCE, then judge every rule on the folded name: that is the
		// name the code generator emits, so a rule applied to the raw name
		// guards a string that never reaches SQL.
		folded := sqlgen.ParquetAttrColumn(col.Attribute)
		if folded == parquetAttrFallbackColumn && col.Attribute != parquetAttrFallbackColumn {
			// The literal "attr" is ParquetAttrColumn's placeholder: it
			// substitutes it whenever the fold empties the name — "", "[]",
			// "``" — and a name like "[attr]" or "`attr`" strips down onto it
			// directly. That placeholder exists for the export writer, which
			// needs some column name for every attribute; here either route
			// would silently retarget the cursor at a real attribute called
			// "attr".
			return fmt.Errorf("keyset cursor column %q folds onto the placeholder %q, which would silently retarget the cursor at a real attribute of that name: name a visible-CTE system column (row_id, created_at, ver_ts, deleted_ts) or a schema attribute that folds to its own name", col.Attribute, folded)
		}
		// Both map lookups are case-normalised: DuckDB resolves unquoted
		// identifiers case-insensitively, so "RN" reaches the same column
		// as "rn". Only the LOOKUP KEY is lower-cased — the emitted name and
		// every error message keep the caller's spelling.
		key := strings.ToLower(folded)
		if _, ok := keysetSystemColumns[key]; ok {
			continue
		}
		if _, ok := keysetRejectedColumns[key]; ok {
			return fmt.Errorf("keyset cursor column %q is federated dedup machinery, not a queryable column: cursor on row_id, created_at, ver_ts, deleted_ts or a schema attribute", col.Attribute)
		}
		if !safeSQLIdentifier.MatchString(folded) {
			return fmt.Errorf("keyset cursor column %q folds to %q, which is not a safe SQL identifier: a cursor column is interpolated as an identifier, so it must match %s after the ParquetAttrColumn fold", col.Attribute, folded, safeSQLIdentifier.String())
		}
	}
	return nil
}

// hasKeysetCursor reports whether the query carries an ACTIVE keyset cursor.
// A nil cursor or an empty column list is the open first page, which carries
// no continuation obligation.
//
// The predicate itself lives on model.KeysetCursor (IsActive) so the
// internal/sqlgen sites that decide whether the clause is RENDERED share it
// with the sites here that decide whether a cursor is HONOURED OR REFUSED
// (#381 item 9). This function is the package-local convenience wrapper that
// also absorbs a nil query.
func hasKeysetCursor(fq *model.FederatedAttributeQuery) bool {
	return fq != nil && fq.KeysetCursor.IsActive()
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
// A plain read-path error mirroring validateKeysetCursor: the submitted
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
