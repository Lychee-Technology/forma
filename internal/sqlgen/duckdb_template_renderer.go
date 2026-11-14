package sqlgen

import (
	"fmt"
	"strings"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// requireS3Paths fails a parquet-only render whose parquet scan source
// is unbound. The template renders FROM {{.S3_SCAN_SOURCE}} with no guard of
// its own, so an absent binding used to reach DuckDB as the literal
// "<no value>" — a parser error that classified as a degradable read failure,
// indistinguishable from a transient S3 outage (#299). The engine now rejects
// an empty resolved path set before it gets here (ErrNoParquetPaths); this
// keeps the bad render unreachable from every other caller too, the same way
// requireParquetOnlyProjection keeps the retired toy-projection hole (#222)
// closed.
// It checks S3_SCAN_SOURCE rather than S3_PATHS because that is what the
// template actually consumes since #255 — the scan source is derived from the
// path set in injectDuckDBTemplateParams, so an unresolved path set still
// fails here, and a scan source injected directly is equally covered.
func requireS3Paths(params map[string]any) error {
	if src, ok := params["S3_SCAN_SOURCE"].(string); ok && src != "" {
		return nil
	}
	return fmt.Errorf("advanced DuckDB template render is missing S3_SCAN_SOURCE " +
		"(derived from S3_PATHS); resolve a non-empty parquet path set first " +
		"(render hint or manifest source) — an unbound value renders a " +
		"FROM <no value> parser error (#299, #255)")
}

func defaultIfEmpty(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

// FederatedQueryHasHot reports whether the hot tier participates in a
// federated pass (#184): the hot form stages the widened dirty set and reads
// the hot keys, the hot-excluded form the strict set alone. Empty PreferredTiers means the default
// all-tier form (the HTTP layer and harness both normalize to all three, and
// legacy nil-query renders must keep the historical full shape); a non-empty
// list participates only when it names hot. Routing has already intercepted
// the hot-only cases (engine gate), so a false here always coexists with a
// parquet source. internal/queryplan mirrors this hasHot membership in the
// shape hash — the two must stay in lockstep or the plan cache serves the
// wrong skeleton.
func FederatedQueryHasHot(q *model.FederatedAttributeQuery) bool {
	if q == nil || len(q.PreferredTiers) == 0 {
		return true
	}
	for _, tier := range q.PreferredTiers {
		if tier == model.DataTierHot {
			return true
		}
	}
	return false
}

func injectDuckDBTemplateParams(params map[string]any, q *model.FederatedAttributeQuery, dual *DualClauses) error {
	// The parquet path set and the scan source derived from it are set before
	// the nil-q return too: the template consumes S3_SCAN_SOURCE
	// unconditionally, so every render must be able to bind it.
	if _, ok := params["S3_PATHS"]; !ok {
		if paths, ok := params["DuckDBS3Paths"].([]string); ok && len(paths) > 0 {
			params["S3_PATHS"] = formatDuckDBPathList(paths)
		}
	}
	// The scan source folds the resolved path list with the cold-missing
	// column set (#255) and the cold-pinned set (#371). Derived here, the
	// single point every render flows through, so no two renders of one
	// path set can disagree.
	// EmptyParquetScan (#720) replaces the read with a relation holding no
	// rows, for a schema the engine has shown has no parquet to read.
	if _, ok := params["S3_SCAN_SOURCE"]; !ok {
		missing, _ := params["ColdMissingColumns"].([]ScanColumn)
		if empty, _ := params["EmptyParquetScan"].(bool); empty {
			params["S3_SCAN_SOURCE"] = BuildEmptyParquetScanSource(missing)
		} else if pathsSQL, ok := params["S3_PATHS"].(string); ok && pathsSQL != "" {
			pinned, _ := params["ColdPinnedColumns"].([]ScanColumn)
			params["S3_SCAN_SOURCE"] = BuildParquetScanSource(pathsSQL, missing, pinned)
		}
	}

	if q == nil {
		return nil
	}

	params["SCHEMA_ID"] = q.SchemaID
	params["PAGE_SIZE"] = q.Limit
	params["OFFSET"] = q.Offset

	if _, ok := params["LOGICAL_WHERE_CLAUSE"]; !ok {
		if anchor, ok := params["Anchor"].(map[string]any); ok {
			if cond, ok := anchor["Condition"].(string); ok && cond != "" {
				params["LOGICAL_WHERE_CLAUSE"] = cond
			}
		}
	}

	// Keyset pagination: inject cursor-derived WHERE clause and ORDER BY.
	// The clause uses positional "?" placeholders; its args are appended after
	// all condition args by appendKeysetArgs, matching the clause's position at
	// the end of the visible CTE.
	if q.KeysetCursor.IsActive() {
		// The ORDER BY below renders from the cursor alone, so the cursor must
		// be a continuation of q.AttributeOrders rather than a replacement
		// for it. The federated seams check this too (validateKeysetCursor);
		// repeated here, like ValidateShape in generateKeysetWhereClause, so
		// a direct sqlgen caller cannot render a page ordered on something
		// the request never sorted on (#381 review).
		if err := q.KeysetCursor.ValidateContinuation(q.AttributeOrders); err != nil {
			return fmt.Errorf("inject keyset params: %w", err)
		}
		keysetClause, keysetArgs, err := generateKeysetWhereClause(q.KeysetCursor)
		if err != nil {
			return fmt.Errorf("inject keyset params: %w", err)
		}
		params["HAS_KEYSET"] = true
		params["KEYSET_WHERE_CLAUSE"] = keysetClause
		params["KEYSET_ARGS"] = keysetArgs
		params["ORDER_BY"] = buildKeysetOrderBy(q.KeysetCursor)
	} else {
		params["HAS_KEYSET"] = false
	}

	// Non-keyset ORDER BY: use AttributeOrders when present, fall back to created_at DESC.
	if _, ok := params["NON_KEYSET_ORDER_BY"]; !ok {
		params["NON_KEYSET_ORDER_BY"] = buildNonKeysetOrderBy(q)
	}
	return nil
}

func formatDuckDBPathList(paths []string) string {
	quoted := make([]string, 0, len(paths))
	for _, path := range paths {
		quoted = append(quoted, fmt.Sprintf("'%s'", sqlutil.EscapeLiteral(path)))
	}
	if len(quoted) == 1 {
		return quoted[0]
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

// buildNonKeysetOrderBy constructs an ORDER BY fragment from a query's AttributeOrders.
// Main-table attributes are sorted by their bound ColumnName; EAV attributes are sorted
// by their logical AttrName, which the unified CTE exposes as a named column via the
// EAV pivot. A trailing "row_id ASC" tiebreak is always appended so equal-key rows have
// a total order — mirroring the PG optimized template's trailing m.ltbase_row_id —
// keeping LIMIT/OFFSET windows stable across requests (#183).
func buildNonKeysetOrderBy(q *model.FederatedAttributeQuery) string {
	return strings.Join(nonKeysetOrderTerms(q), ", ")
}

// nonKeysetOrderTerms returns buildNonKeysetOrderBy's terms, "<column> <dir>"
// each, so the parquet-only template can render the same order with an
// explicit NULLS LAST on every term.
func nonKeysetOrderTerms(q *model.FederatedAttributeQuery) []string {
	const stableTiebreak = "row_id ASC"
	defaultTerms := []string{"created_at DESC", stableTiebreak}
	if q == nil || len(q.AttributeOrders) == 0 {
		return defaultTerms
	}
	var parts []string
	for _, ao := range q.AttributeOrders {
		dir := "ASC"
		if ao.Desc() {
			dir = "DESC"
		}
		if ao.IsMainColumn() {
			parts = append(parts, fmt.Sprintf("%s %s", ao.ColumnName, dir))
		} else if ao.AttrName != "" {
			// EAV attributes are projected as named columns in the unified CTE via the
			// EAV pivot (MAX(CASE WHEN attr_id = N THEN value_col END) AS attr_name),
			// under their parquet column alias (#260).
			parts = append(parts, fmt.Sprintf("%s %s", ParquetAttrColumn(ao.AttrName), dir))
		}
		// If neither ColumnName nor AttrName is set the attribute is unresolvable; skip it.
	}
	if len(parts) == 0 {
		return defaultTerms
	}
	return append(parts, stableTiebreak)
}
