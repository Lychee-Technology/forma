package sqlgen

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"text/template"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/model"
)

// dirtyIDsSentinel is the placeholder rendered into a compiled skeleton where
// the per-request dirty-ID VALUES list belongs. UUID CSV output can never
// contain this text, so a plain string splice is unambiguous.
const dirtyIDsSentinel = "__FORMA_DIRTY_IDS_SENTINEL__"

// flushGraceCutoffSentinel is the placeholder rendered into a compiled
// skeleton where the per-request flush-visibility cutoff (#252) belongs. The
// cutoff is a server-generated epoch-ms int64, never user input, so a literal
// splice at Bind is injection-free and keeps the plan-cache key independent
// of the per-request value (a bind ? would slot into two different positions
// of the HasHot-sensitive arg interleave).
const flushGraceCutoffSentinel = "__FORMA_FLUSH_GRACE_CUTOFF__"

// FlushGraceCutoffDisabled renders the pre-#252 dirty barrier: real
// flushed_at stamps are epoch milliseconds, so `flushed_at >= MaxInt64` is
// never true and only flushed_at = 0 rows count as dirty. It is also the
// defensive default when a render path fails to supply the cutoff —
// rendering 0 would pull every flushed row back to hot (bypassing the
// parquet tiers), and an absent key would render invalid SQL.
const FlushGraceCutoffDisabled int64 = math.MaxInt64

// DuckDBCompiledQuery is the shape/scope-stable half of BuildDuckDBQuery for
// the production path (advanced template + dual clauses): the rendered SQL
// skeleton plus the template-collected args (#142 phase 5). Dirty IDs and all
// condition/keyset operands stay per-request and flow through Bind.
type DuckDBCompiledQuery struct {
	Skeleton string
	TplArgs  []any
	HasDirty bool
	// HasHot records the tier form the skeleton was rendered with (#184):
	// hot-excluded skeletons have no pg_source CTE, so Bind must drop the
	// PgMainArgs occurrence to keep binds aligned with placeholders. The
	// shape hash splits on hasHot membership, so a cache hit never crosses
	// tier forms.
	HasHot bool
}

// CompileDuckDBQuery renders the advanced-template skeleton once for a query
// shape. It mirrors BuildDuckDBQuery's dual-clause branch exactly, with the
// dirty-ID CSV replaced by a sentinel (hasDirty selects the template branch,
// so it must be part of the caller's cache key). It returns nil when the
// input is not the cacheable production path (non-advanced template or empty
// dual clause); callers must then fall back to BuildDuckDBQuery.
func CompileDuckDBQuery(tpl *template.Template, params map[string]any, q *model.FederatedAttributeQuery, dual *DualClauses, hasDirty bool) (*DuckDBCompiledQuery, error) {
	if tpl != AdvancedQueryTemplateDuckDB || dual == nil || dual.DuckClause == "" {
		return nil, nil
	}

	m := make(map[string]any, len(params)+8)
	for k, v := range params {
		m[k] = v
	}
	anchor, ok := m["Anchor"].(map[string]any)
	if !ok || anchor == nil {
		anchor = map[string]any{}
	} else {
		anchorCopy := make(map[string]any, len(anchor)+1)
		for k, v := range anchor {
			anchorCopy[k] = v
		}
		anchor = anchorCopy
	}
	m["Anchor"] = anchor

	// Mirror of BuildDuckDBQuery's dual+advanced branch (kept in lockstep by
	// TestCompiledQueryParity — change both together).
	anchor["Condition"] = dual.DuckClause
	m["PgMainClause"] = dual.PgMainClause
	m["PgMainArgs"] = dual.PgMainArgs
	m["HasPgMainClause"] = dual.PgMainClause != ""
	m["LOGICAL_WHERE_CLAUSE"] = dual.DuckClause
	m["PG_WHERE_CLAUSE"] = defaultIfEmpty(dual.PgMainClause, "1=1")
	hasHot := FederatedQueryHasHot(q)
	m["HasHot"] = hasHot

	if err := injectDuckDBTemplateParams(m, q, dual); err != nil {
		return nil, fmt.Errorf("compile DuckDB query: %w", err)
	}
	if err := requireProjectionParams(m); err != nil {
		return nil, fmt.Errorf("compile DuckDB query: %w", err)
	}
	if err := requireS3Paths(m); err != nil {
		return nil, fmt.Errorf("compile DuckDB query: %w", err)
	}
	// Compile-time keyset arg values are discarded; Bind re-derives them from
	// the request cursor.
	delete(m, "KEYSET_ARGS")

	m["HasDirtyIDs"] = hasDirty
	if hasDirty {
		m["DirtyIDsCSV"] = dirtyIDsSentinel
	} else {
		m["DirtyIDsCSV"] = ""
	}
	// The cutoff is per-request: the skeleton carries the sentinel (keeping
	// the cache key cutoff-independent) and Bind splices the real value.
	m["FlushGraceCutoffMs"] = flushGraceCutoffSentinel

	sql, tplArgs, err := RenderSQLTemplate(tpl, m)
	if err != nil {
		return nil, err
	}
	return &DuckDBCompiledQuery{Skeleton: sql, TplArgs: tplArgs, HasDirty: hasDirty, HasHot: hasHot}, nil
}

// Bind produces the executable SQL and full argument list for a request whose
// shape matches the compiled skeleton: dirty CSV and flush-grace cutoff
// spliced, condition args in the advanced-template interleave (DuckArgs,
// PgMainArgs, DuckArgs), keyset cursor values, then the cached template args.
// It errors on a cursor that fails model.KeysetCursor.ValidateShape rather
// than binding SQL NULL for an unfilled arm (#381 item 7).
func (c *DuckDBCompiledQuery) Bind(q *model.FederatedAttributeQuery, dual DualClauses, dirtyIDs []uuid.UUID, graceCutoffMs int64) (string, []any, error) {
	sql := c.Skeleton
	if c.HasDirty {
		sql = strings.ReplaceAll(sql, dirtyIDsSentinel, RenderDirtyIDsValuesCSV(dirtyIDs))
	}
	sql = strings.ReplaceAll(sql, flushGraceCutoffSentinel, strconv.FormatInt(graceCutoffMs, 10))

	args := make([]any, 0, 2*len(dual.DuckArgs)+len(dual.PgMainArgs)+len(c.TplArgs)+4)
	args = append(args, dual.DuckArgs...)
	if c.HasHot {
		args = append(args, dual.PgMainArgs...)
	}
	args = append(args, dual.DuckArgs...)
	if q != nil && q.KeysetCursor.IsActive() {
		_, keysetArgs, err := generateKeysetWhereClause(q.KeysetCursor)
		if err != nil {
			return "", nil, fmt.Errorf("bind keyset args: %w", err)
		}
		args = append(args, keysetArgs...)
	}
	args = append(args, c.TplArgs...)
	return sql, args, nil
}
