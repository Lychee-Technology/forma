package federated

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// keysetCursorAfterCreatedAt builds a minimal well-formed cursor: one ordering
// column plus the trailing row_id tiebreak validateKeysetCursor requires
// (#183). The values are inert — these tests assert routing, never row
// selection, so no seeded row needs to match.
func keysetCursorAfterCreatedAt() *model.KeysetCursor {
	return &model.KeysetCursor{
		Columns: []model.KeysetColumn{
			{Attribute: "created_at", Direction: forma.SortOrderAsc},
			{Attribute: "row_id", Direction: forma.SortOrderAsc},
		},
		Values: []any{int64(500), "00000000-0000-0000-0000-000000000000"},
		Mode:   model.KeysetCursorModeAfter,
	}
}

// TestQueryPreferHotRejectsKeysetCursor pins door 1 of #354: the hot-only gate
// short-circuits to a Postgres-only path with no keyset support, so pre-fix a
// PreferHot request carrying a cursor received an unfiltered first page and
// pagination never advanced.
func TestQueryPreferHotRejectsKeysetCursor(t *testing.T) {
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 5}}
	engine := NewDBFederatedQueryEngine(pg, nil, nil, nil, forma.DuckDBConfig{Enabled: true}, nil, "")

	page, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav"},
		&model.FederatedAttributeQuery{
			AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
			PreferHot:      true,
			KeysetCursor:   keysetCursorAfterCreatedAt(),
		},
		&model.FederatedQueryOptions{IncludeExecutionPlan: true})

	require.Nil(t, page)
	require.ErrorIs(t, err, ErrKeysetUnsupportedOnPostgres)
	require.NotErrorIs(t, err, forma.ErrInvalidInput,
		"settled error class: a plain read-path error, never a write-path validation carrier — rebuilding this as forma.InvalidInputf would silently turn an operator-visible routing failure into a caller-facing 4xx")
	require.Zero(t, pg.queryCalls,
		"fail closed: postgres must not be queried at all, or the engine pays for a page the caller must never see")
}

// TestQueryHotOnlyTiersRejectsKeysetCursor pins door 1's other half: the gate
// fires on PreferredTiers == [hot] as well as on PreferHot (engine.go:150).
func TestQueryHotOnlyTiersRejectsKeysetCursor(t *testing.T) {
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 5}}
	engine := NewDBFederatedQueryEngine(pg, nil, nil, nil, forma.DuckDBConfig{Enabled: true}, nil, "")

	page, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav"},
		&model.FederatedAttributeQuery{
			AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
			PreferredTiers: []model.DataTier{model.DataTierHot},
			KeysetCursor:   keysetCursorAfterCreatedAt(),
		},
		&model.FederatedQueryOptions{IncludeExecutionPlan: true})

	require.Nil(t, page)
	require.ErrorIs(t, err, ErrKeysetUnsupportedOnPostgres)
	require.Zero(t, pg.queryCalls)
}

// TestQueryRejectsKeysetCursorWhenDuckDBDisabled pins door 2b: with DuckDB
// globally disabled every request routes Postgres-only and there is nothing to
// override onto, so a cursor must fail rather than be dropped.
func TestQueryRejectsKeysetCursorWhenDuckDBDisabled(t *testing.T) {
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 5}}
	engine := NewDBFederatedQueryEngine(pg, nil, nil, nil, forma.DuckDBConfig{Enabled: false}, nil, "")

	page, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav"},
		&model.FederatedAttributeQuery{
			AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
			PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierCold},
			KeysetCursor:   keysetCursorAfterCreatedAt(),
		},
		&model.FederatedQueryOptions{IncludeExecutionPlan: true})

	require.Nil(t, page)
	require.ErrorIs(t, err, ErrKeysetUnsupportedOnPostgres)
	require.Zero(t, pg.queryCalls)
}

// TestQueryWithoutCursorStillServesPostgresOnly is the negative control: the
// guard must key on an ACTIVE cursor only, so an absent cursor and an empty
// column list (the open first page — the same no-op contract
// validateKeysetCursor applies) both keep the Postgres-only path working.
func TestQueryWithoutCursorStillServesPostgresOnly(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cursor *model.KeysetCursor
	}{
		{"nil_cursor", nil},
		{"empty_columns", &model.KeysetCursor{Mode: model.KeysetCursorModeAfter}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 3}}
			engine := NewDBFederatedQueryEngine(pg, nil, nil, nil, forma.DuckDBConfig{Enabled: true}, nil, "")

			page, err := engine.Query(context.Background(),
				model.StorageTables{EntityMain: "main", EAVData: "eav"},
				&model.FederatedAttributeQuery{
					AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
					PreferHot:      true,
					KeysetCursor:   tc.cursor,
				},
				&model.FederatedQueryOptions{IncludeExecutionPlan: true})

			require.NoError(t, err)
			require.Equal(t, int64(3), page.TotalRecords)
			require.Equal(t, 1, pg.queryCalls)
		})
	}
}

// TestEvaluateRoutingPolicyKeysetCursorOverridesCostHeuristic pins door 2a of
// #354 at the policy level. forma's DEFAULT strategy is hybrid
// (config_duckdb.go), whose "small result set" rule routes Limit < 1000 to
// Postgres-only — and keyset pagination is by definition a small-Limit shape.
// Postgres applies no cursor predicate, so the cost heuristic must yield.
func TestEvaluateRoutingPolicyKeysetCursorOverridesCostHeuristic(t *testing.T) {
	cfg := forma.DuckDBConfig{Enabled: true, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid}}
	fq := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
		KeysetCursor:   keysetCursorAfterCreatedAt(),
	}

	dec := EvaluateRoutingPolicy(cfg, fq, nil)

	require.True(t, dec.UseDuckDB,
		"a small-limit cursor query must not take the postgres-only shortcut: postgres applies no cursor predicate (#354)")
	require.Contains(t, dec.Reason, "keyset cursor",
		"the override must be visible in the execution plan, not silent")
}

// TestEvaluateRoutingPolicyKeysetCursorDoesNotOverrideExplicitHotOnly pins the
// intent split: a hot-only request is the caller's SEMANTIC declaration, which
// the engine rejects (Task 1) rather than silently reinterprets.
// EvaluateRoutingPolicy is exported, so it must hold this on its own rather
// than relying on the engine gate intercepting first.
//
// Both spellings of hot-only are covered. The PreferredTiers form is the one
// the local hotOnly variable (routing.go, PreferHot only) does NOT see, so
// without the shared isHotOnlyRequest helper the override fires and reroutes
// an explicitly hot-only request.
func TestEvaluateRoutingPolicyKeysetCursorDoesNotOverrideExplicitHotOnly(t *testing.T) {
	for _, tc := range []struct {
		name string
		fq   *model.FederatedAttributeQuery
	}{
		{"prefer_hot_flag", &model.FederatedAttributeQuery{
			AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
			PreferHot:      true,
			KeysetCursor:   keysetCursorAfterCreatedAt(),
		}},
		{"hot_only_preferred_tiers", &model.FederatedAttributeQuery{
			AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
			PreferredTiers: []model.DataTier{model.DataTierHot},
			KeysetCursor:   keysetCursorAfterCreatedAt(),
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := forma.DuckDBConfig{Enabled: true, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid}}

			dec := EvaluateRoutingPolicy(cfg, tc.fq, nil)

			require.False(t, dec.UseDuckDB, "an explicit hot-only request must never be silently rerouted")
			require.NotContains(t, dec.Reason, "keyset cursor",
				"the plan must not report an override that must never happen for a hot-only request")
		})
	}
}

// TestEvaluateRoutingPolicyKeysetCursorDoesNotOverrideDisabledDuckDB pins door
// 2b at the policy level: with DuckDB disabled there is nothing to override
// onto, and the engine guard fails the request instead.
//
// The Reason assertion is what gives this test teeth, though not in the way
// one might expect. UseDuckDB is already false via the !cfg.Enabled early
// return no matter what the override clause does, so the UseDuckDB assertion
// alone cannot fail. Pinning the reason catches a real and distinct mutation:
// degrading that early return into a plain assignment (dropping its `return`)
// lets the strategy heuristics run and rewrite Reason to "hybrid small result
// set" while UseDuckDB stays false — verified by mutation. So this test pins
// "the disabled verdict is final and returns immediately", NOT the override
// clause's cfg.Enabled conjunct; that conjunct is unreachable in every
// ordering reachable today and no mutation of it alone turns this test red.
func TestEvaluateRoutingPolicyKeysetCursorDoesNotOverrideDisabledDuckDB(t *testing.T) {
	cfg := forma.DuckDBConfig{Enabled: false, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid}}
	fq := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
		KeysetCursor:   keysetCursorAfterCreatedAt(),
	}

	dec := EvaluateRoutingPolicy(cfg, fq, nil)

	require.False(t, dec.UseDuckDB, "a disabled engine must never be routed onto")
	require.Equal(t, "duckdb disabled", dec.Reason,
		"the reason must stay the disabled verdict, unmodified by the override clause")
}

// TestEvaluateRoutingPolicyKeysetCursorLeavesAlreadyFederatedReasonIntact pins
// the override clause's !dec.UseDuckDB conjunct. Dropping it changes no
// routing — a cold-only query is federated either way — but it appends the
// override suffix to a decision that was never overridden, so the execution
// plan would report a reroute that did not happen.
func TestEvaluateRoutingPolicyKeysetCursorLeavesAlreadyFederatedReasonIntact(t *testing.T) {
	cfg := forma.DuckDBConfig{Enabled: true, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid}}
	fq := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
		PreferredTiers: []model.DataTier{model.DataTierWarm, model.DataTierCold},
		KeysetCursor:   keysetCursorAfterCreatedAt(),
	}

	dec := EvaluateRoutingPolicy(cfg, fq, nil)

	require.True(t, dec.UseDuckDB, "a cold-only query is federated on its own merits")
	require.Equal(t, "hybrid cold only", dec.Reason,
		"a decision that already chose DuckDB was not overridden, so the plan must not say it was")
}

// TestQueryRoutesKeysetCursorToDuckDBUnderHybridSmallLimit is the same door at
// the engine seam: the cursor must reach the DuckDB builder, postgres must not
// be touched, and the page's plan must explain the reroute.
func TestQueryRoutesKeysetCursorToDuckDBUnderHybridSmallLimit(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 5}}
	duck := &fakeDuckDBExecutor{rows: &singleDuckDBRow{rowID: uuid.New()}}
	var built []model.FederatedAttributeQuery
	engine := newEmptyPageTestEngine(t, pg, duck, &built)

	page, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		&model.FederatedAttributeQuery{
			AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
			KeysetCursor:   keysetCursorAfterCreatedAt(),
		},
		&model.FederatedQueryOptions{IncludeExecutionPlan: true})

	require.NoError(t, err)
	require.Zero(t, pg.queryCalls, "the cursor query must not be served postgres-only")
	require.Equal(t, 1, duck.calls)
	require.True(t, page.ExecutionPlan.Routing.UseDuckDB)
	require.Contains(t, page.ExecutionPlan.Routing.Reason, "keyset cursor")
	require.Len(t, built, 1)
	require.NotNil(t, built[0].KeysetCursor, "the cursor must survive into the DuckDB builder")
}

// TestQueryWithKeysetCursorDoesNotDegradeToPostgres pins door 3 of #354. The
// degraded fallback IS the Postgres-only path, so absorbing a DuckDB failure
// for a cursor-bearing request would answer an unfiltered first page —
// precisely the silent wrong answer the degraded-mode contract exists to
// avoid. This mirrors the five error-class exemptions degradableFederatedError
// already carries for the same reason.
func TestQueryWithKeysetCursorDoesNotDegradeToPostgres(t *testing.T) {
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 1}}
	duck := &fakeDuckDBExecutor{err: fmt.Errorf("forced duck failure")}
	engine := NewDBFederatedQueryEngine(pg, nil, duck, nil,
		forma.DuckDBConfig{Enabled: true, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid}},
		testMetadataCacheSchema7(t), "", withTestParquetPath())

	page, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav"},
		&model.FederatedAttributeQuery{
			// Limit 2000 keeps the hybrid cost rule on the DuckDB side, so this
			// test isolates the degrade decision from Task 2's override.
			AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 2000},
			PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierCold},
			KeysetCursor:   keysetCursorAfterCreatedAt(),
		},
		&model.FederatedQueryOptions{AllowPartialDegradedMode: true})

	require.Nil(t, page)
	require.Error(t, err)
	require.Contains(t, err.Error(), "forced duck failure",
		"the real cause must surface rather than be replaced by the refusal")
	require.Contains(t, err.Error(), "declined",
		"an operator who set AllowPartialDegradedMode must be told why it did not apply")
	// The assertions above pin the prose; these pin the wrap chain. Under a
	// %w -> %v mutation in explainDeclinedDegradation a caller still reads the
	// right words while every downstream sentinel discriminator is gone
	// (ErrFederatedReadFailed, forma.ErrInvalidInput and its 400,
	// ErrParquetSetInconsistent and httpapi's key redaction), so prose alone
	// cannot be the contract in a repo that matches with errors.Is.
	require.ErrorIs(t, err, ErrFederatedReadFailed)
	require.NotErrorIs(t, err, ErrKeysetUnsupportedOnPostgres,
		"the fallback must not have been entered: its guard's rejection must not appear")
	// A cheap cross-task tripwire, not a measure of whether the fallback ran:
	// for a cursor query this counter stays 0 either way, because Task 1's
	// guard is the first statement of queryPostgresOnly. It goes red only if
	// that guard is ever weakened. NotErrorIs above is what pins "never
	// entered".
	require.Zero(t, pg.queryCalls, "postgres must not be queried on this route")
}

// TestQueryWithKeysetCursorDoesNotDegradeOnRecountFailure pins door 3 at its
// SECOND site: the empty-page recount (#181) is a DuckDB query with its own
// degrade branch, so a cursor query deep enough to trigger a recount reaches
// the same fallback by a different route. Without this test the recount site
// can be reverted alone and the whole package stays green — reinstating the
// exact defect, with the fallback running, the misdirecting "enable DuckDB"
// remediation back, and the recount failure dropped from the chain.
func TestQueryWithKeysetCursorDoesNotDegradeOnRecountFailure(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &sequencedDuckDBExecutor{
		rows: []duckDBRowsIterator{&emptyDuckDBRows{}},
		errs: []error{nil, fmt.Errorf("forced recount failure")},
	}
	var built []model.FederatedAttributeQuery
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 7}}
	engine := newEmptyPageTestEngine(t, pg, duck, &built)

	// An empty first page at offset 50 is what reaches the recount; warm+cold
	// tiers keep the hybrid strategy on DuckDB regardless of page size.
	page, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		&model.FederatedAttributeQuery{
			AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10, Offset: 50},
			PreferredTiers: []model.DataTier{model.DataTierWarm, model.DataTierCold},
			KeysetCursor:   keysetCursorAfterCreatedAt(),
		},
		&model.FederatedQueryOptions{AllowPartialDegradedMode: true})

	require.Nil(t, page)
	require.ErrorContains(t, err, "forced recount failure",
		"the real cause must surface rather than be replaced by the refusal")
	require.ErrorContains(t, err, "declined")
	// Prose alone cannot be the contract here any more than at the page-fetch
	// site above: a %w -> %v mutation in the recount site's own wrap
	// (engine.go, "compute empty-page federated count") still reads correctly
	// while flattening the chain, so ErrFederatedReadFailed, forma.ErrInvalidInput
	// (and its 400), ErrParquetSetInconsistent and ErrNoParquetPaths all stop
	// matching for every failure arriving through this door.
	require.ErrorIs(t, err, ErrFederatedReadFailed)
	require.NotErrorIs(t, err, ErrKeysetUnsupportedOnPostgres,
		"the fallback must not have been entered at the recount site either")
	require.Zero(t, pg.queryCalls, "postgres must not be queried on this route")
}

// TestQueryWithKeysetCursorLeavesNonDegradableFailureUnannotated pins the
// complement: when the error CLASS already disqualifies degradation, the
// cursor is not the reason and must not be blamed. Annotating it would tell an
// operator to "retry without a cursor to allow degradation" — an action that
// cannot work, since degradableFederatedError refuses this class with or
// without a cursor.
func TestQueryWithKeysetCursorLeavesNonDegradableFailureUnannotated(t *testing.T) {
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 1}}
	duck := &fakeDuckDBExecutor{err: fmt.Errorf("unrenderable path template: %w", forma.ErrInvalidInput)}
	engine := NewDBFederatedQueryEngine(pg, nil, duck, nil,
		forma.DuckDBConfig{Enabled: true, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid}},
		testMetadataCacheSchema7(t), "", withTestParquetPath())

	page, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav"},
		&model.FederatedAttributeQuery{
			AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 2000},
			PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierCold},
			KeysetCursor:   keysetCursorAfterCreatedAt(),
		},
		&model.FederatedQueryOptions{AllowPartialDegradedMode: true})

	require.Nil(t, page)
	require.ErrorIs(t, err, forma.ErrInvalidInput,
		"the class that refused the degrade must reach the caller unchanged")
	require.NotContains(t, err.Error(), "declined",
		"the cursor is not why this failed, so it must not be offered as the thing to drop")
	require.Zero(t, pg.queryCalls)
}
