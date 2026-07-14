//go:build e2e

package production

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sqlgen/sqlgentest"
)

// TestTierHintPruningMatrix (#184): every PreferredTiers subset must restrict
// the physical data sources, not just routing metadata. Physical sources come
// in two equivalence classes (#177 acceptance note: base and delta parquet
// share one flat glob and the reader is manifest-blind, so warm and cold are
// one physical source): hot ⇔ the pg_source postgres_scan of entity_main,
// warm∪cold ⇔ read_parquet of the schema glob. The dirty_ids change_log scan
// is a consistency barrier and survives every DuckDB form: excluding hot
// makes unflushed rows consistently invisible — their stale parquet versions
// are discarded, never replaced (adjudicated on the issue).
//
// The seed relies on the harness default routing strategy ("") routing every
// non-gate combination to DuckDB; do NOT set WithRoutingStrategy(Hybrid) here
// or small limits would collapse hasHot combinations to Postgres-only.
func TestTierHintPruningMatrix(t *testing.T) {
	cluster := SharedCluster(t)
	wide := DefaultSchemaFixtures()[1] // e2e_wide
	ctx := context.Background()
	env := NewEnv(t, cluster)

	seed := seedTierMatrix(ctx, t, env, wide)

	for _, tiers := range [][]model.DataTier{
		{model.DataTierHot, model.DataTierWarm},
		{model.DataTierHot, model.DataTierCold},
		{model.DataTierHot, model.DataTierWarm, model.DataTierCold},
		nil, // empty/default: all three tiers
	} {
		assertTierMatrixHasHot(ctx, t, env, wide, tiers)
	}
	for _, tiers := range [][]model.DataTier{
		{model.DataTierWarm},
		{model.DataTierCold},
		{model.DataTierWarm, model.DataTierCold},
	} {
		assertTierMatrixS3Only(ctx, t, env, wide, tiers, seed)
	}
	assertTierMatrixHotGate(ctx, t, env, wide, Query{Schema: wide, PreferredTiers: []model.DataTier{model.DataTierHot}, Limit: 100})
	assertTierMatrixHotGate(ctx, t, env, wide, Query{Schema: wide, PreferHot: true, Limit: 100})
}

// tierMatrixSeed captures the per-tier row populations built by
// seedTierMatrix plus the adversary row and the literals filters need.
type tierMatrixSeed struct {
	cold    []uuid.UUID // pure cold rows (base parquet only, change_log cleaned)
	warm    []uuid.UUID // flushed rows (delta parquet, flushed_at > 0)
	hot     []uuid.UUID // unflushed rows (change_log flushed_at = 0, no parquet)
	rowR    uuid.UUID   // adversary: v1 in base parquet, unflushed v2 in hot
	rV1     string      // R's v1 title (only in parquet)
	coldPos string      // a pure-cold row's title (positive filter control)
}

// seedTierMatrix builds hot, warm, and cold populations in one env through
// the real write/flush/init path: cold via cdc-init + onboarding change_log
// cleanup, warm via a drained flush, hot by leaving creates unflushed. Row R
// is exported to base at v1, then updated in place so its v1 parquet copy is
// shadowed by an unflushed hot v2 — the dirty-barrier adversary.
func seedTierMatrix(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) tierMatrixSeed {
	t.Helper()
	seed := tierMatrixSeed{}

	coldEvents := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 6})
	if err := env.ApplyEvents(ctx, coldEvents...); err != nil {
		t.Fatalf("apply cold creates: %v", err)
	}
	report, err := env.RunInit(ctx, wide)
	if err != nil {
		t.Fatalf("run init: %v", err)
	}
	if report.RowsExported != 6 {
		t.Fatalf("init exported %d rows, want 6", report.RowsExported)
	}
	env.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1", wide.ID)

	rEvent := coldEvents[len(coldEvents)-1]
	seed.rowR = rEvent.RowID
	seed.rV1 = rEvent.Attrs["title"].(string)
	seed.coldPos = coldEvents[0].Attrs["title"].(string)
	for _, ev := range coldEvents[:len(coldEvents)-1] {
		seed.cold = append(seed.cold, ev.RowID)
	}

	warmEvents := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 4})
	if err := env.ApplyEvents(ctx, warmEvents...); err != nil {
		t.Fatalf("apply warm creates: %v", err)
	}
	mustFlush(ctx, t, env)
	for _, ev := range warmEvents {
		seed.warm = append(seed.warm, ev.RowID)
	}

	hotEvents := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 3})
	if err := env.ApplyEvents(ctx, hotEvents...); err != nil {
		t.Fatalf("apply hot creates: %v", err)
	}
	for _, ev := range hotEvents {
		seed.hot = append(seed.hot, ev.RowID)
	}

	rV2 := UpdateEvent(wide, seed.rowR, map[string]any{"title": "tier-hints-r-v2"})
	if err := env.ApplyEvents(ctx, rV2); err != nil {
		t.Fatalf("apply R v2 update: %v", err)
	}

	var dirty int
	if err := env.Pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM change_log WHERE schema_id = $1 AND flushed_at = 0", wide.ID).Scan(&dirty); err != nil {
		t.Fatalf("count dirty rows: %v", err)
	}
	if dirty != len(seed.hot)+1 {
		t.Fatalf("dirty set has %d rows, want %d (hot + adversary)", dirty, len(seed.hot)+1)
	}
	return seed
}

// assertTierMatrixHasHot: combinations containing hot serve the full state
// (oracle-checked) and must render BOTH physical sources.
func assertTierMatrixHasHot(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, tiers []model.DataTier) {
	t.Helper()
	name := tierComboName(tiers)

	result := env.AssertQueryMatches(ctx, Query{Schema: wide, PreferredTiers: tiers, Limit: 100})
	if result == nil {
		return
	}
	if !result.Plan.Routing.UseDuckDB {
		t.Errorf("%s: expected DuckDB routing, got %+v", name, result.Plan.Routing)
	}
	sqlText := duckdbPlanSQL(result.Plan)
	if sqlText == "" {
		t.Errorf("%s: no duckdb source with rendered SQL in plan", name)
		return
	}
	if !strings.Contains(sqlText, "pg_source") {
		t.Errorf("%s: hot requested but pg_source CTE missing from SQL", name)
	}
	if !strings.Contains(sqlText, "read_parquet") {
		t.Errorf("%s: warm/cold requested but read_parquet missing from SQL", name)
	}
	if calls := sqlgentest.FindPostgresScanCalls(sqlText); len(calls) < 3 {
		t.Errorf("%s: hot requested: want >=3 postgres_scan calls (dirty_ids + pg_source), got %d", name, len(calls))
	}
	if !planHasReason(result.Plan, "pushdown fragment") {
		t.Errorf("%s: hot requested but no pushdown-fragment source recorded", name)
	}
	if !planHasReason(result.Plan, "dirty id set fetched") {
		t.Errorf("%s: hot requested: dirty source must keep the data-source reason", name)
	}
	if !planNotesContain(result.Plan, "physically serves warm+cold") {
		t.Errorf("%s: plan must state the physical parquet coverage, notes: %v", name, result.Plan.Notes)
	}
}

// assertTierMatrixS3Only: combinations excluding hot must prune pg_source
// (SQL and plan), serve only parquet-resident rows, and keep the dirty
// barrier: hot rows and the adversary R are consistently invisible.
func assertTierMatrixS3Only(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, tiers []model.DataTier, seed tierMatrixSeed) {
	t.Helper()
	name := tierComboName(tiers)

	result, err := env.Query(ctx, Query{Schema: wide, PreferredTiers: tiers, Limit: 100})
	if err != nil {
		t.Fatalf("%s: query: %v", name, err)
	}
	if !result.Plan.Routing.UseDuckDB {
		t.Errorf("%s: expected DuckDB routing, got %+v", name, result.Plan.Routing)
	}
	assertRowIDSet(t, name, result.Records, seed.cold, seed.warm)

	sqlText := duckdbPlanSQL(result.Plan)
	if sqlText == "" {
		t.Fatalf("%s: no duckdb source with rendered SQL in plan", name)
	}
	if strings.Contains(sqlText, "pg_source") {
		t.Errorf("%s: hot excluded but pg_source CTE still renders", name)
	}
	if !strings.Contains(sqlText, "read_parquet") {
		t.Errorf("%s: read_parquet missing from SQL", name)
	}
	calls := sqlgentest.FindPostgresScanCalls(sqlText)
	if len(calls) != 1 || !strings.Contains(calls[0], "change_log") {
		t.Errorf("%s: want exactly the dirty_ids change_log scan, got %d calls: %v", name, len(calls), calls)
	}
	if planHasReason(result.Plan, "pushdown fragment") {
		t.Errorf("%s: hot excluded but a pushdown-fragment source was recorded", name)
	}
	if !planHasReason(result.Plan, "consistency barrier (dirty-id anti-join)") {
		t.Errorf("%s: hot excluded: dirty source must be labeled a consistency barrier", name)
	}
	requested := make([]string, 0, len(tiers))
	representative := "cold"
	for _, tier := range []model.DataTier{model.DataTierWarm, model.DataTierCold} {
		for _, p := range tiers {
			if p == tier {
				requested = append(requested, string(tier))
			}
		}
	}
	if requested[0] == "warm" {
		representative = "warm"
	}
	if !planNotesContain(result.Plan, "requested parquet tiers: "+strings.Join(requested, ",")) {
		t.Errorf("%s: plan note must echo the requested parquet subset, notes: %v", name, result.Plan.Notes)
	}
	if !planNotesContain(result.Plan, "physically serves warm+cold") {
		t.Errorf("%s: plan must state the physical parquet coverage, notes: %v", name, result.Plan.Notes)
	}
	if tier := duckdbSourceTier(result.Plan); tier != representative {
		t.Errorf("%s: duckdb source tier = %q, want representative %q", name, tier, representative)
	}

	// Adversary invisibility: R's v1 title exists only in base parquet, but
	// the row is dirty — the barrier discards it without replacement.
	assertTitleFilterCount(ctx, t, env, wide, tiers, name+"/adversary-v1", seed.rV1, 0)
	// Positive control on the same attribute/operator/tiers: a clean cold
	// row's title must match, proving the zero above is the barrier at work.
	assertTitleFilterCount(ctx, t, env, wide, tiers, name+"/positive-control", seed.coldPos, 1)
}

// assertTierMatrixHotGate: [hot] and PreferHot short-circuit to the OLTP
// Postgres path — full state, no DuckDB source, and (post-#184) a routing
// entry plus a postgres source so the plan reflects the actual access.
func assertTierMatrixHotGate(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, q Query) {
	t.Helper()
	name := "hot-gate"
	if q.PreferHot {
		name = "prefer-hot"
	}

	result := env.AssertQueryMatches(ctx, q)
	if result == nil {
		return
	}
	if result.Plan.Routing.UseDuckDB {
		t.Errorf("%s: expected Postgres-only routing, got %+v", name, result.Plan.Routing)
	}
	if sqlText := duckdbPlanSQL(result.Plan); sqlText != "" {
		t.Errorf("%s: hot-only query recorded a duckdb source:\n%s", name, sqlText)
	}
	if !planHasEngine(result.Plan, "postgres") {
		t.Errorf("%s: plan must record the postgres source actually served", name)
	}
}

// TestTierHintRoutingAndAnchor (#184 preference behaviors): PreferHot forces
// the hot-only path, empty/default tiers use all three, and UseMainAsAnchor
// must surface in the execution plan even though the template does not act
// on it yet.
func TestTierHintRoutingAndAnchor(t *testing.T) {
	cluster := SharedCluster(t)
	wide := DefaultSchemaFixtures()[1]
	ctx := context.Background()
	env := NewEnv(t, cluster)

	creates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 3})
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply cold creates: %v", err)
	}
	if _, err := env.RunInit(ctx, wide); err != nil {
		t.Fatalf("run init: %v", err)
	}
	env.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1", wide.ID)
	hot := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 2})
	if err := env.ApplyEvents(ctx, hot...); err != nil {
		t.Fatalf("apply hot creates: %v", err)
	}

	// Default (empty) tiers resolve to all three: full state, both sources.
	result := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 100})
	if result != nil {
		if !result.Plan.Routing.UseDuckDB {
			t.Errorf("default tiers: expected DuckDB routing, got %+v", result.Plan.Routing)
		}
		sqlText := duckdbPlanSQL(result.Plan)
		if !strings.Contains(sqlText, "pg_source") || !strings.Contains(sqlText, "read_parquet") {
			t.Errorf("default tiers must render both physical sources, SQL:\n%s", sqlText)
		}
	}

	// PreferHot: hot-only query path.
	result = env.AssertQueryMatches(ctx, Query{Schema: wide, PreferHot: true, Limit: 100})
	if result != nil {
		if result.Plan.Routing.UseDuckDB {
			t.Errorf("PreferHot: expected Postgres-only routing, got %+v", result.Plan.Routing)
		}
		if sqlText := duckdbPlanSQL(result.Plan); sqlText != "" {
			t.Errorf("PreferHot: recorded a duckdb source:\n%s", sqlText)
		}
	}

	// UseMainAsAnchor: the hint must be visible in the plan.
	anchored, err := env.Query(ctx, Query{
		Schema:          wide,
		PreferredTiers:  []model.DataTier{model.DataTierHot, model.DataTierWarm},
		UseMainAsAnchor: true,
		Limit:           100,
	})
	if err != nil {
		t.Fatalf("anchored query: %v", err)
	}
	if !planNotesContain(anchored.Plan, "UseMainAsAnchor hint requested") {
		t.Errorf("UseMainAsAnchor: plan notes must record the hint as requested, got %v", anchored.Plan.Notes)
	}

	// Direct engine callers with empty PreferredTiers get the default
	// all-tier form (#184): no harness normalization in the way here.
	directPlan := &model.ExecutionPlan{Timings: map[string]int64{}, Notes: []string{}}
	page, err := env.Engine().Query(ctx, env.Tables, &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: wide.ID, Limit: 100},
		DuckDBHints:    &model.DuckDBRenderHints{S3ParquetPathTemplate: env.ParquetGlob()},
	}, &model.FederatedQueryOptions{IncludeExecutionPlan: true, ExecutionPlan: directPlan})
	if err != nil {
		t.Fatalf("direct engine query with empty tiers: %v", err)
	}
	if len(page.Records) != 5 {
		t.Errorf("direct empty-tiers query returned %d rows, want 5 (all tiers)", len(page.Records))
	}
	if !directPlan.Routing.UseDuckDB {
		t.Errorf("direct empty-tiers query must route to DuckDB, got %+v", directPlan.Routing)
	}
}

// TestTierHintCustomParquetGlob (#184 S3 path template): an explicit
// S3ParquetPathTemplate must direct read_parquet at the specified glob, not
// the schema's default location. Env B seeds a differently-sized cold
// population under its own prefix; querying env A with B's glob must return
// exactly B's rows.
func TestTierHintCustomParquetGlob(t *testing.T) {
	cluster := SharedCluster(t)
	wide := DefaultSchemaFixtures()[1]
	ctx := context.Background()
	envA := NewEnv(t, cluster)
	envB := NewEnv(t, cluster)

	seedColdOnly(ctx, t, envA, wide, 5)
	bIDs := seedColdOnly(ctx, t, envB, wide, 3)

	warmCold := []model.DataTier{model.DataTierWarm, model.DataTierCold}

	// Baseline: A's default glob serves A's population.
	baseline := envA.AssertQueryMatches(ctx, Query{Schema: wide, PreferredTiers: warmCold, Limit: 100})
	if baseline != nil && len(baseline.Records) != 5 {
		t.Fatalf("baseline: got %d rows, want 5", len(baseline.Records))
	}

	// Override: A queries with B's glob and must see exactly B's rows.
	crossed, err := envA.Query(ctx, Query{
		Schema:                wide,
		PreferredTiers:        warmCold,
		S3ParquetPathTemplate: envB.ParquetGlob(),
		Limit:                 100,
	})
	if err != nil {
		t.Fatalf("cross-glob query: %v", err)
	}
	assertRowIDSet(t, "cross-glob", crossed.Records, bIDs)
	sqlText := duckdbPlanSQL(crossed.Plan)
	if !strings.Contains(sqlText, envB.S3Prefix) {
		t.Errorf("cross-glob SQL must read the overridden glob (prefix %s):\n%s", envB.S3Prefix, sqlText)
	}
}
