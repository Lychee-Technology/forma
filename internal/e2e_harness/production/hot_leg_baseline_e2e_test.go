//go:build e2e

package production

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	fedengine "github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// TestHotLegLatencyBaseline measures how a federated pass on today's attach
// path splits between the PostgreSQL copy and the parquet scan (#851, the
// baseline the pgx hot-leg plan's steps 3 and 5 compare against). It is a
// measurement, not a check, and takes minutes per size, so it runs only when
// FORMA_HOT_LEG_BASELINE_SIZES lists the entity_main sizes to measure:
//
//	FORMA_HOT_LEG_BASELINE_SIZES=10000,100000,1000000 \
//	  go test ./internal/e2e_harness/production -tags=e2e -run TestHotLegLatencyBaseline -v -timeout 3h
//
// FORMA_HOT_LEG_BASELINE_REPS sets the timed repetitions per measurement
// (default 5); the table reports their median.
//
// At each size the hot set is the same: baselineHotSize unflushed writes,
// half of them updates of flushed rows. Each query shape is measured in two
// phases, on the Env's one DuckDB instance:
//
//   - the engine pass: wall time of the engine's own query, taken first, while
//     the instance holds nothing but what the engine attaches. This is the
//     latency later steps compare against.
//   - the replays: the engine's rendered statement, rerun on one connection
//     of the same instance against the same attached PostgreSQL catalog,
//     alternately as rendered (full) and retargeted at a hot-only control
//     schema in the same database. The control holds only the change_log
//     rows the statement's hot-leg predicate keeps, and the entity_main and
//     eav_data rows of those row IDs. So the dirty set, the hot rows, the
//     answer, the catalog, its settings and the parquet objects are all the
//     statement's own; only the flushed rows the statement copies from
//     PostgreSQL and then discards are gone. A profiled run of each checks
//     that DuckDB plans both identically, operator for operator. Full minus
//     hot-only is then what copying and discarding those rows costs the pass,
//     which is what the pgx hot leg removes.
//
// DuckDB's profiler does not split the pass itself: the postgres extension's
// scans report no operator time, whatever they copy. Hence the control.
func TestHotLegLatencyBaseline(t *testing.T) {
	sizes := baselineSizes(t)
	ctx := context.Background()
	// The factory's DuckDB defaults, not the harness's CI-sized ones.
	env := NewEnv(t, SharedCluster(t), WithDuckMemoryMB(4096), WithDuckThreads(4))
	wide := DefaultSchemaFixtures()[1]
	// init-db's change_log index (#850), which the engine's dirty-set read uses.
	env.ExecSQL(ctx, `CREATE INDEX IF NOT EXISTS change_log_flushed_idx ON change_log (schema_id, flushed_at) INCLUDE (row_id, changed_at)`)

	seedBaselineTemplates(ctx, t, env, wide)
	next := int64(0)
	var report []baselineRow
	for _, size := range sizes {
		live := countRows(ctx, t, env, "entity_main")
		if target := size - baselineHotSize/2; target > live {
			next = cloneBaselineRows(ctx, t, env, wide, next, target-live)
		}
		flushAndConsolidate(ctx, t, env, wide)
		baselineHotSet(ctx, t, env, wide, baselineHotSize)
		if got := countRows(ctx, t, env, "entity_main"); got != size {
			t.Fatalf("entity_main holds %d rows, not the requested %d", got, size)
		}
		// Settle the tables as a deployment's are: vacuumed and analyzed, so
		// no autovacuum of the bulk load runs under the timings.
		env.ExecSQL(ctx, "VACUUM (ANALYZE) change_log, entity_main, eav_data")
		rows := measureBaselineSize(ctx, t, env, wide)
		t.Logf("entity_main at %d rows:\n%s", size, formatBaseline(rows))
		report = append(report, rows...)
	}
	t.Logf("hot-leg latency baseline (median of %d):\n%s", baselineReps(t), formatBaseline(report))
}

func baselineSizes(t *testing.T) []int64 {
	raw := os.Getenv("FORMA_HOT_LEG_BASELINE_SIZES")
	if raw == "" {
		t.Skip("set FORMA_HOT_LEG_BASELINE_SIZES (e.g. 10000,100000) to run the hot-leg latency baseline")
	}
	sizes, err := parseBaselineSizes(raw)
	if err != nil {
		t.Fatalf("FORMA_HOT_LEG_BASELINE_SIZES: %v", err)
	}
	return sizes
}

func baselineReps(t *testing.T) int {
	raw := os.Getenv("FORMA_HOT_LEG_BASELINE_REPS")
	if raw == "" {
		return 5
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		t.Fatalf("FORMA_HOT_LEG_BASELINE_REPS: %q is not a positive count", raw)
	}
	return n
}

// baselineShape is one measured query shape.
type baselineShape struct {
	name  string
	query func(title string) Query
}

// baselineShapes are a selective lookup, where the parquet side reads little
// and the copy should dominate, and an unfiltered first page, where the
// parquet side ranks every row.
func baselineShapes(schema SchemaRef) []baselineShape {
	return []baselineShape{
		{name: "title equals (1 row)", query: func(title string) Query {
			return Query{Schema: schema, Filters: []Filter{{Attr: "title", Op: "equals", Value: title}}, Limit: 50}
		}},
		{name: "first page by count", query: func(string) Query {
			return Query{Schema: schema, Sorts: []Sort{{Attr: "count"}}, Limit: 50}
		}},
	}
}

// baselineRow is one size and shape's medians.
type baselineRow struct {
	shape                          string
	entityMain, eavData, changeLog int64
	parquetObjects                 int
	parquetMB                      float64
	engine, engineFetch            time.Duration
	full, hotOnly                  time.Duration
	fullProfile, hotProfile        baselineProfile
	parquetOnly                    parquetOnlyTiming
}

// measureBaselineSize measures every shape at the Env's current size: every
// engine pass first, then the replays.
func measureBaselineSize(ctx context.Context, t *testing.T, env *Env, schema SchemaRef) []baselineRow {
	t.Helper()
	sizes := baselineRow{
		entityMain: countRows(ctx, t, env, "entity_main"),
		eavData:    countRows(ctx, t, env, "eav_data"),
		changeLog:  countRows(ctx, t, env, "change_log"),
	}
	manifests, err := env.loadManifests(ctx)
	if err != nil || manifests[schema.ID] == nil {
		t.Fatalf("load the schema's manifest: %v", err)
	}
	for _, f := range manifests[schema.ID].Files {
		sizes.parquetObjects++
		sizes.parquetMB += float64(f.SizeBytes) / (1 << 20)
	}
	var title string
	if err := env.Pool.QueryRow(ctx, `SELECT text_01 FROM entity_main WHERE ltbase_schema_id = $1 AND text_01 LIKE '%-c%'
		ORDER BY ltbase_row_id DESC LIMIT 1`, schema.ID).Scan(&title); err != nil {
		t.Fatalf("pick a cloned title: %v", err)
	}
	shapes := baselineShapes(schema)
	rows := make([]baselineRow, len(shapes))
	firsts := make([]*QueryResult, len(shapes))
	for i, shape := range shapes {
		rows[i] = sizes
		rows[i].shape = shape.name
		firsts[i] = timeBaselineEngine(ctx, t, env, shape.query(title), &rows[i])
	}
	r := newBaselineReplayer(ctx, t, env)
	defer r.close(ctx, t, env)
	for i := range shapes {
		stmt, params := renderedStatement(t, firsts[i].Plan)
		r.ensureHotOnly(ctx, t, env, stmt)
		replayBaselineShape(ctx, t, r, stmt, params, firsts[i].Total, &rows[i])
		rows[i].parquetOnly = r.measureParquetOnly(ctx, t, env, firsts[i], stmt)
	}
	return rows
}

// timeBaselineEngine runs the engine once as a warm-up, which also renders
// the statement to replay, then times baselineReps passes.
func timeBaselineEngine(ctx context.Context, t *testing.T, env *Env, q Query, row *baselineRow) *QueryResult {
	t.Helper()
	first := runBaselineEngine(ctx, t, env, q)
	var engine, fetch []time.Duration
	for range baselineReps(t) {
		start := time.Now()
		res := runBaselineEngine(ctx, t, env, q)
		engine = append(engine, time.Since(start))
		fetch = append(fetch, time.Duration(res.Plan.Timings["duckdb_fetch"])*time.Millisecond)
	}
	row.engine, row.engineFetch = median(engine), median(fetch)
	return first
}

// replayBaselineShape warms up and times baselineReps alternating rounds of
// the full and the hot-only statement, then profiles one run of each and
// requires the two plans to match.
func replayBaselineShape(ctx context.Context, t *testing.T, r *baselineReplayer, stmt string, params []any, wantTotal int64, row *baselineRow) {
	t.Helper()
	hot, err := retargetHotLeg(stmt, r.alias, baselineHotSchema)
	if err != nil {
		t.Fatalf("retarget the statement at the hot-only control: %v", err)
	}
	var full, hotOnly []time.Duration
	for rep := 0; rep <= baselineReps(t); rep++ {
		f := r.replay(ctx, t, stmt, params, wantTotal)
		h := r.replay(ctx, t, hot, params, wantTotal)
		if rep > 0 { // the first round is the warm-up
			full, hotOnly = append(full, f), append(hotOnly, h)
		}
	}
	row.full, row.hotOnly = median(full), median(hotOnly)
	row.fullProfile = r.profile(ctx, t, stmt, params, wantTotal)
	row.hotProfile = r.profile(ctx, t, hot, params, wantTotal)
	if row.fullProfile.plan != row.hotProfile.plan {
		t.Fatalf("DuckDB plans the hot-only control differently from the statement, so their difference is not the flushed rows' cost.\nstatement:\n%s\ncontrol:\n%s",
			row.fullProfile.plan, row.hotProfile.plan)
	}
	t.Logf("%s: the full and hot-only plans match, %d operators", row.shape, strings.Count(row.fullProfile.plan, "\n"))
}

func runBaselineEngine(ctx context.Context, t *testing.T, env *Env, q Query) *QueryResult {
	t.Helper()
	res, err := env.Query(ctx, q)
	if err != nil {
		t.Fatalf("engine query: %v", err)
	}
	if !res.Plan.Routing.UseDuckDB {
		t.Fatalf("engine query did not route through DuckDB: %+v", res.Plan.Routing)
	}
	return res
}

// renderedStatement returns the DuckDB statement and bind parameters the
// engine ran, from its execution plan.
func renderedStatement(t *testing.T, plan *model.ExecutionPlan) (string, []any) {
	t.Helper()
	for i := len(plan.Sources) - 1; i >= 0; i-- {
		if src := plan.Sources[i]; src.Engine == "duckdb" {
			params := make([]any, len(src.Params))
			for j, p := range src.Params {
				params[j] = p
			}
			return src.SQL, params
		}
	}
	t.Fatalf("execution plan has no DuckDB source: %+v", plan.Sources)
	return "", nil
}

func countRows(ctx context.Context, t *testing.T, env *Env, table string) int64 {
	t.Helper()
	var n int64
	if err := env.Pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func median(ds []time.Duration) time.Duration {
	s := slices.Clone(ds)
	slices.Sort(s)
	return s[len(s)/2]
}

// baselineHotSchema is the hot-only control schema, in the test's private
// database.
const baselineHotSchema = "hot_leg_hot_only"

// baselineReplayer reruns a rendered statement on one pinned DuckDB
// connection of the Env's client, which carries the engine's S3 settings and
// attaches the same PostgreSQL catalog the engine reads, with the same
// settings.
type baselineReplayer struct {
	conn       *sql.Conn
	alias, out string
	cutoff     int64
}

func newBaselineReplayer(ctx context.Context, t *testing.T, env *Env) *baselineReplayer {
	t.Helper()
	connStr := fedengine.DuckDBPostgresConnStringFromPool(env.Pool)
	conn, err := env.Duck.DB.Conn(ctx)
	if err != nil {
		t.Fatalf("take a duckdb connection: %v", err)
	}
	r := &baselineReplayer{conn: conn, alias: sqlgen.PostgresCatalogAlias(connStr), out: filepath.Join(t.TempDir(), "profile.json")}
	r.exec(ctx, t, sqlgen.PostgresAttachSQL(connStr))
	return r
}

// close drops the hot-only control and releases the connection.
func (r *baselineReplayer) close(ctx context.Context, t *testing.T, env *Env) {
	t.Helper()
	if r.cutoff != 0 {
		env.ExecSQL(ctx, "DROP SCHEMA "+baselineHotSchema+" CASCADE")
		r.exec(ctx, t, "CALL postgres_clear_cache()")
	}
	_ = r.conn.Close()
}

func (r *baselineReplayer) exec(ctx context.Context, t *testing.T, stmt string) {
	t.Helper()
	if _, err := r.conn.ExecContext(ctx, stmt); err != nil {
		t.Fatalf("duckdb %q: %v", stmt, err)
	}
}

// ensureHotOnly builds the hot-only control for the statement's flush
// cutoff, unless it holds that cutoff's rows already. The engine renders the
// cutoff from the time of the query, so each statement can carry its own.
func (r *baselineReplayer) ensureHotOnly(ctx context.Context, t *testing.T, env *Env, stmt string) {
	t.Helper()
	cutoff, err := hotLegCutoff(stmt)
	if err != nil {
		t.Fatalf("read the statement's hot-leg predicate: %v", err)
	}
	if cutoff == r.cutoff {
		return
	}
	r.cutoff = cutoff
	h := baselineHotSchema
	env.ExecSQL(ctx, "DROP SCHEMA IF EXISTS "+h+" CASCADE")
	env.ExecSQL(ctx, "CREATE SCHEMA "+h)
	// Autovacuum stays off, so the statistics copied below stay put.
	off := " WITH (autovacuum_enabled = false) AS "
	env.ExecSQL(ctx, `CREATE TABLE `+h+`.change_log`+off+`SELECT * FROM public.change_log WHERE flushed_at = 0 OR flushed_at >= $1`, cutoff)
	env.ExecSQL(ctx, `CREATE TABLE `+h+`.entity_main`+off+`SELECT m.* FROM public.entity_main m
		WHERE EXISTS (SELECT 1 FROM `+h+`.change_log c WHERE c.schema_id = m.ltbase_schema_id AND c.row_id = m.ltbase_row_id)`)
	env.ExecSQL(ctx, `CREATE TABLE `+h+`.eav_data`+off+`SELECT e.* FROM public.eav_data e
		WHERE EXISTS (SELECT 1 FROM `+h+`.change_log c WHERE c.schema_id = e.schema_id AND c.row_id = e.row_id)`)
	// DuckDB's cardinality estimates for a PostgreSQL table, and so its join
	// order, come from pg_class. The control takes the full tables' figures,
	// so DuckDB plans it as it plans the statement (replayBaselineShape
	// checks that it does) and splits its scans into as many tasks.
	for _, table := range []string{"change_log", "entity_main", "eav_data"} {
		env.ExecSQL(ctx, `UPDATE pg_class c SET relpages = s.relpages, reltuples = s.reltuples FROM pg_class s
			WHERE s.oid = $1::regclass AND c.oid = $2::regclass`, "public."+table, h+"."+table)
	}
	// The attached catalog lists schemas once; make it see the new one.
	r.exec(ctx, t, "CALL postgres_clear_cache()")
}

// replay runs stmt, drains every row, checks the statement's total_records
// against the engine's, and returns the wall time.
func (r *baselineReplayer) replay(ctx context.Context, t *testing.T, stmt string, params []any, wantTotal int64) time.Duration {
	t.Helper()
	start := time.Now()
	rows, err := r.conn.QueryContext(ctx, stmt, params...)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	total, err := drainReplay(rows)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if total != wantTotal {
		t.Fatalf("replay counted %d rows, the engine %d", total, wantTotal)
	}
	return elapsed
}

// profile runs stmt once with DuckDB's profiler on and returns what the
// profile shows. Profiling stays off for the timed replays.
func (r *baselineReplayer) profile(ctx context.Context, t *testing.T, stmt string, params []any, wantTotal int64) baselineProfile {
	t.Helper()
	if err := os.Remove(r.out); err != nil && !os.IsNotExist(err) {
		t.Fatalf("remove the previous profile: %v", err)
	}
	r.exec(ctx, t, "PRAGMA enable_profiling = 'json'")
	r.exec(ctx, t, "PRAGMA profiling_output = '"+r.out+"'")
	r.replay(ctx, t, stmt, params, wantTotal)
	r.exec(ctx, t, "PRAGMA disable_profiling")
	raw, err := os.ReadFile(r.out)
	if err != nil {
		t.Fatalf("read the profile: %v", err)
	}
	prof, err := summarizeProfile(raw, stmt, r.alias)
	if err != nil {
		t.Fatalf("profiled replay: %v", err)
	}
	return prof
}

// drainReplay reads every row and returns the statement's total_records, or
// 0 when the page is empty.
func drainReplay(rows *sql.Rows) (int64, error) {
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		return 0, fmt.Errorf("read columns: %w", err)
	}
	totalAt := slices.Index(cols, "total_records")
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	var total int64
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return 0, fmt.Errorf("scan row: %w", err)
		}
		if totalAt >= 0 {
			if n, ok := vals[totalAt].(int64); ok {
				total = n
			}
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate rows: %w", err)
	}
	return total, nil
}

// formatBaseline renders the medians as the Markdown table the plan records.
// Times are ms. The flushed rows' cost is the full replay minus the hot-only
// one, and its share is of the full replay. The last four columns are the
// parquet leg of the pgx pass (#854): the dirty set's snapshot read and
// staging, and the parquet-only statement.
func formatBaseline(rows []baselineRow) string {
	var b strings.Builder
	b.WriteString("| entity_main | eav_data | change_log | parquet | shape | engine pass | engine DuckDB fetch | full replay | hot-only replay | flushed rows' cost | share | rows from PostgreSQL, full | rows from PostgreSQL, hot-only | rows from parquet | dirty IDs | dirty-set read (pgx) | staging | parquet-only statement |\n")
	b.WriteString("| ---: | ---: | ---: | ---: | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, r := range rows {
		share := 0.0
		if r.full > 0 {
			share = 100 * float64(r.full-r.hotOnly) / float64(r.full)
		}
		po := r.parquetOnly
		fmt.Fprintf(&b, "| %d | %d | %d | %d (%.1f MB) | %s | %s | %s | %s | %s | %s | %.0f%% | %d | %d | %d | %d | %s | %s | %s |\n",
			r.entityMain, r.eavData, r.changeLog, r.parquetObjects, r.parquetMB, r.shape,
			ms(r.engine), ms(r.engineFetch), ms(r.full), ms(r.hotOnly), ms(r.full-r.hotOnly), share,
			r.fullProfile.postgresRows, r.hotProfile.postgresRows, r.fullProfile.parquetRows,
			po.dirtyIDs, ms(po.snapshotRead), ms(po.stage), ms(po.query))
	}
	return b.String()
}

func ms(d time.Duration) string {
	return strconv.FormatFloat(float64(d.Microseconds())/1000, 'f', 1, 64)
}
