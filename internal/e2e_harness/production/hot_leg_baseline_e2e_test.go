//go:build e2e

package production

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestHotLegLatencyBaseline measures a federated pass on the pgx hot leg
// and the parquet-only statement that bounds its parquet side (#851, #854,
// #856; pgx-hot-leg-plan.md steps 3 and 5). It is a measurement, not a
// check, and takes minutes per size, so it runs only when
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
//   - the engine pass: wall time of the engine's own query, taken first.
//     This is the latency the plan's tables record.
//   - the parquet-only statement (measureParquetOnly): the dirty set read at
//     the pass's cutoff, staged on one connection of the same instance, and
//     the parquet-only statement over the same parquet objects.
//
// Until step 7 the test also timed the attach pass and replayed its
// statement on the attached PostgreSQL catalog against a hot-only control;
// those need the ATTACH the federated client no longer makes.
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

// baselineShapes are a selective lookup, where the parquet side reads little,
// and an unfiltered first page, where the parquet side ranks every row.
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
	// engine is the pass's wall time; engineFetch and engineSnapshot are its
	// plan's duckdb_fetch and pg_snapshot timings.
	engine, engineFetch, engineSnapshot time.Duration
	parquetOnly                         parquetOnlyTiming
}

// measureBaselineSize measures every shape at the Env's current size: every
// engine pass first, then the parquet-only statements.
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
	conn, err := env.Duck.DB.Conn(ctx)
	if err != nil {
		t.Fatalf("take a duckdb connection: %v", err)
	}
	defer func() { _ = conn.Close() }()
	for i := range shapes {
		rows[i].parquetOnly = measureParquetOnly(ctx, t, env, conn, firsts[i])
	}
	return rows
}

// timeBaselineEngine runs the engine once as a warm-up, whose answer the
// parquet-only statement is checked against, then times baselineReps
// passes.
func timeBaselineEngine(ctx context.Context, t *testing.T, env *Env, q Query, row *baselineRow) *QueryResult {
	t.Helper()
	first := runBaselineEngine(ctx, t, env, q)
	var engine, fetch, snapshot []time.Duration
	for range baselineReps(t) {
		start := time.Now()
		res := runBaselineEngine(ctx, t, env, q)
		engine = append(engine, time.Since(start))
		fetch = append(fetch, time.Duration(res.Plan.Timings["duckdb_fetch"])*time.Millisecond)
		snapshot = append(snapshot, time.Duration(res.Plan.Timings["pg_snapshot"])*time.Millisecond)
	}
	row.engine, row.engineFetch, row.engineSnapshot = median(engine), median(fetch), median(snapshot)
	return first
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

// drainTotal reads every row and returns the statement's total_records, or
// 0 when the page is empty.
func drainTotal(rows *sql.Rows) (int64, error) {
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
// Times are ms. The last four columns are the parquet leg of the pass
// (#854): the dirty set's snapshot read and staging, and the parquet-only
// statement.
func formatBaseline(rows []baselineRow) string {
	var b strings.Builder
	b.WriteString("| entity_main | eav_data | change_log | parquet | shape | engine pass | engine pg_snapshot | engine DuckDB fetch | dirty IDs | dirty-set read | staging | parquet-only statement |\n")
	b.WriteString("| ---: | ---: | ---: | ---: | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, r := range rows {
		po := r.parquetOnly
		fmt.Fprintf(&b, "| %d | %d | %d | %d (%.1f MB) | %s | %s | %s | %s | %d | %s | %s | %s |\n",
			r.entityMain, r.eavData, r.changeLog, r.parquetObjects, r.parquetMB, r.shape,
			ms(r.engine), ms(r.engineSnapshot), ms(r.engineFetch),
			po.dirtyIDs, ms(po.snapshotRead), ms(po.stage), ms(po.query))
	}
	return b.String()
}

func ms(d time.Duration) string {
	return strconv.FormatFloat(float64(d.Microseconds())/1000, 'f', 1, 64)
}
