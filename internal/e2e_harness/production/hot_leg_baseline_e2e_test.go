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
// FORMA_HOT_LEG_BASELINE_REPS sets the timed repetitions per variant
// (default 5); the table reports their median.
//
// At each size the hot set is the same: baselineHotSize unflushed writes,
// half of them updates of flushed rows. The engine runs each query shape
// once, and its rendered statement is then replayed on one DuckDB
// connection against three hot-leg catalogs:
//
//   - attach: the PostgreSQL catalog the engine attaches, filter pushdown
//     off, exactly as the engine reads it;
//   - local: an in-memory DuckDB copy of change_log, entity_main and
//     eav_data, taken before timing. The statement reads the same rows
//     without PostgreSQL, so attach minus local is what the copy costs;
//   - empty: the same tables with no rows. What remains is the parquet scan
//     and its merge.
//
// The statement text is unchanged but for the catalog name, so all three
// read the same parquet objects with the same plan shape.
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
		rows := measureBaselineSize(ctx, t, env, wide)
		t.Logf("entity_main at %d rows:\n%s", size, formatBaseline(rows))
		report = append(report, rows...)
	}
	t.Logf("hot-leg latency baseline (median of %d, ms):\n%s", baselineReps(t), formatBaseline(report))
}

// baselineHotSize is the fixed hot set: unflushed writes at every size.
const baselineHotSize = 1_000

func baselineSizes(t *testing.T) []int64 {
	raw := os.Getenv("FORMA_HOT_LEG_BASELINE_SIZES")
	if raw == "" {
		t.Skip("set FORMA_HOT_LEG_BASELINE_SIZES (e.g. 10000,100000) to run the hot-leg latency baseline")
	}
	var sizes []int64
	for _, field := range strings.Split(raw, ",") {
		n, err := strconv.ParseInt(strings.TrimSpace(field), 10, 64)
		if err != nil || n < baselineTemplates+baselineHotSize {
			t.Fatalf("FORMA_HOT_LEG_BASELINE_SIZES: %q is not a size of at least %d", field, baselineTemplates+baselineHotSize)
		}
		sizes = append(sizes, n)
	}
	if !slices.IsSorted(sizes) {
		t.Fatalf("FORMA_HOT_LEG_BASELINE_SIZES must ascend: the loader only grows the tables")
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
	shape                             string
	entityMain, eavData, changeLog    int64
	parquetObjects                    int
	parquetMB                         float64
	engine, engineFetch               time.Duration
	attach, local, empty, materialize time.Duration
}

// measureBaselineSize measures every shape at the Env's current size.
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
	r := newBaselineReplayer(ctx, t, env)
	defer r.close()
	sizes.materialize = r.materialize(ctx, t)
	var rows []baselineRow
	for _, shape := range baselineShapes(schema) {
		row := sizes
		row.shape = shape.name
		measureBaselineShape(ctx, t, env, r, shape.query(title), &row)
		rows = append(rows, row)
	}
	return rows
}

// measureBaselineShape runs the engine once to render the statement, warms
// every variant once, then times baselineReps rounds of all four.
func measureBaselineShape(ctx context.Context, t *testing.T, env *Env, r *baselineReplayer, q Query, row *baselineRow) {
	t.Helper()
	first := runBaselineEngine(ctx, t, env, q)
	stmt, params := renderedStatement(t, first.Plan)
	var engine, fetch, attach, local, empty []time.Duration
	for rep := 0; rep <= baselineReps(t); rep++ {
		start := time.Now()
		res := runBaselineEngine(ctx, t, env, q)
		engineTime := time.Since(start)
		a := r.replay(ctx, t, stmt, params, r.alias, res.Total)
		l := r.replay(ctx, t, stmt, params, baselineLocal, res.Total)
		e := r.replay(ctx, t, stmt, params, baselineEmpty, -1)
		if rep == 0 {
			continue // warm-up
		}
		engine = append(engine, engineTime)
		fetch = append(fetch, time.Duration(res.Plan.Timings["duckdb_fetch"])*time.Millisecond)
		attach, local, empty = append(attach, a), append(local, l), append(empty, e)
	}
	row.engine, row.engineFetch = median(engine), median(fetch)
	row.attach, row.local, row.empty = median(attach), median(local), median(empty)
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

// Catalog names of the replay's local and empty hot-leg copies.
const (
	baselineLocal = "hot_leg_local"
	baselineEmpty = "hot_leg_empty"
)

// baselineReplayer replays a rendered statement on one pinned DuckDB
// connection of the Env's client, which carries the engine's S3 settings and
// the attached PostgreSQL catalog.
type baselineReplayer struct {
	conn  *sql.Conn
	alias string
}

func newBaselineReplayer(ctx context.Context, t *testing.T, env *Env) *baselineReplayer {
	t.Helper()
	connStr := fedengine.DuckDBPostgresConnStringFromPool(env.Pool)
	conn, err := env.Duck.DB.Conn(ctx)
	if err != nil {
		t.Fatalf("take a duckdb connection: %v", err)
	}
	if _, err := conn.ExecContext(ctx, sqlgen.PostgresAttachSQL(connStr)); err != nil {
		_ = conn.Close()
		t.Fatalf("attach the postgres catalog: %v", err)
	}
	return &baselineReplayer{conn: conn, alias: sqlgen.PostgresCatalogAlias(connStr)}
}

func (r *baselineReplayer) close() { _ = r.conn.Close() }

// materialize copies the three hot-leg tables, every column, into the local
// catalog, and their definitions into the empty one. It returns how long the
// local copy took: whole tables, so an upper bound on what the scans copy.
func (r *baselineReplayer) materialize(ctx context.Context, t *testing.T) time.Duration {
	t.Helper()
	var copied time.Duration
	for _, target := range []string{baselineLocal, baselineEmpty} {
		r.exec(ctx, t, "DETACH DATABASE IF EXISTS "+target)
		r.exec(ctx, t, "ATTACH ':memory:' AS "+target)
		r.exec(ctx, t, "CREATE SCHEMA "+target+".public")
		limit := ""
		if target == baselineEmpty {
			limit = " LIMIT 0"
		}
		start := time.Now()
		for _, table := range []string{"change_log", "entity_main", "eav_data"} {
			r.exec(ctx, t, fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM %s%s",
				sqlgen.PostgresTableRef(target, "public", table), sqlgen.PostgresTableRef(r.alias, "public", table), limit))
		}
		if target == baselineLocal {
			copied = time.Since(start)
		}
	}
	return copied
}

func (r *baselineReplayer) exec(ctx context.Context, t *testing.T, stmt string) {
	t.Helper()
	if _, err := r.conn.ExecContext(ctx, stmt); err != nil {
		t.Fatalf("duckdb %q: %v", stmt, err)
	}
}

// replay runs stmt against the hot-leg catalog named catalog, drains every
// row, and returns the wall time. A non-negative wantTotal must match the
// statement's total_records, which ties the replay to the engine's answer.
func (r *baselineReplayer) replay(ctx context.Context, t *testing.T, stmt string, params []any, catalog string, wantTotal int64) time.Duration {
	t.Helper()
	if !strings.Contains(stmt, r.alias+".") {
		t.Fatalf("rendered statement does not read the attached catalog %s", r.alias)
	}
	stmt = strings.ReplaceAll(stmt, r.alias+".", catalog+".")
	start := time.Now()
	rows, err := r.conn.QueryContext(ctx, stmt, params...)
	if err != nil {
		t.Fatalf("replay against %s: %v", catalog, err)
	}
	total, err := drainReplay(rows)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("replay against %s: %v", catalog, err)
	}
	if wantTotal >= 0 && total != wantTotal {
		t.Fatalf("replay against %s counted %d rows, the engine %d", catalog, total, wantTotal)
	}
	return elapsed
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
func formatBaseline(rows []baselineRow) string {
	var b strings.Builder
	b.WriteString("| entity_main | eav_data | change_log | parquet | shape | engine pass | engine DuckDB fetch | attach replay | local replay | empty replay | copy (attach - local) | whole-table copy |\n")
	b.WriteString("| ---: | ---: | ---: | ---: | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, r := range rows {
		fmt.Fprintf(&b, "| %d | %d | %d | %d (%.1f MB) | %s | %s | %s | %s | %s | %s | %s | %s |\n",
			r.entityMain, r.eavData, r.changeLog, r.parquetObjects, r.parquetMB, r.shape,
			ms(r.engine), ms(r.engineFetch), ms(r.attach), ms(r.local), ms(r.empty), ms(r.attach-r.local), ms(r.materialize))
	}
	return b.String()
}

func ms(d time.Duration) string {
	return strconv.FormatFloat(float64(d.Microseconds())/1000, 'f', 1, 64)
}
