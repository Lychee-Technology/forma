//go:build e2e

package production

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// This file is #715's $now acceptance. A deployment that runs DuckDB sends
// every non-transactional Query through the federated engine, and $now is
// PostgreSQL's clock, which DuckDB cannot read: the service reads the
// instant once from PostgreSQL before routing and compares both legs against
// it. The probes compare three deployments' answers, each against the
// truth table the seeded instants give: a PostgreSQL deployment, whose
// statement renders now() inline (the #646 form), and a federated deployment
// on its PostgreSQL route and on its DuckDB route. The attributes cover an
// EAV datetime and date, a unix_ms-bound datetime and date, and an
// rfc3339-bound datetime and date (#842), on every scalar operator with
// offsets, while the rows sit on the hot tier, in delta parquet, and in base
// parquet beside new hot rows.

const serverTimeProps = `{
    "name": { "type": "string" },
    "evAt": { "type": "string", "format": "date-time" },
    "evOn": { "type": "string", "format": "date" },
    "msAt": { "type": "string", "format": "date-time" },
    "msOn": { "type": "string", "format": "date" },
    "txAt": { "type": "string", "format": "date-time" },
    "txOn": { "type": "string", "format": "date" }
  }`

const serverTimeAttrs = `{
  "name": { "attributeID": 1, "valueType": "text", "column_binding": { "col_name": "text_01" } },
  "evAt": { "attributeID": 2, "valueType": "datetime" },
  "evOn": { "attributeID": 3, "valueType": "date" },
  "msAt": { "attributeID": 4, "valueType": "datetime", "column_binding": { "col_name": "bigint_01", "encoding": "unix_ms" } },
  "msOn": { "attributeID": 5, "valueType": "date", "column_binding": { "col_name": "bigint_02", "encoding": "unix_ms" } },
  "txAt": { "attributeID": 6, "valueType": "datetime", "column_binding": { "col_name": "text_02", "encoding": "rfc3339" } },
  "txOn": { "attributeID": 7, "valueType": "date", "column_binding": { "col_name": "text_03", "encoding": "rfc3339" } }
}
`

const dayMs = int64(86_400_000)

// serverTimeRow is a seeded row and the instants it holds, by attribute.
type serverTimeRow struct {
	id     uuid.UUID
	values map[string]int64
}

// serverTimeSeed holds the seeded rows. The datetimes sit hours apart around
// the clock read at seeding, and the dates days apart, so the instant a
// probe compares against cannot drift across a value while the test runs.
type serverTimeSeed struct {
	rows []serverTimeRow
}

func (s *serverTimeSeed) create(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, label string, nowMs int64, n int) {
	t.Helper()
	events := make([]*Event, 0, 4)
	rows := make([]serverTimeRow, 0, 4)
	hours := []int64{-3, -1, 1}
	days := []int64{-2, 0, 2}
	for i := range hours {
		at := nowMs + hours[i]*3_600_000
		on := floorDay(nowMs) + days[i]*dayMs
		row := serverTimeRow{values: map[string]int64{}}
		attrs := map[string]any{"name": fmt.Sprintf("%s-%d-%d", label, n, i)}
		for _, attr := range []string{"evAt", "msAt", "txAt"} {
			attrs[attr] = time.UnixMilli(at).UTC().Format("2006-01-02T15:04:05.000Z")
			row.values[attr] = at
		}
		for _, attr := range []string{"evOn", "msOn", "txOn"} {
			attrs[attr] = time.UnixMilli(on).UTC().Format("2006-01-02")
			row.values[attr] = on
		}
		events = append(events, CreateEvent(schema, attrs))
		rows = append(rows, row)
	}
	// A row holding no date matches no $now comparison, not_equals included.
	events = append(events, CreateEvent(schema, map[string]any{"name": fmt.Sprintf("%s-%d-unset", label, n)}))
	rows = append(rows, serverTimeRow{values: map[string]int64{}})
	mustApplyEvents(ctx, t, env, label+" creates", events...)
	// Applying assigns the row IDs.
	for i, ev := range events {
		rows[i].id = ev.RowID
	}
	s.rows = append(s.rows, rows...)
}

// want answers which rows a probe matches against the instant nowMs.
func (s *serverTimeSeed) want(attr, op string, offsetMs, nowMs int64) []uuid.UUID {
	instant := nowMs + offsetMs
	if attr[2:] == "On" {
		instant = floorDay(instant)
	}
	var ids []uuid.UUID
	for _, row := range s.rows {
		v, ok := row.values[attr]
		if ok && compareInstant(op, v, instant) {
			ids = append(ids, row.id)
		}
	}
	return sortedUUIDs(ids)
}

func compareInstant(op string, v, instant int64) bool {
	switch op {
	case "equals":
		return v == instant
	case "not_equals":
		return v != instant
	case "gt":
		return v > instant
	case "gte":
		return v >= instant
	case "lt":
		return v < instant
	case "lte":
		return v <= instant
	}
	panic("unknown operator " + op)
}

func floorDay(ms int64) int64 {
	q := ms / dayMs
	if ms%dayMs < 0 {
		q--
	}
	return q * dayMs
}

// serverTimeOffsets are the probes' $now operands. The datetime offsets land
// between the seeded hours; the date ones on and between the seeded days.
var serverTimeOffsets = map[string][]struct {
	operand  string
	offsetMs int64
}{
	"At": {{"$now-2h", -2 * 3_600_000}, {"$now+30m", 30 * 60_000}},
	"On": {{"$now", 0}, {"$now+48h", 2 * dayMs}, {"$now-24h", -dayMs}},
}

// serverTimeDeployment is one deployment a probe runs on, and the route its
// answer must report.
type serverTimeDeployment struct {
	name    string
	manager forma.EntityManager
	// duckdb is nil for the PostgreSQL deployment, which records no plan.
	duckdb *bool
}

func serverTimeDeployments(env *Env) []serverTimeDeployment {
	no, yes := false, true
	return []serverTimeDeployment{
		{name: "postgres deployment", manager: env.PostgresEntityManager()},
		{name: "federated, postgres route", manager: env.RoutedEntityManager(forma.RoutingStrategyHybrid), duckdb: &no},
		{name: "federated, duckdb route", manager: env.DuckDBRoutedEntityManager(), duckdb: &yes},
	}
}

func readPostgresClock(ctx context.Context, t *testing.T, env *Env) int64 {
	t.Helper()
	var ms int64
	if err := env.Pool.QueryRow(ctx, "SELECT "+sqlgen.ServerTimeMillisSQL).Scan(&ms); err != nil {
		t.Fatalf("read the postgresql clock: %v", err)
	}
	return ms
}

// runServerTimeProbes runs every attribute, operator and offset on every
// deployment and requires each answer to match the truth table at the
// instant PostgreSQL reads just before the probe.
func runServerTimeProbes(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, phase string, seed *serverTimeSeed, deployments []serverTimeDeployment) {
	t.Helper()
	for _, attr := range []string{"evAt", "evOn", "msAt", "msOn", "txAt", "txOn"} {
		for _, offset := range serverTimeOffsets[attr[2:]] {
			for _, op := range []string{"equals", "not_equals", "gt", "gte", "lt", "lte"} {
				cond := &forma.KvCondition{Attr: attr, Value: op + ":" + offset.operand}
				want := seed.want(attr, op, offset.offsetMs, readPostgresClock(ctx, t, env))
				for _, d := range deployments {
					res, err := d.manager.Query(ctx, &forma.QueryRequest{
						SchemaName: schema.Name, Page: 1, ItemsPerPage: 100, Condition: cond, IncludeExecutionPlan: true,
					})
					label := fmt.Sprintf("%s/%s %s %s:%s", phase, d.name, attr, op, offset.operand)
					if err != nil {
						t.Fatalf("%s: %v", label, err)
					}
					if d.duckdb != nil && (res.ExecutionPlan == nil || res.ExecutionPlan.Routing.UsedDuckDB != *d.duckdb) {
						t.Fatalf("%s: routed %+v, want use_duckdb=%v", label, res.ExecutionPlan, *d.duckdb)
					}
					got := make([]uuid.UUID, 0, len(res.Data))
					for _, rec := range res.Data {
						got = append(got, rec.RowID)
					}
					if g := sortedUUIDs(got); fmt.Sprint(g) != fmt.Sprint(want) {
						t.Errorf("%s = %v, want %v", label, g, want)
					}
				}
			}
		}
	}
}

func TestServerTimeParityAcrossDeploymentsAndTiers(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t), WithSchemaDir(writeSimpleSchemaDir(t, serverTimeProps, serverTimeAttrs)))
	simple := DefaultSchemaFixtures()[0]

	// A flushed anchor row gives the schema its first parquet object and
	// manifest before the probed rows arrive.
	mustApplyEvents(ctx, t, env, "anchor create", CreateEvent(simple, map[string]any{"name": "anchor"}))
	mustFlush(ctx, t, env)

	deployments := serverTimeDeployments(env)
	seed := &serverTimeSeed{}
	seed.create(ctx, t, env, simple, "first", readPostgresClock(ctx, t, env), 0)
	runServerTimeProbes(ctx, t, env, simple, "hot", seed, deployments)

	mustFlush(ctx, t, env)
	runServerTimeProbes(ctx, t, env, simple, "warm", seed, deployments)

	if _, err := env.RunCompaction(ctx, simple); err != nil {
		t.Fatalf("compaction: %v", err)
	}
	seed.create(ctx, t, env, simple, "second", readPostgresClock(ctx, t, env), 1)
	runServerTimeProbes(ctx, t, env, simple, "cold+hot", seed, deployments)
}
