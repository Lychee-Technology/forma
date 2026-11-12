//go:build e2e

package production

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/lychee-technology/forma/internal/compaction"
)

// rangeSQLOps spells the four range operators for a direct SQL compare.
var rangeSQLOps = map[string]string{"gt": ">", "gte": ">=", "lt": "<", "lte": "<="}

// rangeProbe is one range filter and, as stored values, the rows it must select.
type rangeProbe struct {
	op, operand string
	want        []string
}

// byteOrderRoutes are the two ways the harness reaches a tier.
var byteOrderRoutes = []struct {
	name    string
	base    func(SchemaRef) Query
	useDuck bool
}{
	{"pg", func(s SchemaRef) Query {
		return Query{Schema: s, PreferHot: true, PreferredTiers: allTiers, Limit: 100}
	}, false},
	{"duck", func(s SchemaRef) Query { return Query{Schema: s, PreferredTiers: allTiers, Limit: 100} }, true},
}

// byteOrderTextValues are stored mixed-case and non-ASCII text. Byte order is
// B < Z < a < zebra < é; en_US.utf8 orders them a, B, é, Z, zebra.
var byteOrderTextValues = []string{"B", "Z", "a", "zebra", "é"}

var byteOrderTextProbes = []rangeProbe{
	{"gt", "B", []string{"Z", "a", "zebra", "é"}},
	{"gte", "B", []string{"B", "Z", "a", "zebra", "é"}},
	{"lt", "B", nil},
	{"lte", "B", []string{"B"}},
	{"gt", "Z", []string{"a", "zebra", "é"}},
	{"gte", "a", []string{"a", "zebra", "é"}},
	{"lt", "a", []string{"B", "Z"}},
	{"lte", "a", []string{"B", "Z", "a"}},
	{"gt", "é", nil},
	{"gte", "é", []string{"é"}},
	{"lt", "é", []string{"B", "Z", "a", "zebra"}},
	{"lte", "zebra", []string{"B", "Z", "a", "zebra"}},
}

const (
	byteOrderUUIDLow  = "00000000-0000-4000-8000-000000000001"
	byteOrderUUIDMid  = "7fffffff-ffff-4fff-bfff-ffffffffffff"
	byteOrderUUIDTop  = "80000000-0000-4000-8000-000000000000"
	byteOrderUUIDA    = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	byteOrderUUIDHigh = "ffffffff-ffff-4fff-bfff-fffffffffffe"
)

var byteOrderUUIDValues = []string{byteOrderUUIDLow, byteOrderUUIDMid, byteOrderUUIDTop, byteOrderUUIDA, byteOrderUUIDHigh}

// The operands are uppercase where they hold a letter: a uuid operand compares
// in its canonical lowercase form.
var byteOrderUUIDProbes = []rangeProbe{
	{"gt", "7FFFFFFF-FFFF-4FFF-BFFF-FFFFFFFFFFFF", []string{byteOrderUUIDTop, byteOrderUUIDA, byteOrderUUIDHigh}},
	{"gte", byteOrderUUIDTop, []string{byteOrderUUIDTop, byteOrderUUIDA, byteOrderUUIDHigh}},
	{"lt", "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA", []string{byteOrderUUIDLow, byteOrderUUIDMid, byteOrderUUIDTop}},
	{"lte", byteOrderUUIDTop, []string{byteOrderUUIDLow, byteOrderUUIDMid, byteOrderUUIDTop}},
}

// TestTextRangeByteOrderAcrossRoutes pins #728's decision: gt, gte, lt and lte
// on text order by UTF-8 bytes whichever route or tier answers, under a
// database whose default collation orders the values differently. Each stage
// probes column-bound text (title) and eav_data text (note) on the Postgres
// route and on the DuckDB route, as the rows move from hot to warm delta to
// cold base parquet. The uuid attributes, column-bound (ref) and eav_data
// (token), run the same way. Expected rows are spelled out, and
// assertDatabaseCollationDisagrees checks that each text probe can tell the two
// orders apart here, so the test cannot pass by the collation agreeing.
func TestTextRangeByteOrderAcrossRoutes(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	for _, p := range byteOrderTextProbes {
		assertDatabaseCollationDisagrees(ctx, t, env, byteOrderTextValues, p)
	}
	seedByteOrderFiller(ctx, t, env, wide)
	textRows := seedByteOrderRows(ctx, t, env, wide, byteOrderTextValues, "title", "note")
	uuidRows := seedByteOrderRows(ctx, t, env, wide, byteOrderUUIDValues, "ref", "token")

	probeTier := func(tier string) {
		t.Helper()
		for _, route := range byteOrderRoutes {
			label := tier + "-" + route.name
			base := route.base(wide)
			probeByteOrder(ctx, t, env, label, base, route.useDuck, []string{"title", "note"}, textRows, byteOrderTextProbes)
			probeByteOrder(ctx, t, env, label, base, route.useDuck, []string{"ref", "token"}, uuidRows, byteOrderUUIDProbes)
		}
	}

	probeTier("hot")

	mustFlush(ctx, t, env)
	probeTier("warm")

	result, err := env.RunCompaction(ctx, wide)
	if err != nil {
		t.Fatalf("compaction: %v", err)
	}
	if result.Outcome != compaction.RewriteApplied && result.Outcome != compaction.PromotionApplied {
		t.Fatalf("compaction moved nothing to base, so the cold stage would not read the probe rows there: %+v", result)
	}
	probeTier("cold")
}

// TestRangeOrderIgnoresTheColumnCollation shows that the order is the one the
// query names, not one the column happens to agree with. en_US.utf8 orders
// canonical uuids by their bytes, so it cannot tell whether a uuid in eav_data
// compares by bytes; an ICU collation that sorts letters before digits can.
// Under it, on columns declared with it, every range still answers in bytes.
func TestRangeOrderIgnoresTheColumnCollation(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	env.ExecSQL(ctx, `CREATE COLLATION letters_first (provider = icu, locale = 'und', rules = '& z < 0 < 1 < 2 < 3 < 4 < 5 < 6 < 7 < 8 < 9')`)
	env.ExecSQL(ctx, `ALTER TABLE eav_data ALTER COLUMN value_text TYPE text COLLATE letters_first`)
	env.ExecSQL(ctx, `ALTER TABLE entity_main ALTER COLUMN text_01 TYPE text COLLATE letters_first`)
	var lettersFirst bool
	if err := env.Pool.QueryRow(ctx, `SELECT 'a'::text COLLATE letters_first < '9'::text COLLATE letters_first`).Scan(&lettersFirst); err != nil {
		t.Fatalf("probe the collation: %v", err)
	}
	if !lettersFirst {
		t.Fatal("letters_first does not sort a before 9, so this test cannot tell the orders apart")
	}

	const digitUUID, letterUUID = "99999999-9999-4999-8999-999999999999", "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	seedByteOrderFiller(ctx, t, env, wide)
	textRows := seedByteOrderRows(ctx, t, env, wide, []string{"9", "a"}, "title", "note")
	uuidRows := seedByteOrderRows(ctx, t, env, wide, []string{digitUUID, letterUUID}, "ref", "token")

	for _, route := range byteOrderRoutes {
		base := route.base(wide)
		probeByteOrder(ctx, t, env, "hot-"+route.name, base, route.useDuck, []string{"title", "note"}, textRows, []rangeProbe{
			{"gt", "9", []string{"a"}},
			{"gte", "a", []string{"a"}},
			{"lt", "a", []string{"9"}},
			{"lte", "9", []string{"9"}},
		})
		probeByteOrder(ctx, t, env, "hot-"+route.name, base, route.useDuck, []string{"ref", "token"}, uuidRows, []rangeProbe{
			{"gt", digitUUID, []string{letterUUID}},
			{"gte", letterUUID, []string{letterUUID}},
			{"lt", letterUUID, []string{digitUUID}},
			{"lte", digitUUID, []string{digitUUID}},
		})
	}
}

func analyzeByteOrderTables(ctx context.Context, env *Env) {
	env.ExecSQL(ctx, `ANALYZE entity_main`)
	env.ExecSQL(ctx, `ANALYZE eav_data`)
}

// seedByteOrderFiller gives the DuckDB route a tier to read before the probe
// rows are hot. The filler holds only ratio, which no probe names.
func seedByteOrderFiller(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	t.Helper()
	mustApplyEvents(ctx, t, env, "seed cold filler", CreateEvent(wide, map[string]any{"ratio": 0.5}))
	if _, err := env.RunInit(ctx, wide); err != nil {
		t.Fatalf("run init: %v", err)
	}
	env.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1", wide.ID)
}

// seedByteOrderRows writes one row per value, holding it in both attributes, and
// returns each row's ID by value.
func seedByteOrderRows(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, values []string, bound, eav string) map[string]uuid.UUID {
	t.Helper()
	events := make(map[string]*Event, len(values))
	batch := make([]*Event, 0, len(values))
	for _, v := range values {
		ev := CreateEvent(wide, map[string]any{bound: v, eav: v})
		events[v] = ev
		batch = append(batch, ev)
	}
	mustApplyEvents(ctx, t, env, "seed range rows", batch...)
	rows := make(map[string]uuid.UUID, len(values))
	for v, ev := range events { // an event's RowID is assigned when it is applied
		rows[v] = ev.RowID
	}
	return rows
}

func probeByteOrder(ctx context.Context, t *testing.T, env *Env, label string, base Query, useDuck bool,
	attrs []string, rows map[string]uuid.UUID, probes []rangeProbe) {
	t.Helper()
	for _, attr := range attrs {
		for _, p := range probes {
			want := make([]uuid.UUID, 0, len(p.want))
			for _, v := range p.want {
				want = append(want, rows[v])
			}
			assertOperandProbe(ctx, t, env, label, base, useDuck, Filter{Attr: attr, Op: p.op, Value: p.operand}, want)
		}
	}
}

// assertDatabaseCollationDisagrees fails unless the database's default collation
// selects other values than the probe expects. A database under C could not
// tell the two orders apart, and this test would prove nothing there.
func assertDatabaseCollationDisagrees(ctx context.Context, t *testing.T, env *Env, values []string, p rangeProbe) {
	t.Helper()
	rows, err := env.Pool.Query(ctx, fmt.Sprintf(`SELECT v FROM unnest($1::text[]) AS v WHERE v %s $2`, rangeSQLOps[p.op]), values, p.operand)
	if err != nil {
		t.Fatalf("compare under the database collation: %v", err)
	}
	selected, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("read the database collation's answer: %v", err)
	}
	want := slices.Sorted(slices.Values(p.want))
	if slices.Equal(slices.Sorted(slices.Values(selected)), want) {
		t.Fatalf("%s:%s selects %v in byte order and under the database collation too; this test needs a collation that disagrees", p.op, p.operand, want)
	}
}
