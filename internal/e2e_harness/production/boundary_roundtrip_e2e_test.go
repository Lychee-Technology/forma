//go:build e2e

package production

import (
	"context"
	"math"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/model"
)

// maxBoundBigint is the boundary asserted for the bound bigint column.
//
// #205 lifted both float64 hops (write path: exact int64 sidecar through the
// transform chain; read path: typed EAV pivot so the federated projection
// keeps BIGINT through COALESCE/UNION unification), so a bound bigint now
// carries the full int64 range end to end. Pre-#205 the empirical ceiling was
// 1<<62 (the largest float64-exact power of two below 2^63) — see the #174
// finding preserved in git history.
var maxBoundBigint = int64(math.MaxInt64)

// TestBoundBigintAcceptsInt64Probe pins the bigint contract described on
// maxBoundBigint: the write path ACCEPTS an int64 payload without error, and
// the boundary value round-trips through hot storage intact. Post-#205 the
// contract is the full int64 range, so maxBoundBigint is math.MaxInt64 and the
// assertion auto-tightens to it (the exact int64 sidecar carries every int64).
func TestBoundBigintAcceptsInt64Probe(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]

	ev := CreateEvent(wide, map[string]any{
		"title":  "int64-probe",
		"amount": maxBoundBigint,
	})
	if err := env.ApplyEvents(ctx, ev); err != nil {
		t.Fatalf("write path rejected int64 payload: %v", err)
	}
	hot, err := env.Query(ctx, Query{Schema: wide, PreferHot: true, Limit: 10})
	if err != nil {
		t.Fatalf("hot query: %v", err)
	}
	if len(hot.Records) == 0 {
		t.Fatal("hot query returned no records for the int64 probe row")
	}
	got, ok := hot.Records[0].Int64Items["bigint_01"]
	if !ok || got != maxBoundBigint {
		t.Fatalf("bound bigint stored %d (present=%t), want %d (MaxInt64)", got, ok, maxBoundBigint)
	}
}

// TestBoundBigintPreservesNegMaxInt64 pins the #205 Hop-1 fix: pre-#205 the
// float64 write hop deterministically corrupted a -math.MaxInt64 payload to
// math.MinInt64 (float64(-MaxInt64) rounds to -2^63). Post-#205 the exact
// int64 sidecar must deliver the caller's value unchanged.
func TestBoundBigintPreservesNegMaxInt64(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]

	ev := CreateEvent(wide, map[string]any{
		"title":  "bigint-neg-max",
		"amount": int64(-math.MaxInt64),
	})
	if err := env.ApplyEvents(ctx, ev); err != nil {
		t.Fatalf("write path rejected -MaxInt64 payload: %v", err)
	}
	hot, err := env.Query(ctx, Query{Schema: wide, PreferHot: true, Limit: 10})
	if err != nil {
		t.Fatalf("hot query: %v", err)
	}
	if len(hot.Records) == 0 {
		t.Fatal("hot query returned no records for the -MaxInt64 row")
	}
	got, ok := hot.Records[0].Int64Items["bigint_01"]
	if !ok {
		t.Fatal("bound bigint absent from hot record")
	}
	if got != -math.MaxInt64 {
		t.Fatalf("bound bigint stored %d, want %d — the write path must not round through float64 (#205)",
			got, int64(-math.MaxInt64))
	}
}

// TestFederatedReadReturnsStoredMaxInt64 proves the #205 Hop-2 fix: a stored
// math.MaxInt64 — a legal Postgres BIGINT state an external writer can produce,
// injected via ExecSQL because the pre-#205 write path could not produce it
// portably — now survives the federated DuckDB projection exactly. Pre-#205
// the COALESCE(DOUBLE pivot, m.bigint_01) unification made CAST-back overflow
// ("Conversion Error ... out of range for INT64") on every federated read of
// the schema.
func TestFederatedReadReturnsStoredMaxInt64(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]

	ev := CreateEvent(wide, map[string]any{"title": "bigint-ceiling", "amount": int64(1)})
	if err := env.ApplyEvents(ctx, ev); err != nil {
		t.Fatalf("apply create: %v", err)
	}
	// Base file so the federated query routes to DuckDB; the row stays
	// unflushed in change_log, so it is served from the hot pg_source leg.
	if _, err := env.RunInit(ctx, wide); err != nil {
		t.Fatalf("run init: %v", err)
	}
	env.ExecSQL(ctx,
		"UPDATE entity_main SET bigint_01 = $1 WHERE ltbase_schema_id = $2",
		int64(math.MaxInt64), wide.ID)

	// Direct hot path: storage holds MaxInt64 losslessly (unchanged pre/post fix).
	hot, err := env.Query(ctx, Query{Schema: wide, PreferHot: true, Limit: 10})
	if err != nil {
		t.Fatalf("hot query: %v", err)
	}
	if len(hot.Records) == 0 {
		t.Fatal("hot query returned no records")
	}
	if got := hot.Records[0].Int64Items["bigint_01"]; got != math.MaxInt64 {
		t.Fatalf("stored bound bigint via hot read = %d, want %d", got, int64(math.MaxInt64))
	}

	// Federated path: must succeed and be exact (#205).
	fed, ferr := env.Query(ctx, Query{Schema: wide, Limit: 10})
	if ferr != nil {
		t.Fatalf("federated read of stored MaxInt64 must succeed post-#205: %v", ferr)
	}
	if !fed.Plan.Routing.UseDuckDB {
		t.Errorf("federated query did not route to duckdb: %+v", fed.Plan.Routing)
	}
	if len(fed.Records) == 0 {
		t.Fatal("federated query returned no records")
	}
	if got := fed.Records[0].Int64Items["bigint_01"]; got != math.MaxInt64 {
		t.Fatalf("federated bound bigint = %d, want %d (exact MaxInt64)", got, int64(math.MaxInt64))
	}
}

const zeroUUID = "00000000-0000-0000-0000-000000000000"
const maxUUID = "ffffffff-ffff-ffff-ffff-ffffffffffff"

// float64-exact EAV bigint bound: EAV values travel as float64 through the
// Go model (transform.extractValueFromEAVRecord), so 2^53 is the largest
// exactly-representable EAV integer. Bound bigints don't share this limit.
// As of #205 a bound bigint carries the full int64 range, so it no longer
// shares this 2^53 EAV ceiling.
const maxEAVInt = float64(1 << 53)

// preEpochJoinedMS is 1900-01-01T00:00:00Z in epoch milliseconds — the
// b-min row's pre-epoch bound date (negative epoch-ms must survive).
const preEpochJoinedMS = int64(-2208988800000)

// buildBoundaryEvents builds the four boundary shapes: a NULL row (only the bound
// title set), a zero/empty row (every column an explicit zero-value, not
// NULL), a max row (upper bounds of every type + unicode text), and a min row
// (lower bounds + pre-epoch dates that must survive as negative epoch-ms).
func buildBoundaryEvents(wide SchemaRef) (nullRow, zeroRow, maxRow, minRow *Event) {
	nullRow = CreateEvent(wide, map[string]any{"title": "b-null"})
	zeroRow = CreateEvent(wide, map[string]any{
		"title": "", "note": "", "ref": zeroUUID, "token": zeroUUID,
		"rank": float64(0), "count": float64(0), "amount": float64(0),
		"score": float64(0), "level": float64(0), "qty": float64(0),
		"total": float64(0), "ratio": float64(0), "active": false,
		"born": "1970-01-01", "joined": "1970-01-01",
		"seen": "1970-01-01T00:00:00Z", "touched": "1970-01-01T00:00:00Z",
	})
	maxRow = CreateEvent(wide, map[string]any{
		"title": "标题-🍋-boundary-max", "note": "ünïcode ✓ 空白  两个空格",
		"ref": maxUUID, "token": maxUUID,
		"rank": float64(32767), "count": float64(2147483647),
		"amount": maxBoundBigint,
		"score": math.MaxFloat64, "level": float64(32767),
		"qty": float64(2147483647), "total": maxEAVInt,
		"ratio": float64(1048576.25), "active": true,
		"born": "9999-12-31", "joined": "9999-12-31",
		"seen": "9999-12-31T23:59:59Z", "touched": "9999-12-31T23:59:59Z",
	})
	minRow = CreateEvent(wide, map[string]any{
		"title": "b-min", "note": "b-min",
		"rank": float64(-32768), "count": float64(-2147483648),
		"amount": -maxBoundBigint,
		"score": -math.MaxFloat64, "level": float64(-32768),
		"qty": float64(-2147483648), "total": -maxEAVInt,
		"ratio": float64(-1048576.25), "active": false,
		// pre-epoch: negative epoch-ms must survive every tier
		"born": "1900-01-01", "joined": "1900-01-01",
		"seen": "1900-01-01T00:00:00Z", "touched": "1900-01-01T00:00:00Z",
	})
	return
}

// TestNullAndBoundaryRoundTripAcrossTiers proves that NULL, empty-string,
// zero, and extreme boundary values survive write → CDC export → parquet →
// federated merge-on-read intact across all three tiers. Null+max land
// cold-only, zero+min stay warm, and fresh null+max copies stay hot. The
// physical layer reuses Task 4's per-file value assertion; the logical layer
// adds float64-proof exact typed assertions the oracle cannot make.
func TestNullAndBoundaryRoundTripAcrossTiers(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]

	nullRow, zeroRow, maxRow, minRow := buildBoundaryEvents(wide)
	batch := []*Event{nullRow, zeroRow, maxRow, minRow}
	if err := env.ApplyEvents(ctx, batch...); err != nil {
		t.Fatalf("apply boundary creates: %v", err)
	}

	// Hot baseline: oracle parity + exact typed assertions.
	hot := env.AssertQueryMatches(ctx, Query{Schema: wide, PreferHot: true, Limit: 100})
	assertBoundaryRecords(t, "hot", hot, nullRow, zeroRow, maxRow, minRow)

	// Tier split: null+max cold-only, zero+min warm.
	if _, err := env.RunInit(ctx, wide); err != nil {
		t.Fatalf("run init: %v", err)
	}
	env.ExecSQL(ctx,
		"DELETE FROM change_log WHERE schema_id = $1 AND row_id = ANY($2)",
		wide.ID, rowIDs([]*Event{nullRow, maxRow}))
	if _, err := env.RunFlush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Hot tier: fresh copies of the null and max shapes stay unflushed.
	hotNull, _, hotMax, _ := buildBoundaryEvents(wide)
	if err := env.ApplyEvents(ctx, hotNull, hotMax); err != nil {
		t.Fatalf("apply hot boundary creates: %v", err)
	}

	// Physical layer: reuse Task 4's per-file value assertion (NULL columns
	// included), then the NULL-vs-zero distinction per boundary row.
	truth := buildWideTruth(ctx, t, env, wide.ID, append(batch, hotNull, hotMax))
	manifests, err := env.loadManifests(ctx)
	if err != nil {
		t.Fatalf("load manifests: %v", err)
	}
	m := manifests[wide.ID]
	if m == nil {
		t.Fatal("no manifest for e2e_wide")
	}
	for _, f := range m.Files {
		assertWideParquetValues(ctx, t, env, f.Path, f.Tier, wide.ID, truth)
	}

	// Logical layer: federated merge equals oracle; exact typed spot checks.
	fed := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 100})
	if fed == nil {
		return
	}
	assertBoundaryRecords(t, "federated", fed, nullRow, zeroRow, maxRow, minRow, hotNull, hotMax)
}

// assertBoundaryRecords asserts exact typed values that the oracle's float64
// normalization cannot distinguish (MaxInt64) and the null/zero/empty-string
// distinctions.
func assertBoundaryRecords(t *testing.T, label string, res *QueryResult, evs ...*Event) {
	t.Helper()
	recs := map[uuid.UUID]*model.PersistentRecord{}
	for _, r := range res.Records {
		recs[r.RowID] = r
	}
	for _, ev := range evs {
		rec := recs[ev.RowID]
		if rec == nil {
			t.Errorf("%s: missing boundary row %s (%s)", label, ev.RowID, ev.Attrs["title"])
			continue
		}
		switch ev.Attrs["title"] {
		case "b-null":
			// only text_01 set; every other bound column absent, no EAV rows
			if _, ok := rec.Int64Items["bigint_01"]; ok {
				t.Errorf("%s b-null: amount present, want absent", label)
			}
			if len(rec.OtherAttributes) != 0 {
				t.Errorf("%s b-null: %d EAV attrs, want 0", label, len(rec.OtherAttributes))
			}
		case "":
			// empty string is a value, not NULL
			v, ok := rec.TextItems["text_01"]
			if !ok || v != "" {
				t.Errorf("%s b-zero: title = %q (present=%t), want empty string", label, v, ok)
			}
		case "标题-🍋-boundary-max":
			if got := rec.Int64Items["bigint_01"]; got != maxBoundBigint {
				t.Errorf("%s b-max: amount = %d, want %d", label, got, maxBoundBigint)
			}
			if got := rec.UUIDItems["uuid_01"]; got.String() != maxUUID {
				t.Errorf("%s b-max: ref = %s, want %s", label, got, maxUUID)
			}
			assertEAVNumeric(t, label+" b-max", rec, 15, maxEAVInt) // total
			assertEAVText(t, label+" b-max", rec, 17, maxUUID)      // token
		case "b-min":
			if got := rec.Int64Items["bigint_01"]; got != -maxBoundBigint {
				t.Errorf("%s b-min: amount = %d, want %d", label, got, -maxBoundBigint)
			}
			assertEAVNumeric(t, label+" b-min", rec, 15, -maxEAVInt)
			// pre-epoch date: negative epoch-ms
			if got := rec.Int64Items["bigint_02"]; got != preEpochJoinedMS {
				t.Errorf("%s b-min: joined = %d, want %d", label, got, preEpochJoinedMS)
			}
		}
	}
}

func assertEAVNumeric(t *testing.T, label string, rec *model.PersistentRecord, attrID int16, want float64) {
	t.Helper()
	for _, a := range rec.OtherAttributes {
		if a.AttrID == attrID {
			if a.ValueNumeric == nil || *a.ValueNumeric != want {
				t.Errorf("%s: eav attr %d numeric = %v, want %v", label, attrID, a.ValueNumeric, want)
			}
			return
		}
	}
	t.Errorf("%s: eav attr %d missing", label, attrID)
}

func assertEAVText(t *testing.T, label string, rec *model.PersistentRecord, attrID int16, want string) {
	t.Helper()
	for _, a := range rec.OtherAttributes {
		if a.AttrID == attrID {
			if a.ValueText == nil || *a.ValueText != want {
				t.Errorf("%s: eav attr %d text = %v, want %q", label, attrID, a.ValueText, want)
			}
			return
		}
	}
	t.Errorf("%s: eav attr %d missing", label, attrID)
}
