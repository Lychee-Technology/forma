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
// FINDING (#174): math.MaxInt64 is NOT faithfully representable end to end,
// even though bigint_01 is a true BIGINT at rest. The write path (forma
// EntityManager) marshals numeric payloads through float64 — exactly as any
// production JSON API does — so a bound bigint cannot carry every int64:
//
//   - +math.MaxInt64 (2^63-1) marshals to float64 2^63, which SATURATES back
//     to MaxInt64 on the way into the Postgres int8 column: a false positive
//     that hides the precision loss (the naive probe below would have passed).
//   - -math.MaxInt64 marshals to float64 -2^63 and lands as math.MinInt64, an
//     off-by-one that DOES surface; and a stored MaxInt64 even breaks the
//     federated DuckDB read (CAST of DOUBLE 2^63 → INT64 overflows).
//
// The largest power of two below 2^63 — 2^62 — is exactly representable in
// float64 and round-trips faithfully through every tier, so it is the value
// the boundary matrix asserts. This answers issue #174's "max int64" bullet:
// the ceiling for a bound bigint under float64 marshaling is 2^62, not 2^63-1.
//
// Both failure modes are not merely documented here — they are pinned by
// TestBoundBigintMaxInt64WriteCorrupts (the write-path off-by-one, deterministic
// on every platform) and TestFederatedReadRejectsStoredMaxInt64 (the federated
// DOUBLE→INT64 cast overflow). Both are #205 regression tripwires: when #205
// lifts the float64 hop they must fail, and their doc comments say how to flip
// them and raise maxBoundBigint to math.MaxInt64.
var maxBoundBigint = int64(1) << 62

// TestBoundBigintAcceptsInt64Probe pins the bigint contract described on
// maxBoundBigint: the write path ACCEPTS an int64 payload without error, and
// the chosen float64-exact boundary (2^62) round-trips through hot storage
// intact. math.MaxInt64 is deliberately not asserted here — see the finding
// above for why it is unrepresentable by design.
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
		t.Fatalf("bound bigint stored %d (present=%t), want %d (2^62)", got, ok, maxBoundBigint)
	}
}

// TestBoundBigintMaxInt64WriteCorrupts pins the float64 write-path hop (#205):
// the forma EntityManager marshals numeric payloads through float64, so a bound
// bigint payload of -math.MaxInt64 (-2^63+1) is silently corrupted on the way
// into Postgres. Unlike +math.MaxInt64 — whose out-of-range float64→int64
// conversion is implementation-dependent per the Go spec, so its stored value
// is NOT portable and must not be asserted — this case is fully defined on
// EVERY platform: float64(-math.MaxInt64) rounds to exactly -2^63, and
// int64(-2^63) IS in range, so the conversion is deterministic. The stored
// value is therefore always math.MinInt64: the caller wrote
// -9223372036854775807 but storage holds -9223372036854775808.
//
// When #205 fixes the write path (numeric payloads no longer routed through
// float64) this test MUST fail — got will equal -math.MaxInt64 — at which point
// flip the assertion to want == int64(-math.MaxInt64).
func TestBoundBigintMaxInt64WriteCorrupts(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]

	ev := CreateEvent(wide, map[string]any{
		"title":  "bigint-min-corruption",
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
	// #205: caller wrote -math.MaxInt64, storage holds math.MinInt64 (the
	// off-by-one the float64 write path introduces). Flip when #205 lands.
	if got != math.MinInt64 {
		t.Fatalf("bound bigint stored %d, want %d (math.MinInt64) — the -MaxInt64 write "+
			"corrupted via the float64 hop; if got == -MaxInt64, #205 is fixed", got, int64(math.MinInt64))
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
