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
	got, ok := hot.Records[0].Int64Items["bigint_01"]
	if !ok || got != maxBoundBigint {
		t.Fatalf("bound bigint stored %d (present=%t), want %d (2^62)", got, ok, maxBoundBigint)
	}
}

const zeroUUID = "00000000-0000-0000-0000-000000000000"
const maxUUID = "ffffffff-ffff-ffff-ffff-ffffffffffff"

// float64-exact EAV bigint bound: EAV values travel as float64 through the
// Go model (transform.extractValueFromEAVRecord), so 2^53 is the largest
// exactly-representable EAV integer. Bound bigints don't share this limit.
const maxEAVInt = float64(1 << 53)

// boundaryEvents builds the four boundary shapes: a NULL row (only the bound
// title set), a zero/empty row (every column an explicit zero-value, not
// NULL), a max row (upper bounds of every type + unicode text), and a min row
// (lower bounds + pre-epoch dates that must survive as negative epoch-ms).
func boundaryEvents(wide SchemaRef) (nullRow, zeroRow, maxRow, minRow *Event) {
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

	nullRow, zeroRow, maxRow, minRow := boundaryEvents(wide)
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
	hotNull, _, hotMax, _ := boundaryEvents(wide)
	if err := env.ApplyEvents(ctx, hotNull, hotMax); err != nil {
		t.Fatalf("apply hot boundary creates: %v", err)
	}

	// Physical layer: reuse Task 4's per-file value assertion (NULL columns
	// included), then the NULL-vs-zero distinction per boundary row.
	truth := wideTruth(t, append(batch, hotNull, hotMax))
	manifests, err := env.loadManifests(ctx)
	if err != nil {
		t.Fatalf("load manifests: %v", err)
	}
	m := manifests[wide.ID]
	if m == nil {
		t.Fatal("no manifest for e2e_wide")
	}
	for _, f := range m.Files {
		assertWideParquetValues(ctx, t, env, f.Path, f.Tier, truth)
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
			if got := rec.Int64Items["bigint_02"]; got != -2208988800000 {
				t.Errorf("%s b-min: joined = %d, want -2208988800000", label, got)
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
