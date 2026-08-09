//go:build e2e

package production

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal"
	"github.com/lychee-technology/forma/internal/model"
)

// TestBatchAttrValueLookupBindsEAVNumericExactly is the real-Postgres half of
// the #355 verification. It calls the batch anchor directly rather than through
// relation enrichment for a reason recorded in the design: fetchParents derives
// its operands from stored child records, which have already been through the
// write path's float64 hop, so no non-float64-exact operand can ever reach the
// anchor that way and the contract difference is invisible end to end.
//
// The contract mirrors TestEAVBigintFilterBoundaryBothDialects: the write path
// rounded 2^53+1 down to 2^53 (#205 ceiling), so an exact operand of 2^53+1
// must NOT find the row by colliding with that same rounding error, while the
// value actually stored must stay addressable. The mixed integral/fractional
// probe additionally proves what pgxmock structurally cannot: that pgx encodes
// a per-element-typed []any into numeric[].
func TestBatchAttrValueLookupBindsEAVNumericExactly(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]

	// total (attr 15) is EAV-only bigint; ratio (attr 16) is EAV-only numeric.
	aboveCeiling := CreateEvent(wide, map[string]any{
		"title": "batch-2p53p1", "total": int64(1)<<53 + 1, "ratio": 2.5,
	})
	small := CreateEvent(wide, map[string]any{
		"title": "batch-small", "total": int64(7), "ratio": float64(8),
	})
	mustApplyEvents(ctx, t, env, "batch anchor seed", aboveCeiling, small)

	repo := internal.NewDBPersistentRecordRepository(env.Pool, env.Metadata)

	lookup := func(t *testing.T, attr string, values []string) []*model.PersistentRecord {
		t.Helper()
		page, err := repo.QueryPersistentRecordsByAttrValues(
			ctx, env.Tables, wide.ID, attr, values, len(values))
		if err != nil {
			t.Fatalf("batch lookup on %s%v: %v", attr, values, err)
		}
		return page.Records
	}

	t.Run("exact_operand_above_ceiling_matches_nothing", func(t *testing.T) {
		// 9007199254740993 was never stored — the write rounded it to 2^53.
		// Pre-fix, ParseFloat rounded the operand the same way and "found" the
		// row anyway. No groups means: the result must be empty.
		assertRowIDSet(t, "total=2^53+1",
			lookup(t, "total", []string{"9007199254740993"}))
	})

	t.Run("stored_rounded_value_stays_addressable", func(t *testing.T) {
		assertRowIDSet(t, "total=2^53",
			lookup(t, "total", []string{"9007199254740992"}),
			[]uuid.UUID{aboveCeiling.RowID})
	})

	t.Run("exponent_spelling_addresses_the_same_row", func(t *testing.T) {
		assertRowIDSet(t, "total=9.007199254740992e15",
			lookup(t, "total", []string{"9.007199254740992e15"}),
			[]uuid.UUID{aboveCeiling.RowID})
	})

	t.Run("multi_operand_integral_set", func(t *testing.T) {
		assertRowIDSet(t, "total IN (2^53, 7)",
			lookup(t, "total", []string{"9007199254740992", "7"}),
			[]uuid.UUID{aboveCeiling.RowID, small.RowID})
	})

	t.Run("mixed_integral_and_fractional_operands_encode", func(t *testing.T) {
		// The []any carries an int64 and a float64 in one array. If pgx cannot
		// encode that into numeric[], this is where it fails — loudly, with an
		// encode error rather than a wrong row set.
		assertRowIDSet(t, "ratio IN (2.5, 8)",
			lookup(t, "ratio", []string{"2.5", "8"}),
			[]uuid.UUID{aboveCeiling.RowID, small.RowID})
	})
}
