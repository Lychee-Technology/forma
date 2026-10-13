package internal

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIncrementStaysWithinItsValueType(t *testing.T) {
	cases := []struct {
		vt       forma.ValueType
		current  int64
		by       int64
		want     int64
		refusal  string
		describe string
	}{
		{forma.ValueTypeSmallInt, math.MaxInt16 - 1, 1, math.MaxInt16, "", "smallint up to its maximum"},
		{forma.ValueTypeSmallInt, math.MaxInt16, 1, 0, "past the smallint maximum 32767", "smallint past its maximum"},
		{forma.ValueTypeSmallInt, math.MinInt16 + 1, -1, math.MinInt16, "", "smallint down to its minimum"},
		{forma.ValueTypeSmallInt, math.MinInt16, -1, 0, "past the smallint minimum -32768", "smallint past its minimum"},
		{forma.ValueTypeSmallInt, 0, math.MaxInt64, 0, "past the smallint maximum", "smallint by a bigint step"},
		{forma.ValueTypeInteger, math.MaxInt32 - 5, 5, math.MaxInt32, "", "integer up to its maximum"},
		{forma.ValueTypeInteger, math.MaxInt32, 1, 0, "past the integer maximum 2147483647", "integer past its maximum"},
		{forma.ValueTypeInteger, math.MinInt32, -1, 0, "past the integer minimum -2147483648", "integer past its minimum"},
		{forma.ValueTypeBigInt, math.MaxInt64 - 1, 1, math.MaxInt64, "", "bigint up to its maximum"},
		{forma.ValueTypeBigInt, math.MaxInt64, 1, 0, "past the bigint maximum 9223372036854775807", "bigint past its maximum"},
		{forma.ValueTypeBigInt, math.MinInt64 + 1, -1, math.MinInt64, "", "bigint down to its minimum"},
		{forma.ValueTypeBigInt, math.MinInt64, -1, 0, "past the bigint minimum -9223372036854775808", "bigint past its minimum"},
		{forma.ValueTypeBigInt, -1, math.MinInt64, 0, "past the bigint minimum", "bigint by the most negative step"},
		{forma.ValueTypeBigInt, 1, math.MaxInt64, 0, "past the bigint maximum", "bigint by the largest step"},
		{forma.ValueTypeInteger, 7, -10, -3, "", "a negative step"},
		{forma.ValueTypeInteger, 7, 0, 7, "", "a zero step"},
		{forma.ValueTypeBigInt, math.MaxInt64, 0, math.MaxInt64, "", "a zero step at the maximum"},
	}
	for _, tc := range cases {
		t.Run(tc.describe, func(t *testing.T) {
			got, err := incrementWithin(tc.current, tc.by, "n", tc.vt)
			if tc.refusal == "" {
				require.NoError(t, err)
				assert.Equal(t, tc.want, got)
				return
			}
			require.ErrorIs(t, err, forma.ErrInvalidInput)
			assert.Contains(t, err.Error(), tc.refusal)
			assert.Contains(t, err.Error(), "attribute 'n'")
		})
	}
}

func TestServerTimeTakesTheAttributesPrecision(t *testing.T) {
	cache := condSchemaCache()
	at := func(s string) time.Time {
		v, err := time.Parse(time.RFC3339Nano, s)
		require.NoError(t, err)
		return v
	}
	// condClockMs is 2026-10-12T08:15:30.250Z.
	assert.Equal(t, at("2026-10-12T08:15:30.250Z"), serverTimeValue(condClockMs, 0, cache["due"]), "datetime keeps the millisecond")
	assert.Equal(t, at("2026-10-12T09:15:30.250Z"), serverTimeValue(condClockMs, time.Hour, cache["due_main"]), "unix_ms binding keeps the millisecond")
	assert.Equal(t, at("2026-10-12T08:15:30Z"), serverTimeValue(condClockMs, 0, cache["due_iso"]), "iso8601 binding floors to the second")
	assert.Equal(t, at("2026-10-12T00:00:00Z"), serverTimeValue(condClockMs, 0, cache["day"]), "date floors to the UTC day")
	assert.Equal(t, at("2026-10-11T00:00:00Z"), serverTimeValue(condClockMs, -9*time.Hour, cache["day"]), "the offset applies before the floor")
	assert.Equal(t, at("1969-12-31T00:00:00Z"), serverTimeValue(-1, 0, cache["day"]), "a pre-epoch instant floors down, not toward zero")
}

// TestUpdateAppliesOperatorsToTheLockedBase pins the merge order: the locked
// row, then Updates, then the operators computed from the row's own values
// and the transaction clock, over EAV and column-bound attributes alike.
func TestUpdateAppliesOperatorsToTheLockedBase(t *testing.T) {
	f := newCondFixture(t)
	rowID := f.seed(t, map[string]any{
		"name": "a", "counter": int64(10), "small": int64(-3), "big": int64(1 << 40),
		"version": int64(7), "rank": int64(2),
		"stats": map[string]any{"attempts": int64(4), "note": "kept"},
	})

	op := condOp(rowID)
	op.Updates = map[string]any{"name": "b", "stats": map[string]any{"note": "changed"}}
	op.UpdateOps = []forma.UpdateOp{
		forma.Increment("counter", 5),
		forma.Increment("small", -2),
		forma.Increment("big", 1),
		forma.Increment("version", 1),
		forma.Increment("rank", -2),
		forma.Increment("stats.attempts", 1),
		forma.ServerTime("due", 0),
		forma.ServerTime("due_main", 2*time.Hour),
		forma.ServerTime("due_iso", 0),
		forma.ServerTime("day", 0),
	}
	_, err := f.em.Update(f.ctx, op)
	require.NoError(t, err)

	got := f.stored(t, rowID)
	assert.Equal(t, "b", got["name"])
	assert.EqualValues(t, 15, got["counter"])
	assert.EqualValues(t, -5, got["small"])
	assert.EqualValues(t, int64(1<<40)+1, got["big"])
	assert.EqualValues(t, 8, got["version"], "column-bound bigint")
	assert.EqualValues(t, 0, got["rank"], "column-bound integer")
	stats, ok := got["stats"].(map[string]any)
	require.True(t, ok, "stats is an object: %#v", got["stats"])
	assert.EqualValues(t, 5, stats["attempts"])
	assert.Equal(t, "changed", stats["note"])

	assert.Equal(t, time.UnixMilli(condClockMs).UTC(), got["due"])
	assert.Equal(t, time.UnixMilli(condClockMs).Add(2*time.Hour).UTC(), got["due_main"])
	assert.Equal(t, time.UnixMilli(condClockMs).Truncate(time.Second).UTC(), got["due_iso"])
	assert.Equal(t, time.UnixMilli(condClockMs).Truncate(24*time.Hour).UTC(), got["day"])
	assert.Equal(t, 1, f.clockReads, "every server_time in one update shares one clock reading")
	assert.Equal(t, 1, f.repo.mergeCalls)
}

func TestServerTimeCreatesTheObjectItNestsIn(t *testing.T) {
	cache := forma.SchemaAttributeCache{"meta.seenAt": {AttributeID: 1, ValueType: forma.ValueTypeDateTime}}
	doc := map[string]any{"name": "a"}
	clock := func(context.Context) (int64, error) { return condClockMs, nil }
	require.NoError(t, applyUpdateOps(context.Background(), doc, []forma.UpdateOp{forma.ServerTime("meta.seenAt", 0)}, cache, clock))
	assert.Equal(t, map[string]any{"seenAt": time.UnixMilli(condClockMs).UTC()}, doc["meta"])
}

func TestServerTimeFailsWithoutAClock(t *testing.T) {
	cache := forma.SchemaAttributeCache{"at": {AttributeID: 1, ValueType: forma.ValueTypeDateTime}}
	err := applyUpdateOps(context.Background(), map[string]any{}, []forma.UpdateOp{forma.ServerTime("at", 0)}, cache, nil)
	require.Error(t, err)
	assert.False(t, errors.Is(err, forma.ErrInvalidInput), "a missing clock is a wiring fault, not the caller's")

	failing := func(context.Context) (int64, error) { return 0, errors.New("connection reset") }
	err = applyUpdateOps(context.Background(), map[string]any{}, []forma.UpdateOp{forma.ServerTime("at", 0)}, cache, failing)
	require.ErrorContains(t, err, "server_time on attribute 'at'")
	require.ErrorContains(t, err, "connection reset")
}

// TestUpdateOperatorRefusesARelationAttribute pins that an operator cannot
// write beneath an x-relation root, which is derived on read (#318).
func TestUpdateOperatorRefusesARelationAttribute(t *testing.T) {
	h := newShippedSchemaHarness(t)
	op := &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "visit", RowID: uuid.New()},
		UpdateOps:        []forma.UpdateOp{forma.ServerTime("contactSnapshot.birthday", 0)},
	}
	_, err := h.manager.Update(context.Background(), op)
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	message, published := forma.ResolvePublicMessage(err)
	require.True(t, published)
	assert.Contains(t, message, "attribute 'contactSnapshot.birthday' belongs to relation 'contactSnapshot'")
	assert.Zero(t, h.repo.mergeCalls)
}
