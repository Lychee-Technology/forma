package internal

import (
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

const dayMs = int64(86400000)

// pgNowMs reads the server clock the $now operand is defined by. The test
// never consults the Go clock: every seed is an offset from this value.
func (f *dslFixture) pgNowMs(t *testing.T) int64 {
	t.Helper()
	var ms int64
	require.NoError(t, f.pool.QueryRow(f.ctx, "SELECT floor(extract(epoch from now()) * 1000)::bigint").Scan(&ms))
	return ms
}

func isoImage(ms int64) string {
	return time.UnixMilli(ms - ms%1000).UTC().Format(time.RFC3339)
}

// TestConditionDSLServerTime seeds rows an hour either side of the server's
// now() and a day either side of its UTC date, then compares through $now on
// every storage form: epoch millis in entity_main and eav_data, and the
// iso8601 text image.
func TestConditionDSLServerTime(t *testing.T) {
	f := newDSLFixture(t, true)
	now := f.pgNowMs(t)
	today := now - now%dayMs
	at := func(ms, day int64) map[string]any {
		return map[string]any{"bigint_01": ms, "text_03": isoImage(ms), "bigint_02": day, "text_04": isoImage(day)}
	}
	past := f.seed(t, at(now-3600000, today-dayMs), eavInt("seen", now-3600000), eavInt("day", today-dayMs))
	future := f.seed(t, at(now+3600000, today+dayMs), eavInt("seen", now+3600000), eavInt("day", today+dayMs))
	current := f.seed(t, map[string]any{"bigint_02": today, "text_04": isoImage(today)}, eavInt("day", today))
	literal := f.seed(t, map[string]any{"text_01": "$now"})
	f.seed(t, map[string]any{}) // every attribute absent

	for _, attr := range []string{"due", "due_iso", "seen"} {
		t.Run(attr, func(t *testing.T) {
			require.Equal(t, sortedIDs(past), f.match(t, kv(attr, "lte:$now")))
			require.Equal(t, sortedIDs(past), f.match(t, kv(attr, "lt:$now-30m")))
			require.Equal(t, sortedIDs(future), f.match(t, kv(attr, "gt:$now")))
			require.Equal(t, sortedIDs(future), f.match(t, kv(attr, "gte:$now+30m")))
			require.Equal(t, sortedIDs(past, future), f.match(t, kv(attr, "gt:$now-2h")))
			require.Equal(t, sortedIDs(past, future), f.match(t, kv(attr, "lt:$now+2h")))
			require.Equal(t, sortedIDs(past, future), f.match(t, kv(attr, "not_equals:$now")), "an absent value never matches")
			require.Empty(t, f.match(t, kv(attr, "equals:$now")))
		})
	}

	for _, attr := range []string{"day", "day_main", "day_iso"} {
		t.Run(attr, func(t *testing.T) {
			if ms := f.pgNowMs(t); ms-ms%dayMs != today {
				t.Skip("the server's UTC date changed after seeding; the date fixture no longer brackets it")
			}
			require.Equal(t, sortedIDs(current), f.match(t, kv(attr, "equals:$now")), "a date compares the UTC calendar date")
			require.Equal(t, sortedIDs(past), f.match(t, kv(attr, "equals:$now-24h")))
			require.Equal(t, sortedIDs(future), f.match(t, kv(attr, "eq:$now+24h")))
			require.Equal(t, sortedIDs(past), f.match(t, kv(attr, "lt:$now")))
			require.Equal(t, sortedIDs(current, future), f.match(t, kv(attr, "gte:$now")))
			require.Equal(t, sortedIDs(past, future), f.match(t, kv(attr, "neq:$now")))
		})
	}

	t.Run("text keeps $now as a literal", func(t *testing.T) {
		require.Equal(t, sortedIDs(literal), f.match(t, kv("code", "equals:$now")))
	})

	t.Run("$now composes with the other operators", func(t *testing.T) {
		require.Equal(t, sortedIDs(past), f.match(t, &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{
			kv("due", "is_not_null:true"), kv("seen", "lte:$now"),
		}}))
	})
}
