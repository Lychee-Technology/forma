package internal

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// Real-PostgreSQL tests of unique keys over every scalar value type (#651):
// uktyped declares keys whose components are datetime, date, uuid, bool,
// text, bigint and numeric attributes, each type once bound to a main-table
// column and once stored in EAV. Equal typed values conflict however the
// write spelled them, and the key table matches the rows after every step.

// ukTypedData is a uktyped row whose every key value is unique to seed.
func ukTypedData(seed int) map[string]any {
	return map[string]any{
		"at":        fmt.Sprintf("2026-01-01T00:00:%02dZ", seed),
		"ref":       fmt.Sprintf("00000000-0000-4000-8000-%012d", seed),
		"day":       fmt.Sprintf("2026-02-%02d", seed+1),
		"refEav":    fmt.Sprintf("10000000-0000-4000-8000-%012d", seed),
		"flag":      true,
		"tag":       fmt.Sprintf("tag%d", seed),
		"flagEav":   false,
		"big":       int64(1)<<40 + int64(seed),
		"amount":    1.5 + float64(seed),
		"amountCol": 2.25 + float64(seed),
		"bigEav":    100 + seed,
	}
}

func TestUniqueKeysCompareTypedValuesInColumnsAndEAV(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	first := env.mustCreate(t, ukTyped, ukTypedData(1))
	env.assertKeyTableMatchesRows(t)
	require.Len(t, env.keyEntriesOfRow(t, first), 6, "the row holds one entry per key")

	// Each case gives a fresh row first's value of one key, spelled another
	// way where the type has another spelling; every other key stays unique.
	cases := []struct {
		key  string
		take map[string]any
	}{
		{"at_ref", map[string]any{
			"at":  "2026-01-01T01:00:01.0004+01:00", // the same instant, floored to the millisecond on write
			"ref": strings.ToUpper("00000000-0000-4000-8000-000000000001"),
		}},
		{"day_ref_eav", map[string]any{"day": "2026-02-02", "refEav": strings.ToUpper("10000000-0000-4000-8000-000000000001")}},
		{"flag_tag", map[string]any{"tag": "tag1"}},
		{"flag_eav_big", map[string]any{"big": float64(int64(1)<<40 + 1)}},
		{"amount", map[string]any{"amount": 2.5}},
		{"amount_col_big_eav", map[string]any{"amountCol": 3.25, "bigEav": 101.0}},
	}
	for i, tc := range cases {
		data := ukTypedData(10 + i)
		for attr, v := range tc.take {
			data[attr] = v
		}
		_, err := env.manager.Create(env.ctx, ukCreateOp(ukTyped, uuid.Nil, data))
		requireKeyConflict(t, err, ukTyped, tc.key, "tag1")

		// One component away from first's value is no conflict.
		data["flag"], data["flagEav"], data["ref"] = false, true, uuid.NewString()
		data["day"], data["amount"], data["bigEav"] = "2026-03-01", 99.5+float64(i), 900+i
		env.mustCreate(t, ukTyped, data)
	}
	env.assertKeyTableMatchesRows(t)

	// A row with a component absent is not subject to that key.
	partial := ukTypedData(1)
	delete(partial, "at")
	delete(partial, "refEav")
	delete(partial, "tag")
	delete(partial, "big")
	delete(partial, "amount")
	delete(partial, "bigEav")
	env.mustCreate(t, ukTyped, partial)
	env.assertKeyTableMatchesRows(t)

	// An update to a typed value another row holds conflicts, and takes it
	// once the holder releases it.
	second := env.mustCreate(t, ukTyped, ukTypedData(2))
	_, err := env.manager.Update(env.ctx, txUpdateOp(ukTyped, second, map[string]any{"at": "2026-01-01T00:00:01.000Z", "ref": "00000000-0000-4000-8000-000000000001"}))
	requireKeyConflict(t, err, ukTyped, "at_ref", "00000000")
	_, err = env.manager.Update(env.ctx, txUpdateOp(ukTyped, first, map[string]any{"at": "2027-01-01T00:00:00Z"}))
	require.NoError(t, err)
	_, err = env.manager.Update(env.ctx, txUpdateOp(ukTyped, second, map[string]any{"at": "2026-01-01T00:00:01.000Z", "ref": "00000000-0000-4000-8000-000000000001"}))
	require.NoError(t, err)
	env.assertKeyTableMatchesRows(t)

	require.NoError(t, env.manager.Delete(env.ctx, txDeleteOp(ukTyped, second)))
	_, err = env.manager.Create(env.ctx, ukCreateOp(ukTyped, uuid.Nil, map[string]any{"at": "2026-01-01T00:00:01Z", "ref": "00000000-0000-4000-8000-000000000001"}))
	require.NoError(t, err, "a deleted row's typed values can be taken again")
	env.assertKeyTableMatchesRows(t)
}
