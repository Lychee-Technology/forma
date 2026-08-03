package federated

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// The validator's cache used to be keyed by URI alone, on the premise that
// parquet objects are write-once. That premise is false for init: cdc's base
// keys are deterministic ({min}_{max}.parquet), so an init rerun overwrites
// the object in place and rewrites its manifest stamp under the SAME path. A
// warmed server would then serve the pre-rerun columns forever. These tests
// pin the fix: an entry is only a hit while the manifest stamp it was
// validated under still matches the current one.

const stampCachePath = "s3://b/7/base/0_9.parquet"

// stampWith returns an invariant-satisfying stamp carrying the given extra
// attribute columns — the part an init rerun can legitimately change.
func stampWith(extra ...string) map[string]string {
	cols := map[string]string{"row_id": "UUID", "changed_at": "BIGINT", "deleted_at": "BIGINT"}
	for _, name := range extra {
		cols[name] = "BIGINT"
	}
	return cols
}

// describeRowsFor renders a column map in the DESCRIBE row shape.
func describeRowsFor(cols map[string]string) [][2]string {
	out := make([][2]string, 0, len(cols))
	for name, typ := range cols {
		out = append(out, [2]string{name, typ})
	}
	return out
}

func stampCacheValidator(t *testing.T) *parquetSchemaValidator {
	t.Helper()
	return newParquetSchemaValidator()
}

// validateOne runs one concrete path through the validator and returns the
// column union it produced.
func validateOne(t *testing.T, v *parquetSchemaValidator, duck DuckDBQueryExecutor, stamp map[string]string) map[string]string {
	t.Helper()
	stamps := map[string]map[string]string{}
	if stamp != nil {
		stamps[stampCachePath] = stamp
	}
	union, complete, err := v.Validate(context.Background(), duck, []string{stampCachePath}, stamps)
	require.NoError(t, err)
	require.True(t, complete, "a single validated path must produce a complete union")
	return union
}

// TestValidatorCacheHitsWhileStampIsUnchanged is the regression pin for the
// cold-start win: re-keying the cache must not turn every query back into a
// probe. Same stamp, same path — no DuckDB traffic at all after the first
// acceptance.
func TestValidatorCacheHitsWhileStampIsUnchanged(t *testing.T) {
	v := stampCacheValidator(t)
	duck := &fakeDuckDBExecutor{describeCols: map[string][][2]string{}}

	stamp := stampWith("city")
	for i := 0; i < 3; i++ {
		union := validateOne(t, v, duck, stamp)
		require.Contains(t, union, "city")
	}
	require.Equal(t, 0, duck.describeCalls,
		"an unchanged stamp must keep sparing the path its footer probe (#256 cold-start win)")
}

// TestValidatorReValidatesWhenStampChanges is the init-rerun case. The object
// at this path was overwritten and its manifest entry re-stamped with an extra
// column; the cached entry belongs to the previous stamp and must not answer.
func TestValidatorReValidatesWhenStampChanges(t *testing.T) {
	v := stampCacheValidator(t)
	duck := &fakeDuckDBExecutor{describeCols: map[string][][2]string{}}

	before := validateOne(t, v, duck, stampWith("city"))
	require.NotContains(t, before, "score")

	after := validateOne(t, v, duck, stampWith("city", "score"))
	require.Contains(t, after, "score",
		"a rewritten stamp must invalidate the entry it replaced; the cache served pre-rerun columns")
	require.Equal(t, 0, duck.describeCalls, "the new stamp satisfies the invariant, so no probe is needed")
}

// TestValidatorReValidatesOnStampAppearingAndDisappearing covers both
// transitions across the nil boundary: a path first validated by probe (no
// stamp) that later gains one, and a stamped path whose entry is later
// unstamped. Neither may serve the other's cached columns.
func TestValidatorReValidatesOnStampAppearingAndDisappearing(t *testing.T) {
	probed := stampWith("probed_only")
	duck := &fakeDuckDBExecutor{describeCols: map[string][][2]string{
		stampCachePath: describeRowsFor(probed),
	}}

	t.Run("nil_then_stamp", func(t *testing.T) {
		v := stampCacheValidator(t)
		duck.describeCalls = 0

		first := validateOne(t, v, duck, nil)
		require.Contains(t, first, "probed_only")
		require.Equal(t, 1, duck.describeCalls, "an unstamped path probes")

		second := validateOne(t, v, duck, stampWith("stamped_only"))
		require.Contains(t, second, "stamped_only",
			"a stamp appearing on a probe-validated path must re-validate")
		require.NotContains(t, second, "probed_only")
	})

	t.Run("stamp_then_nil", func(t *testing.T) {
		v := stampCacheValidator(t)
		duck.describeCalls = 0

		first := validateOne(t, v, duck, stampWith("stamped_only"))
		require.Contains(t, first, "stamped_only")
		require.Equal(t, 0, duck.describeCalls)

		second := validateOne(t, v, duck, nil)
		require.Contains(t, second, "probed_only",
			"an entry losing its stamp must fall back to the footer, not to the stamp's columns")
		require.Equal(t, 1, duck.describeCalls)
	})
}

// TestValidatorCrossCheckPrefersFooterAndWarns is the opportunistic
// cross-check. A probe only runs on a stamped path when the stamp failed the
// invariant, so this is exactly the case where the manifest claimed something
// the bytes do not support. The footer wins the union, the divergence is
// reported, and the cache is not poisoned with the stamp's columns.
func TestValidatorCrossCheckPrefersFooterAndWarns(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	v := newParquetSchemaValidator()
	v.logger = zap.New(core)

	footer := stampWith("real_column")
	duck := &fakeDuckDBExecutor{describeCols: map[string][][2]string{
		stampCachePath: describeRowsFor(footer),
	}}

	// row_id missing: the stamp cannot satisfy parquetcheck, so the path falls
	// through to the probe.
	bad := map[string]string{"changed_at": "BIGINT", "deleted_at": "BIGINT", "phantom_column": "BIGINT"}
	union := validateOne(t, v, duck, bad)

	require.Contains(t, union, "real_column", "the footer's columns must reach the union")
	require.NotContains(t, union, "phantom_column",
		"a column only the stamp claims must never enter the union: #255 would augment a NULL alias over it")
	require.Equal(t, 1, duck.describeCalls)

	entries := logs.FilterMessageSnippet("stamp").All()
	require.Len(t, entries, 1, "the divergence must be reported once")
	fields := entries[0].ContextMap()
	require.Equal(t, stampCachePath, fields["path"])
	require.EqualValues(t, len(bad), fields["stamp_columns"])
	require.EqualValues(t, len(footer), fields["footer_columns"])
	require.NotContains(t, fields, "stamp", "full column maps belong at Debug, not Warn")

	// The rejected stamp is recorded with the probed columns, so the next query
	// carrying the same stamp is a hit rather than a probe on every request.
	again := validateOne(t, v, duck, bad)
	require.Contains(t, again, "real_column")
	require.Equal(t, 1, duck.describeCalls,
		"a rejected stamp must not cost a probe on every single query")
	require.Len(t, logs.FilterMessageSnippet("stamp").All(), 1,
		"the warning follows the probe, so a cache hit must not re-emit it")
}

// TestValidatorCrossCheckSilentWhenStampMatchesFooter keeps the warning
// meaningful: an agreeing stamp is not news.
func TestValidatorCrossCheckSilentWhenStampMatchesFooter(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	v := newParquetSchemaValidator()
	v.logger = zap.New(core)

	duck := &fakeDuckDBExecutor{describeCols: map[string][][2]string{
		stampCachePath: describeRowsFor(stampWith("city")),
	}}
	validateOne(t, v, duck, nil)

	require.Zero(t, logs.Len(), "an unstamped path has nothing to cross-check")
}
