package federated

import (
	"context"
	"maps"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// The validator's cache used to be keyed by URI alone, on the premise that
// parquet objects are write-once. Forma's writers honour that premise (flush
// and compaction always did; init mints write-once keys since #416), but a
// rewrite under a listed path is still reachable — a pre-#416 init overwrote
// its deterministic {min}_{max}.parquet key in place, and an operator repair
// can republish bytes under an existing key — and each re-stamps the entry
// under the SAME path. A warmed server would then serve the pre-rewrite
// columns forever. These tests pin the fix: an entry is only a hit while the
// manifest stamp it was validated under still matches the current one.

const stampCachePath = "s3://b/7/base/0_9.parquet"

// stampWith returns an invariant-satisfying stamp carrying the given extra
// attribute columns — the part an init rerun can legitimately change.
func stampWith(extra ...string) map[string]string {
	cols := map[string]string{"row_id": "UUID", "changed_at": "BIGINT", "deleted_at": "BIGINT", "ltbase_created_at": "BIGINT"}
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
	return union.types
}

// TestValidatorCacheHitsWhileStampIsUnchanged is the regression pin for the
// cold-start win: re-keying the cache must not turn every query back into a
// probe. Same stamp, same path — no DuckDB traffic at all after the first
// acceptance.
func TestValidatorCacheHitsWhileStampIsUnchanged(t *testing.T) {
	v := newParquetSchemaValidator()
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
	v := newParquetSchemaValidator()
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
		v := newParquetSchemaValidator()
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
		v := newParquetSchemaValidator()
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

// ---------------------------------------------------------------------------
// Moved verbatim from parquet_schema_validation_test.go (500-line cap): the
// stamp-feeder cases belong with the stamp-keying cases they now share a
// subject with. No test logic changed in the move.
// ---------------------------------------------------------------------------

// stampCols renders the fixture DESCRIBE pairs as a manifest column stamp
// (name → DuckDB type), the shape FileEntry.Columns carries (#256).
func stampCols(pairs [][2]string) map[string]string {
	stamp := map[string]string{}
	for _, p := range pairs {
		stamp[p[0]] = p[1]
	}
	return stamp
}

// A stamped path that satisfies the invariant is validated with ZERO DuckDB
// probes; its columns feed the union and complete stays true (#256).
func TestValidateStampedPathSkipsProbe(t *testing.T) {
	// The executor has no canned answer for this path, so any probe would both
	// be counted and fail — a silent fallback cannot sneak past these asserts.
	exec := &scriptedDescribeExecutor{}
	v := newParquetSchemaValidator()
	stamps := map[string]map[string]string{
		"s3://b/1/a.parquet": stampCols(append(buildValidSystemCols(), [2]string{"score", "INTEGER"})),
	}

	union, complete, err := v.Validate(context.Background(), exec, []string{"s3://b/1/a.parquet"}, stamps)
	require.NoError(t, err)
	require.Empty(t, exec.probes, "a valid stamp must answer the invariant with zero DuckDB probes")
	require.True(t, complete, "a stamped path contributed its columns, so the union is complete")
	require.Contains(t, union.types, "score")
	require.Equal(t, "UUID", union.types["row_id"])

	// The stamp feeds the same cache a probe would, and the cache owns its
	// copy of BOTH halves of the entry: mutating the caller's map afterwards
	// must reach neither the cached columns nor the cached cache key.
	//
	// The re-validation passes a fresh map holding the ORIGINAL stamp content,
	// which is what makes this a proof rather than a coincidence. If the entry
	// had aliased the caller's map, the delete below would have stripped
	// "score" from the recorded stamp too, the stamp-keyed lookup would miss,
	// and the fallback probe — which this executor cannot answer — would fail
	// the assertions instead of quietly passing.
	original := maps.Clone(stamps["s3://b/1/a.parquet"])
	delete(stamps["s3://b/1/a.parquet"], "score")
	union, complete, err = v.Validate(context.Background(), exec,
		[]string{"s3://b/1/a.parquet"}, map[string]map[string]string{"s3://b/1/a.parquet": original})
	require.NoError(t, err)
	require.Empty(t, exec.probes, "an unchanged stamp keeps the path cached")
	require.True(t, complete)
	require.Contains(t, union.types, "score", "the cache holds a clone, not the caller's map")
}

// A stamp violating the invariant must NOT fail the query by itself: the probe
// runs, and a healthy footer wins. Stamps only short-circuit success.
func TestValidateBadStampFallsBackToProbe(t *testing.T) {
	exec := &scriptedDescribeExecutor{cols: map[string][][2]string{
		"a.parquet": append(buildValidSystemCols(), [2]string{"probed_only", "VARCHAR"}),
	}}
	v := newParquetSchemaValidator()
	stamps := map[string]map[string]string{ // stale/corrupt stamp: row_id missing
		"s3://b/1/a.parquet": {"changed_at": "BIGINT", "deleted_at": "BIGINT", "stamp_only": "VARCHAR"},
	}

	union, complete, err := v.Validate(context.Background(), exec, []string{"s3://b/1/a.parquet"}, stamps)
	require.NoError(t, err, "a corrupt manifest stamp must not fail a healthy object")
	require.Len(t, exec.probes, 1, "the rejected stamp falls through to exactly one footer probe")
	require.True(t, complete)
	require.Contains(t, union.types, "probed_only", "the union is built from the PROBED footer")
	require.NotContains(t, union.types, "stamp_only", "the rejected stamp must not contribute columns")
	require.Equal(t, "UUID", union.types["row_id"])
}

// Bad stamp + probe confirming the violation keeps today's loud failure.
func TestValidateBadStampProbeConfirmsViolation(t *testing.T) {
	exec := &scriptedDescribeExecutor{cols: map[string][][2]string{
		"wrong.parquet": {{"wrong_col", "INTEGER"}},
	}}
	v := newParquetSchemaValidator()
	stamps := map[string]map[string]string{"s3://b/1/wrong.parquet": {"wrong_col": "INTEGER"}}

	_, _, err := v.Validate(context.Background(), exec, []string{"s3://b/1/wrong.parquet"}, stamps)
	require.ErrorIs(t, err, ErrFederatedReadFailed)
	require.Contains(t, err.Error(), "wrong.parquet")
	require.Contains(t, err.Error(), "row_id")
	require.Len(t, exec.probes, 1, "byte truth authors the failure — the stamp only declined to skip it")
}

// Mixed set: stamped paths skip probing, unstamped paths probe as today.
func TestValidateMixedStampedAndUnstamped(t *testing.T) {
	exec := &scriptedDescribeExecutor{cols: map[string][][2]string{
		"unstamped.parquet": append(buildValidSystemCols(), [2]string{"probed", "VARCHAR"}),
	}}
	v := newParquetSchemaValidator()
	stamps := map[string]map[string]string{
		"s3://b/1/stamped.parquet": stampCols(append(buildValidSystemCols(), [2]string{"stamped_col", "INTEGER"})),
	}

	union, complete, err := v.Validate(context.Background(), exec,
		[]string{"s3://b/1/stamped.parquet", "s3://b/1/unstamped.parquet"}, stamps)
	require.NoError(t, err)
	require.Len(t, exec.probes, 1, "only the unstamped path reaches DuckDB")
	require.Contains(t, exec.probes[0], "unstamped.parquet")
	require.True(t, complete)
	require.Contains(t, union.types, "stamped_col", "the stamped path contributes its columns")
	require.Contains(t, union.types, "probed", "the probed path contributes its columns")
}

// nil stamps map preserves today's behavior byte-for-byte (regression pin for
// every pre-#256 call site).
func TestValidateNilStampsUnchanged(t *testing.T) {
	exec := &scriptedDescribeExecutor{cols: map[string][][2]string{
		"good.parquet": append(buildValidSystemCols(), [2]string{"score", "INTEGER"}),
	}}
	v := newParquetSchemaValidator()

	union, complete, err := v.Validate(context.Background(), exec, []string{"s3://b/1/good.parquet"}, nil)
	require.NoError(t, err)
	require.Len(t, exec.probes, 1, "without a stamp the footer probe still runs")
	require.True(t, complete)
	require.Contains(t, union.types, "score")

	_, _, err = v.Validate(context.Background(), exec, []string{"s3://b/1/good.parquet"}, nil)
	require.NoError(t, err)
	require.Len(t, exec.probes, 1, "the probe result is still cached")

	bad := &scriptedDescribeExecutor{cols: map[string][][2]string{"wrong.parquet": {{"wrong_col", "INTEGER"}}}}
	_, _, err = newParquetSchemaValidator().Validate(context.Background(), bad,
		[]string{"s3://b/1/wrong.parquet"}, nil)
	require.ErrorIs(t, err, ErrFederatedReadFailed, "violations stay loud when no stamp is supplied")
}

// The glob branch forwards stamps to its expanded matches: a stamped match
// skips its probe while an unstamped sibling is probed as today (#256).
func TestValidateGlobMatchesConsumeStamps(t *testing.T) {
	exec := &scriptedDescribeExecutor{
		globs: map[string][]string{"1/*.parquet": {"s3://b/1/stamped.parquet", "s3://b/1/plain.parquet"}},
		cols:  map[string][][2]string{"plain.parquet": buildValidSystemCols()},
	}
	v := newParquetSchemaValidator()
	stamps := map[string]map[string]string{
		"s3://b/1/stamped.parquet": stampCols(append(buildValidSystemCols(), [2]string{"stamped_col", "INTEGER"})),
	}

	union, complete, err := v.Validate(context.Background(), exec, []string{"s3://b/1/*.parquet"}, stamps)
	require.NoError(t, err)
	require.Len(t, exec.probes, 2, "the glob listing plus one DESCRIBE for the unstamped match only")
	require.Contains(t, exec.probes[1], "plain.parquet")
	require.True(t, complete)
	require.Contains(t, union.types, "stamped_col")
}
