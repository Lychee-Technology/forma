package federated

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// fakeDescribeRows iterates canned (column_name, column_type) pairs in the
// 6-column DESCRIBE shape the schema validator scans.
type fakeDescribeRows struct {
	cols [][2]string
	idx  int
	err  error
}

func (r *fakeDescribeRows) Next() bool {
	if r.idx >= len(r.cols) {
		return false
	}
	r.idx++
	return true
}

func (r *fakeDescribeRows) Scan(dest ...any) error {
	name, ok := dest[0].(*string)
	if !ok {
		return fmt.Errorf("describe scan dest[0] is %T, want *string", dest[0])
	}
	typ, ok := dest[1].(*string)
	if !ok {
		return fmt.Errorf("describe scan dest[1] is %T, want *string", dest[1])
	}
	*name = r.cols[r.idx-1][0]
	*typ = r.cols[r.idx-1][1]
	return nil
}

func (r *fakeDescribeRows) Err() error   { return r.err }
func (r *fakeDescribeRows) Close() error { return nil }

// fakeStringRows iterates a single-column string result (glob expansion).
type fakeStringRows struct {
	vals []string
	idx  int
}

func (r *fakeStringRows) Next() bool {
	if r.idx >= len(r.vals) {
		return false
	}
	r.idx++
	return true
}

func (r *fakeStringRows) Scan(dest ...any) error {
	v, ok := dest[0].(*string)
	if !ok {
		return fmt.Errorf("glob scan dest[0] is %T, want *string", dest[0])
	}
	*v = r.vals[r.idx-1]
	return nil
}

func (r *fakeStringRows) Err() error   { return nil }
func (r *fakeStringRows) Close() error { return nil }

// scriptedDescribeExecutor answers validator probes per path: cols maps a
// path substring to its DESCRIBE rows, globs maps a pattern substring to its
// expanded file list, failPaths force a probe error (unreadable footer /
// unlistable glob). Every probe is recorded so caching is assertable.
type scriptedDescribeExecutor struct {
	cols      map[string][][2]string
	globs     map[string][]string
	failPaths map[string]bool
	probes    []string
}

func (s *scriptedDescribeExecutor) Query(ctx context.Context, sql string, args ...any) (duckDBRowsIterator, error) {
	s.probes = append(s.probes, sql)
	for path, fail := range s.failPaths {
		if fail && strings.Contains(sql, path) {
			return nil, fmt.Errorf("forced probe failure for %s", path)
		}
	}
	if strings.HasPrefix(sql, "SELECT file FROM glob(") {
		for pattern, files := range s.globs {
			if strings.Contains(sql, pattern) {
				return &fakeStringRows{vals: files}, nil
			}
		}
		return nil, fmt.Errorf("unexpected glob probe: %s", sql)
	}
	for path, cols := range s.cols {
		if strings.Contains(sql, path) {
			return &fakeDescribeRows{cols: cols}, nil
		}
	}
	return nil, fmt.Errorf("unexpected probe: %s", sql)
}

func buildValidSystemCols() [][2]string {
	return [][2]string{{"row_id", "UUID"}, {"changed_at", "BIGINT"}, {"deleted_at", "BIGINT"}, {"title", "VARCHAR"}}
}

func TestParquetSchemaValidator_ValidPathsPassAndCache(t *testing.T) {
	duck := &scriptedDescribeExecutor{cols: map[string][][2]string{
		"a.parquet": buildValidSystemCols(),
		"b.parquet": buildValidSystemCols(),
	}}
	v := newParquetSchemaValidator()
	paths := []string{"s3://b/1/a.parquet", "s3://b/1/b.parquet"}

	_, _, err := v.Validate(context.Background(), duck, paths, nil)
	require.NoError(t, err)
	require.Len(t, duck.probes, 2)

	// Nothing changed — same paths, still no stamps — so the cached entries
	// answer and no path is re-probed. (An entry is only re-validated when the
	// manifest stamp keying it moves; see
	// parquet_schema_validation_stamps_test.go.)
	_, _, err = v.Validate(context.Background(), duck, paths, nil)
	require.NoError(t, err)
	require.Len(t, duck.probes, 2)
}

func TestParquetSchemaValidator_MissingSystemColumnFailsClassified(t *testing.T) {
	duck := &scriptedDescribeExecutor{cols: map[string][][2]string{
		"wrong.parquet": {{"wrong_col", "INTEGER"}, {"other_col", "VARCHAR"}},
	}}
	v := newParquetSchemaValidator()

	_, _, err := v.Validate(context.Background(), duck, []string{"s3://b/1/wrong.parquet"}, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrFederatedReadFailed)
	require.NotErrorIs(t, err, ErrParquetSetInconsistent)
	require.Contains(t, err.Error(), "wrong.parquet")
	require.Contains(t, err.Error(), "row_id")
}

func TestParquetSchemaValidator_WrongSystemColumnTypeFailsClassified(t *testing.T) {
	duck := &scriptedDescribeExecutor{cols: map[string][][2]string{
		"poisoned.parquet": {{"row_id", "VARCHAR"}, {"changed_at", "BIGINT"}, {"deleted_at", "BIGINT"}},
	}}
	v := newParquetSchemaValidator()

	_, _, err := v.Validate(context.Background(), duck, []string{"s3://b/1/poisoned.parquet"}, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrFederatedReadFailed)
	require.Contains(t, err.Error(), `"row_id"`)
	require.Contains(t, err.Error(), "VARCHAR")
	require.Contains(t, err.Error(), "UUID")
}

func TestParquetSchemaValidator_UnreadableFooterIsInconclusive(t *testing.T) {
	// Byte corruption / truncation / storage outage: the probe cannot prove a
	// schema violation, so the main read keeps today's execution-path
	// classification (#187 CorruptBytes/Truncated).
	duck := &scriptedDescribeExecutor{failPaths: map[string]bool{"corrupt.parquet": true}}
	v := newParquetSchemaValidator()

	_, _, err := v.Validate(context.Background(), duck, []string{"s3://b/1/corrupt.parquet"}, nil)
	require.NoError(t, err)
	require.Len(t, duck.probes, 1)

	// Inconclusive results are not cached: the next query re-probes.
	_, _, err = v.Validate(context.Background(), duck, []string{"s3://b/1/corrupt.parquet"}, nil)
	require.NoError(t, err)
	require.Len(t, duck.probes, 2)
}

func TestParquetSchemaValidator_GlobExpandsAndValidatesMatches(t *testing.T) {
	// A glob (explicit hint or manifest fallback) must not bypass the
	// invariant: matches are enumerated and validated like listed paths
	// (#189 review P1 — a malformed file under a hinted glob would
	// otherwise vanish silently under union_by_name).
	duck := &scriptedDescribeExecutor{
		globs: map[string][]string{"1/*.parquet": {"s3://b/1/good.parquet", "s3://b/1/wrong.parquet"}},
		cols: map[string][][2]string{
			"good.parquet":  buildValidSystemCols(),
			"wrong.parquet": {{"wrong_col", "INTEGER"}},
		},
	}
	v := newParquetSchemaValidator()

	_, _, err := v.Validate(context.Background(), duck, []string{"s3://b/1/*.parquet"}, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrFederatedReadFailed)
	require.Contains(t, err.Error(), "wrong.parquet")
}

func TestParquetSchemaValidator_UnlistableGlobIsInconclusive(t *testing.T) {
	duck := &scriptedDescribeExecutor{failPaths: map[string]bool{"1/*.parquet": true}}
	v := newParquetSchemaValidator()

	union, complete, err := v.Validate(context.Background(), duck, []string{"s3://b/1/*.parquet"}, nil)
	require.NoError(t, err)
	require.Len(t, duck.probes, 1, "the glob listing attempt is the only probe")
	require.False(t, complete,
		"a failed listing leaves the footer union unknown: it must not be reported complete, "+
			"or #255 would augment columns the unlisted files may actually carry")
	require.Empty(t, union, "nothing was probed, so nothing contributes to the union")
}

func TestParquetSchemaValidator_NilCollaboratorsAreNoops(t *testing.T) {
	duck := &scriptedDescribeExecutor{}
	var nilValidator *parquetSchemaValidator
	_, _, err := nilValidator.Validate(context.Background(), duck, []string{"s3://b/1/a.parquet"}, nil)
	require.NoError(t, err)
	_, _, err = newParquetSchemaValidator().Validate(context.Background(), nil, []string{"s3://b/1/a.parquet"}, nil)
	require.NoError(t, err)
	require.Empty(t, duck.probes)
}

func TestParquetSchemaValidator_ViolationSurfacesThroughEngineQuery(t *testing.T) {
	// Engine-seam proof: a listed object violating the invariant fails the
	// federated query before the main scan, classified ErrFederatedReadFailed
	// (the #187 wrong-schema contract under the union_by_name read).
	restore := initTestDescriptors()
	defer restore()

	src := &fakeParquetSource{paths: []string{"s3://b/1/wrong.parquet"}}
	duck := &describeOverridingExecutor{
		inner: &fakeDuckDBExecutor{rows: &singleDuckDBRow{}},
		cols:  [][2]string{{"wrong_col", "INTEGER"}},
	}
	engine := newParquetSourceTestEngine(t, duck, src)

	_, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		coldTierQuery(), nil)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrFederatedReadFailed)
	require.NotErrorIs(t, err, ErrParquetSetInconsistent)
	require.Equal(t, 0, duck.inner.calls, "the main scan must not run over a schema-violating object")
}

func TestBenchmarkSchemaIDsSkipSchemaValidation(t *testing.T) {
	// Benchmark schemas (100-102) carry the legacy CSV-sniffed harness shape
	// (row_id VARCHAR) that the production invariant would reject; they are
	// exempt from pre-read validation, mirroring the projection special-case.
	restore := initTestDescriptors()
	defer restore()

	src := &fakeParquetSource{paths: []string{"s3://b/100/a.parquet"}}
	duck := &describeOverridingExecutor{
		inner: &fakeDuckDBExecutor{rows: &singleDuckDBRow{}},
		cols:  [][2]string{{"row_id", "VARCHAR"}}, // would violate the invariant
	}
	engine := newParquetSourceTestEngine(t, duck, src)

	q := coldTierQuery()
	q.SchemaID = 100
	_, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		q, nil)

	require.NoError(t, err, "benchmark schema reads must not be schema-validated")
	require.Equal(t, 1, duck.inner.calls, "the main scan must run")
}

func TestBreakerOpenRejectsBeforeSchemaProbes(t *testing.T) {
	// #185 reject-before-DuckDB extends to the #189 pre-read probes: an open
	// breaker must short-circuit before path resolution and any DESCRIBE
	// reaches storage (#189 review P2).
	restore := initTestDescriptors()
	defer restore()

	breaker := NewCircuitBreaker(1, time.Minute, time.Minute)
	breaker.RecordFailure()
	require.True(t, breaker.IsOpen(), "precondition: breaker open")

	src := &fakeParquetSource{paths: []string{"s3://b/1/a.parquet"}}
	duck := &fakeDuckDBExecutor{rows: &singleDuckDBRow{}}
	engine := NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, &fakeDirtyIDFetcher{}, duck, breaker,
		hybridDuckConfig(), testMetadataCacheSchema7(t), "host=x", WithParquetSource(src))

	_, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		coldTierQuery(), nil)

	require.ErrorContains(t, err, "circuit breaker open")
	require.Equal(t, 0, duck.describeCalls, "open breaker must reject before schema probes reach storage")
	require.Equal(t, 0, duck.calls, "open breaker must reject before the main scan")
	require.Equal(t, 0, src.pathsCalls, "open breaker must reject before path resolution")
}

// #255: Validate reports the union of footer columns and whether it is
// complete. The union drives NULL augmentation for never-flushed columns,
// so an incomplete union must be reported as such — augmenting a column
// that exists in an unprobed file would collide with the real column.
func TestValidateReturnsCompleteColumnUnion(t *testing.T) {
	exec := &scriptedDescribeExecutor{cols: map[string][][2]string{
		"base.parquet":  append(buildValidSystemCols(), [2]string{"name", "VARCHAR"}),
		"delta.parquet": append(buildValidSystemCols(), [2]string{"name", "VARCHAR"}, [2]string{"score", "INTEGER"}),
	}}
	v := newParquetSchemaValidator()
	union, complete, err := v.Validate(context.Background(), exec,
		[]string{"s3://b/base.parquet", "s3://b/delta.parquet"}, nil)
	require.NoError(t, err)
	require.True(t, complete)
	require.Contains(t, union, "name")
	require.Contains(t, union, "score")
	require.Contains(t, union, "row_id")
}

func TestValidateUnionIncompleteOnProbeFailure(t *testing.T) {
	exec := &scriptedDescribeExecutor{
		cols:      map[string][][2]string{"good.parquet": buildValidSystemCols()},
		failPaths: map[string]bool{"bad.parquet": true},
	}
	v := newParquetSchemaValidator()
	union, complete, err := v.Validate(context.Background(), exec,
		[]string{"s3://b/good.parquet", "s3://b/bad.parquet"}, nil)
	require.NoError(t, err, "unreadable footer stays inconclusive, not an error")
	require.False(t, complete)
	require.Contains(t, union, "row_id", "probed files still contribute")
}

// A cache hit must contribute its stored columns without a second probe.
func TestValidateCachedPathContributesColumnsWithoutReprobe(t *testing.T) {
	exec := &scriptedDescribeExecutor{cols: map[string][][2]string{
		"a.parquet": append(buildValidSystemCols(), [2]string{"score", "INTEGER"}),
	}}
	v := newParquetSchemaValidator()
	_, _, err := v.Validate(context.Background(), exec, []string{"s3://b/a.parquet"}, nil)
	require.NoError(t, err)
	probesAfterFirst := len(exec.probes)

	union, complete, err := v.Validate(context.Background(), exec, []string{"s3://b/a.parquet"}, nil)
	require.NoError(t, err)
	require.True(t, complete)
	require.Contains(t, union, "score")
	require.Len(t, exec.probes, probesAfterFirst, "cached path must not re-probe")
}

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
	require.Contains(t, union, "score")
	require.Equal(t, "UUID", union["row_id"])

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
	require.Contains(t, union, "score", "the cache holds a clone, not the caller's map")
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
	require.Contains(t, union, "probed_only", "the union is built from the PROBED footer")
	require.NotContains(t, union, "stamp_only", "the rejected stamp must not contribute columns")
	require.Equal(t, "UUID", union["row_id"])
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
	require.Contains(t, union, "stamped_col", "the stamped path contributes its columns")
	require.Contains(t, union, "probed", "the probed path contributes its columns")
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
	require.Contains(t, union, "score")

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
	require.Contains(t, union, "stamped_col")
}

// describeOverridingExecutor overrides the DESCRIBE answer while delegating
// everything else to the shared fake (whose own DESCRIBE handling always
// reports a healthy schema).
type describeOverridingExecutor struct {
	inner *fakeDuckDBExecutor
	cols  [][2]string
}

func (d *describeOverridingExecutor) Query(ctx context.Context, sql string, args ...any) (duckDBRowsIterator, error) {
	if strings.HasPrefix(sql, "DESCRIBE ") {
		return &fakeDescribeRows{cols: d.cols}, nil
	}
	return d.inner.Query(ctx, sql, args...)
}
