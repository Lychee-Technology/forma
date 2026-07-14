package federated

import (
	"context"
	"fmt"
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

	require.NoError(t, v.Validate(context.Background(), duck, paths))
	require.Len(t, duck.probes, 2)

	// Parquet objects are write-once: validated paths are never re-probed.
	require.NoError(t, v.Validate(context.Background(), duck, paths))
	require.Len(t, duck.probes, 2)
}

func TestParquetSchemaValidator_MissingSystemColumnFailsClassified(t *testing.T) {
	duck := &scriptedDescribeExecutor{cols: map[string][][2]string{
		"wrong.parquet": {{"wrong_col", "INTEGER"}, {"other_col", "VARCHAR"}},
	}}
	v := newParquetSchemaValidator()

	err := v.Validate(context.Background(), duck, []string{"s3://b/1/wrong.parquet"})
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

	err := v.Validate(context.Background(), duck, []string{"s3://b/1/poisoned.parquet"})
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

	require.NoError(t, v.Validate(context.Background(), duck, []string{"s3://b/1/corrupt.parquet"}))
	require.Len(t, duck.probes, 1)

	// Inconclusive results are not cached: the next query re-probes.
	require.NoError(t, v.Validate(context.Background(), duck, []string{"s3://b/1/corrupt.parquet"}))
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

	err := v.Validate(context.Background(), duck, []string{"s3://b/1/*.parquet"})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrFederatedReadFailed)
	require.Contains(t, err.Error(), "wrong.parquet")
}

func TestParquetSchemaValidator_UnlistableGlobIsInconclusive(t *testing.T) {
	duck := &scriptedDescribeExecutor{failPaths: map[string]bool{"1/*.parquet": true}}
	v := newParquetSchemaValidator()

	require.NoError(t, v.Validate(context.Background(), duck, []string{"s3://b/1/*.parquet"}))
	require.Len(t, duck.probes, 1, "the glob listing attempt is the only probe")
}

func TestParquetSchemaValidator_NilCollaboratorsAreNoops(t *testing.T) {
	duck := &scriptedDescribeExecutor{}
	var nilValidator *parquetSchemaValidator
	require.NoError(t, nilValidator.Validate(context.Background(), duck, []string{"s3://b/1/a.parquet"}))
	require.NoError(t, newParquetSchemaValidator().Validate(context.Background(), nil, []string{"s3://b/1/a.parquet"}))
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
