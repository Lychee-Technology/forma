package federated

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// scriptedDescribeExecutor answers DESCRIBE probes per path: cols maps a path
// substring to its describe rows, failPaths force a probe error (unreadable
// footer). Every probe is recorded so caching behavior is assertable.
type scriptedDescribeExecutor struct {
	cols      map[string][][2]string
	failPaths map[string]bool
	probes    []string
}

func (s *scriptedDescribeExecutor) Query(ctx context.Context, sql string, args ...any) (duckDBRowsIterator, error) {
	s.probes = append(s.probes, sql)
	for path, fail := range s.failPaths {
		if fail && strings.Contains(sql, path) {
			return nil, fmt.Errorf("forced footer read failure for %s", path)
		}
	}
	for path, cols := range s.cols {
		if strings.Contains(sql, path) {
			return &fakeDescribeRows{cols: cols}, nil
		}
	}
	return nil, fmt.Errorf("unexpected probe: %s", sql)
}

func validSystemCols() [][2]string {
	return [][2]string{{"row_id", "UUID"}, {"changed_at", "BIGINT"}, {"deleted_at", "BIGINT"}, {"title", "VARCHAR"}}
}

func TestParquetSchemaValidator_ValidPathsPassAndCache(t *testing.T) {
	duck := &scriptedDescribeExecutor{cols: map[string][][2]string{
		"a.parquet": validSystemCols(),
		"b.parquet": validSystemCols(),
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

func TestParquetSchemaValidator_SkipsGlobsAndNilCollaborators(t *testing.T) {
	duck := &scriptedDescribeExecutor{}
	v := newParquetSchemaValidator()

	require.NoError(t, v.Validate(context.Background(), duck, []string{"s3://b/1/*.parquet"}))
	require.Empty(t, duck.probes, "glob paths cannot be probed per-file")

	var nilValidator *parquetSchemaValidator
	require.NoError(t, nilValidator.Validate(context.Background(), duck, []string{"s3://b/1/a.parquet"}))
	require.NoError(t, v.Validate(context.Background(), nil, []string{"s3://b/1/a.parquet"}))
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
