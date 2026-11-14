package federated

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// How a failed pgx pass is classified and what it reports (#856 review
// R1-F3, R1-F4).

// deleteAfterListing is a DuckDB executor that removes path once a glob has
// been listed, before the next statement: a writer retiring an object between
// the frozen inventory's listing and the pre-read footer validation.
type deleteAfterListing struct {
	DuckDBQueryExecutor
	t       *testing.T
	path    string
	listed  bool
	deleted bool
}

func (d *deleteAfterListing) Query(ctx context.Context, sqlStr string, args ...any) (duckDBRowsIterator, error) {
	if d.listed && !d.deleted {
		d.deleted = true
		require.NoError(d.t, os.Remove(d.path))
	}
	if strings.Contains(sqlStr, "FROM glob(") {
		d.listed = true
	}
	return d.DuckDBQueryExecutor.Query(ctx, sqlStr, args...)
}

// An object a glob listed and that is gone before its footer is validated
// fails the pre-read check, and the failure keeps the frozen inventory's
// provenance: no manifest listed the object, so it is degradable and claims
// no manifest inconsistency.
func TestPgxPassGlobListedObjectDeletedBeforeValidationIsNotAnInconsistency(t *testing.T) {
	f := newPgxFixture(t, tieRunRows())
	duck := &deleteAfterListing{
		DuckDBQueryExecutor: NewDuckDBClientQueryExecutor(&DuckDBClient{DB: f.db}),
		t:                   t,
		path:                filepath.Join(f.dir, "base.parquet"),
	}
	src := &fakeParquetSource{paths: []string{filepath.Join(f.dir, "*.parquet")}, missing: []string{"base.parquet"}}
	e := f.engineOver(t, duck, nil, WithParquetSource(src))
	_, _, err := e.ExecuteDuckDBFederatedQuery(context.Background(), tieRunTables, pgxQuery(), 5, 0, nil, nil)
	require.True(t, duck.deleted, "the object went between the listing and the validation")
	var unverified *unverifiedParquetError
	require.True(t, errors.As(err, &unverified), "the pre-read validation failed: %v", err)
	require.ErrorIs(t, err, ErrFederatedReadFailed)
	require.NotErrorIs(t, err, ErrParquetSetInconsistent)
	require.True(t, degradableFederatedError(err))
	require.Zero(t, src.missingCalls, "objects a glob listed are not probed as manifest entries")
	require.Zero(t, f.leg.begins, "the pass fails before its snapshot")
}

// A manifest-listed object missing at validation is still a manifest
// inconsistency: non-degradable, as on the attach path.
func TestPgxPassManifestListedObjectMissingAtValidationIsAnInconsistency(t *testing.T) {
	f := newPgxFixture(t, tieRunRows())
	path := filepath.Join(f.dir, "base.parquet")
	require.NoError(t, os.Remove(path))
	src := &fakeParquetSource{paths: []string{path}, missing: []string{"base.parquet"}}
	e := f.engine(t, nil, WithParquetSource(src))
	_, _, err := e.ExecuteDuckDBFederatedQuery(context.Background(), tieRunTables, pgxQuery(), 5, 0, nil, nil)
	require.ErrorIs(t, err, ErrParquetSetInconsistent)
	require.False(t, degradableFederatedError(err))
	require.Equal(t, 1, src.missingCalls)
}

// A pass whose row handler fails is a failed pass: it emits no fed_query_*
// sample, as the attach path emits none for a failed stream. The same pass
// with a handler that succeeds emits its seven.
func TestPgxPassHandlerFailureEmitsNoMetrics(t *testing.T) {
	f := newPgxFixture(t, tieRunRows())
	rec := &metricRecorder{}
	e := f.engine(t, []string{f.dir + "/base.parquet"}, WithMetricEmitter(rec))
	handlerErr := errors.New("the caller stopped reading")
	calls := 0
	_, err := e.StreamDuckDBFederatedQuery(context.Background(), tieRunTables, pgxQuery(), 4, 0, nil, nil,
		func(context.Context, *model.PersistentRecord) error { calls++; return handlerErr })
	require.ErrorIs(t, err, handlerErr)
	require.Equal(t, 1, calls)
	require.Empty(t, fedQueryEvents(rec))

	_, err = e.StreamDuckDBFederatedQuery(context.Background(), tieRunTables, pgxQuery(), 4, 0, nil, nil,
		func(context.Context, *model.PersistentRecord) error { return nil })
	require.NoError(t, err)
	require.Len(t, fedQueryEvents(rec), 7)
}

// fedQueryEvents are the fed_query_* samples rec holds.
func fedQueryEvents(rec *metricRecorder) []string {
	var out []string
	for _, m := range rec.events {
		if strings.HasPrefix(m.Name, "fed_query_") {
			out = append(out, m.Name+"/"+labelString(m.Labels))
		}
	}
	return out
}
