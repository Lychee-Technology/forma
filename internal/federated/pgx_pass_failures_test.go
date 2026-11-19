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

// A listed object missing at validation is a publication inconsistency:
// non-degradable.
func TestPgxPassListedObjectMissingAtValidationIsAnInconsistency(t *testing.T) {
	f := newPgxFixture(t, tieRunRows())
	path := filepath.Join(f.dir, "base.parquet")
	require.NoError(t, os.Remove(path))
	src := &fakeParquetSource{paths: []string{path}, missing: []string{"base.parquet"}}
	e := f.engine(t, nil, withParquetSource(src))
	_, _, err := e.ExecuteDuckDBFederatedQuery(context.Background(), tieRunTables, pgxQuery(), 5, 0, nil, nil)
	require.ErrorIs(t, err, ErrPublicationInconsistent)
	require.False(t, degradableFederatedError(err))
	require.Equal(t, 1, src.missingCalls)
}

// A pass whose row handler fails is a failed pass: it emits no fed_query_*
// sample. The same pass with a handler that succeeds emits its seven.
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
