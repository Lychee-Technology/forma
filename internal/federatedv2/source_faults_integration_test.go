package federatedv2

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/publication"
)

// Faults of what PostgreSQL committed and of what the store answers, told
// apart from failed reads.

// A layout prefix with a glob metacharacter passes manifest verification,
// and every key the manifest lists carries it. DuckDB would expand such a
// key and read whatever it matches: here an unlisted copy of the object,
// under the directory the pattern names, which a real DuckDB would answer
// the read from. The read fails closed instead, before any object is read
// or probed. (The engine's own test, in internal/federated, counts the
// DuckDB statements: none, the listing included.)
func TestAGenerationListingAnAddressDuckDBWouldExpandFailsClosed(t *testing.T) {
	f := newFixture(t)
	root := t.TempDir()
	f.layout = manifestv2.Layout{Prefix: filepath.Join(root, "da[t]a")}
	f.initialize(t, publication.Unpublished)
	a := map[row]int64{allRows["a"]: 100}
	f.write(t, allRows["a"], 100)
	listed := f.deltaFile(t, 1, a)
	g := f.flush(t, a, listed)

	// The pattern da[t]a matches the directory data, not da[t]a itself.
	unlisted := filepath.Join(root, "data", strings.TrimPrefix(listed.Key, f.layout.Prefix))
	require.NoError(t, os.MkdirAll(filepath.Dir(unlisted), 0o755))
	raw, err := os.ReadFile(listed.Key)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(unlisted, raw, 0o600))

	breaker := federated.NewCircuitBreaker(1, time.Minute, time.Hour)
	e := f.engine(t, f.source(t, 1), nil, breaker)

	_, err = e.Query(f.ctx, f.tables, query(), &model.FederatedQueryOptions{AllowPartialDegradedMode: true})
	require.ErrorIs(t, err, federated.ErrPublicationInconsistent)
	require.ErrorIs(t, err, federated.ErrPublishedPathNotConcrete)
	var typed *federated.PublicationConsistencyError
	require.ErrorAs(t, err, &typed)
	require.Equal(t, [3]int64{int64(schemaID), 1, g.Generation}, [3]int64{int64(typed.SchemaID), typed.Epoch, typed.Generation})
	require.Zero(t, f.probes)
	require.False(t, breaker.IsOpen())
}

// A record row the read returned and refused, here a PUBLISHED record that
// names no manifest, is a fault of the committed metadata: the read fails
// closed with the typed error naming the row's epoch and generation, and
// neither degrades nor reaches the breaker. A record the snapshot could not
// read at all, here because its table is gone, is a failed PostgreSQL read,
// which degrades as its V1 counterpart does.
func TestAMalformedRecordFailsClosedAndAFailedRecordReadDegrades(t *testing.T) {
	t.Run("malformed record", func(t *testing.T) {
		f := newFixture(t)
		f.initialize(t, publication.Unpublished)
		f.write(t, allRows["a"], 100)
		_, err := f.pool.Exec(f.ctx, "UPDATE "+f.pub.Record+" SET state = $1, generation = 4 WHERE schema_id = $2",
			string(publication.Published), schemaID)
		require.NoError(t, err)
		breaker := federated.NewCircuitBreaker(1, time.Minute, time.Hour)
		e := f.engine(t, f.source(t, 1), closedDuckDB(t), breaker)

		_, err = e.Query(f.ctx, f.tables, query(), &model.FederatedQueryOptions{AllowPartialDegradedMode: true})
		require.ErrorIs(t, err, federated.ErrPublicationInconsistent)
		require.ErrorIs(t, err, publication.ErrRecordInvalid)
		require.NotErrorIs(t, err, federated.ErrPostgresReadFailed)
		var typed *federated.PublicationConsistencyError
		require.ErrorAs(t, err, &typed)
		require.Equal(t, [3]int64{int64(schemaID), 1, 4}, [3]int64{int64(typed.SchemaID), typed.Epoch, typed.Generation})
		require.False(t, breaker.IsOpen())
		require.Zero(t, f.store.count(), "no manifest fetched")
	})
	t.Run("failed record read", func(t *testing.T) {
		f := newFixture(t)
		f.initialize(t, publication.Unpublished)
		f.write(t, allRows["a"], 100)
		_, err := f.pool.Exec(f.ctx, "DROP TABLE "+f.pub.Record)
		require.NoError(t, err)
		breaker := federated.NewCircuitBreaker(1, time.Minute, time.Hour)
		e := f.engine(t, f.source(t, 1), closedDuckDB(t), breaker)

		_, _, err = e.ExecuteDuckDBFederatedQuery(f.ctx, f.tables, query(), 10, 0, nil, nil)
		require.ErrorIs(t, err, federated.ErrPostgresReadFailed)
		require.NotErrorIs(t, err, federated.ErrPublicationInconsistent)

		opts := &model.FederatedQueryOptions{AllowPartialDegradedMode: true, IncludeExecutionPlan: true}
		page, err := e.Query(f.ctx, f.tables, query(), opts)
		require.NoError(t, err)
		require.Equal(t, "degraded fallback (AllowPartialDegradedMode)", page.ExecutionPlan.Routing.Reason)
		require.Equal(t, int64(1), page.TotalRecords)
		require.False(t, breaker.IsOpen())
	})
}

// A failed probe neither hides nor takes back a key another probe found
// missing, whichever comes first: the answer holds every confirmed loss,
// beside the failed probes.
func TestMissingInKeepsEveryConfirmedLossBesideAFailedProbe(t *testing.T) {
	gen := federated.PublishedGeneration{SchemaID: schemaID, Epoch: 1, Generation: 2,
		Paths: []string{"p/a", "p/b", "p/c"}, Keys: []string{"a", "b", "c"}}
	unavailable := errors.New("HeadObject answered 503")
	for name, answers := range map[string]map[string]error{
		"the failed probe after the lost key":  {"a": nil, "b": unavailable},
		"the failed probe before the lost key": {"a": unavailable, "b": nil},
	} {
		t.Run(name, func(t *testing.T) {
			var probed []string
			src := &Source{exists: func(_ context.Context, key string) (bool, error) {
				probed = append(probed, key)
				err, known := answers[key]
				if !known {
					return true, nil
				}
				return false, err
			}}
			missing, err := src.MissingIn(context.Background(), gen, gen.Paths)
			require.ErrorIs(t, err, unavailable)
			lost := "a"
			if answers["a"] != nil {
				lost = "b"
			}
			require.Equal(t, []string{lost}, missing)
			require.Equal(t, []string{"a", "b", "c"}, probed, "a failed probe stops none of the others")
		})
	}

	t.Run("an ended context stops the probes", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		src := &Source{exists: func(_ context.Context, key string) (bool, error) {
			cancel()
			return false, nil
		}}
		missing, err := src.MissingIn(ctx, gen, gen.Paths)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, []string{"a"}, missing)
	})
}
