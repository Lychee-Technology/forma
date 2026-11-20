package reconcile

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// TestCollectorKeepsAnotherDeploymentsObjectsForAnUnpublishedSchema is
// R6-F1 (#463): an UNPUBLISHED schema with counter 0 and its lifecycle grace
// elapsed has no generation history, so no manifest binds the bucket and
// prefix to this deployment. Pointed at another deployment's bucket that
// holds a live generation of the same schema id, every key matches the V2
// layout and is old, yet neither the diagnosis's plan nor the collection
// deletes any of them. Once the schema's own first generation is in the
// bucket, the bucket is bound and old staging under its prefix goes.
func TestCollectorKeepsAnotherDeploymentsObjectsForAnUnpublishedSchema(t *testing.T) {
	f := newFixture(t)
	rec := f.initialized(t)
	require.Equal(t, publication.Unpublished, rec.State)
	require.Zero(t, rec.Generation)
	old := f.serverMillis(t) - time.Hour.Milliseconds()
	foreign := []string{
		f.layout.ParquetKey(testSchema, 1, manifestv2.TierBase, uuid.Must(uuid.NewV7())),
		f.layout.ParquetKey(testSchema, 1, manifestv2.TierDelta, uuid.Must(uuid.NewV7())),
		f.layout.ManifestKey(testSchema, 1, 3, uuid.New()),
	}
	for _, key := range foreign {
		require.True(t, f.layout.Owns(testSchema, key), "%s is a key a V2 writer stages", key)
		f.store.set(key, []byte("another deployment's live object"), old)
	}
	before := f.store.keys()

	for _, gc := range []bool{true, false} { // the diagnosis plans against the horizon the collection raised
		rep := f.pass(t, testOptions(gc))
		require.NoError(t, rep.Err)
		require.Empty(t, rep.Findings, "gc=%v", gc)
		require.Contains(t, rep.Refused, "no manifest of the schema's generation history is in the bucket", "gc=%v", gc)
		require.Contains(t, rep.Refused, "--data-prefix", "gc=%v", gc)
		require.Empty(t, rep.Plan.Objects, "gc=%v", gc)
		require.Empty(t, rep.Plan.Manifests, "gc=%v", gc)
		if gc {
			require.Empty(t, rep.Collected.Objects)
			require.Empty(t, rep.Collected.Manifests)
		}
		require.Equal(t, before, f.store.keys(), "nothing is deleted, gc=%v", gc)
	}

	g1 := f.publishInit(t, f.record(t), 1)
	rep := f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Refused)
	require.ElementsMatch(t, foreign, rep.Collected.Objects, "the bucket holds the schema's own generation now")
	require.Equal(t, f.keysOf(g1), f.store.keys())
}

// truncatingStore lists LastModified truncated to the second, as S3 does.
type truncatingStore struct{ *memStore }

func (s truncatingStore) List(ctx context.Context, prefix string) ([]publication.Object, error) {
	listed, err := s.memStore.List(ctx, prefix)
	for i := range listed {
		listed[i].LastModified -= listed[i].LastModified % time.Second.Milliseconds()
	}
	return listed, err
}

// TestCollectorKeepsStagingItsListingRoundsBelowTheHorizon is R6-F2: with
// the horizon just below a publication's staged_at, which a compare-and-set
// still accepts, and a clock margin under a millisecond, S3's whole-second
// LastModified lists the staging below the horizon. The pass keeps it, and
// the publication commits.
func TestCollectorKeepsStagingItsListingRoundsBelowTheHorizon(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	g1 := f.publishInit(t, f.initialized(t), 1)
	f.waitIntoASecond(t) // the staging and its uploads fall in one second, well past its start
	inFlight := f.stageFlush(t, g1, 1)
	horizon := inFlight.next.StagedAt - 1
	_, err := f.db.Exec(fmt.Sprintf("UPDATE %s SET gc_horizon = $1 WHERE schema_id = $2", f.tables.Record), horizon, testSchema)
	require.NoError(t, err)

	opts := testOptions(true)
	opts.ClockMargin, opts.OrphanAge = time.Nanosecond, time.Hour
	r := f.reconciler(opts)
	r.Objects = truncatingStore{f.store}
	staging := f.stagedKeys(inFlight, g1)
	listed, err := r.Objects.List(ctx, f.layout.SchemaPrefix(testSchema))
	require.NoError(t, err)
	for _, o := range listed {
		if slices.Contains(staging, o.Key) {
			require.Less(t, o.LastModified, horizon, "the listing rounds %s below the horizon", o.Key)
		}
	}

	report, err := r.Run(ctx)
	require.NoError(t, err)
	rep := report.Schemas[0]
	require.NoError(t, rep.Err)
	require.Equal(t, horizon, rep.Horizon, "the pass collects against the horizon just below the staging")
	require.Empty(t, rep.Collected.Objects, "staging whose publication can still commit is kept")
	require.Equal(t, len(staging), rep.PendingOrphans)
	_, err = f.commit(t, inFlight)
	require.NoError(t, err, "its staged_at is above the horizon, so the publication commits")
}

// waitIntoASecond waits until the database clock is between 100 and 500 ms
// into a second.
func (f *fixture) waitIntoASecond(t *testing.T) {
	t.Helper()
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if ms := f.serverMillis(t) % 1000; ms >= 100 && ms < 500 {
			return
		}
	}
	t.Fatal("the database clock never reached 100 to 500 ms into a second")
}
