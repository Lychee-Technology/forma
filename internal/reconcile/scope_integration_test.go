package reconcile

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

// TestCollectorDeletesNothingInACopyOfTheWritersBucket is R7-F1: generation
// N is copied into a second bucket, and the deployment using that bucket
// then publishes N+1 into it while this database still names N. A collector
// of this database misdirected to the copy verifies the copied N against
// the digest PostgreSQL committed, so no manifest finding stops it, and
// N+1's manifest and the object only it lists are old keys of the layout no
// generation of this history names. The copy is not the target the
// deployment is bound to, so neither the diagnosis's plan nor the
// collection deletes or forgets anything there; pointed at the writers'
// bucket, the same database collects as usual.
func TestCollectorDeletesNothingInACopyOfTheWritersBucket(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	g1 := f.publishInit(t, f.initialized(t), 1)
	old := f.serverMillis(t) - time.Hour.Milliseconds()
	copied := newMemStore(f.store.now)
	for _, key := range f.store.keys() {
		raw, err := f.store.Get(ctx, key)
		require.NoError(t, err)
		copied.set(key, raw, old)
	}
	live := []string{ // the other deployment's N+1, unique to the copy
		f.layout.ManifestKey(testSchema, g1.rec.Epoch, g1.rec.Generation+1, uuid.New()),
		f.layout.ParquetKey(testSchema, g1.rec.Epoch, manifestv2.TierDelta, uuid.Must(uuid.NewV7())),
	}
	for _, key := range live {
		copied.set(key, []byte("the other deployment's live generation"), old)
	}
	before := copied.keys()

	for _, gc := range []bool{false, true} {
		r := f.reconciler(testOptions(gc))
		r.Objects, r.Storage = copied, publication.StorageTarget{Bucket: "copy-bucket", Prefix: f.layout.Prefix}
		report, err := r.Run(ctx)
		require.NoError(t, err)
		rep := report.Schemas[0]
		require.NoError(t, rep.Err)
		require.Equal(t, []FindingKind{UnboundStorage}, kinds(rep.Findings), "the copied N verifies: only the binding stops the pass, gc=%v", gc)
		require.Equal(t, actionRebindStorage, rep.Findings[0].Action)
		require.Contains(t, rep.Findings[0].Detail, `"writers-bucket"`)
		require.Contains(t, rep.Findings[0].Detail, `"copy-bucket"`)
		require.NotEmpty(t, rep.Refused, "gc=%v", gc)
		require.Empty(t, rep.Plan.Objects, "gc=%v", gc)
		require.Empty(t, rep.Plan.Manifests, "gc=%v", gc)
		require.Empty(t, rep.Plan.Forget, "gc=%v", gc)
		if gc {
			require.Empty(t, rep.Collected.Objects)
			require.Empty(t, rep.Collected.Manifests)
			require.Empty(t, rep.Collected.Forgotten)
		}
		require.Equal(t, before, copied.keys(), "nothing is deleted in the copy, gc=%v", gc)
	}

	f.store.set(live[1], []byte("an orphan in the writers' own bucket"), old)
	rep := f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Findings)
	require.Equal(t, []string{live[1]}, rep.Collected.Objects, "under the bound target the collection runs")
}

// TestAnUnboundDeploymentDeletesNothing: without a storage binding nothing
// binds the bucket, so the pass reports it with the binding's action and
// deletes nothing; BindStorage keeps the first binding, and a later one to
// another target does not replace it.
func TestAnUnboundDeploymentDeletesNothing(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.publishInit(t, f.initialized(t), 1)
	orphan := f.layout.ParquetKey(testSchema, 1, manifestv2.TierDelta, uuid.Must(uuid.NewV7()))
	f.store.set(orphan, []byte("old staging"), f.serverMillis(t)-time.Hour.Milliseconds())
	f.exec(t, "DELETE FROM %s", f.tables.Storage)
	before := f.store.keys()

	rep := f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Equal(t, []FindingKind{UnboundStorage}, kinds(rep.Findings))
	require.Equal(t, actionUnboundStorage, rep.Findings[0].Action)
	require.Empty(t, rep.Collected.Objects)
	require.Equal(t, before, f.store.keys())

	b, err := publication.BindStorage(ctx, publication.DB(f.db), f.tables, publication.StorageTarget{Bucket: "writers-bucket", Prefix: "data/"})
	require.NoError(t, err)
	require.True(t, b.Target.Same(f.target()), "a trailing slash names the same prefix")
	b, err = publication.BindStorage(ctx, publication.DB(f.db), f.tables, publication.StorageTarget{Bucket: "copy-bucket", Prefix: "data"})
	require.NoError(t, err)
	require.True(t, b.Target.Same(f.target()), "a later binding does not replace the first")
	rep = f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Findings)
	require.Equal(t, []string{orphan}, rep.Collected.Objects)
}

// TestARunWhoseTargetIsNotItsLayoutFails: the storage target a run checks
// against the binding must be the prefix it lists and deletes under.
func TestARunWhoseTargetIsNotItsLayoutFails(t *testing.T) {
	f := newFixture(t)
	r := f.reconciler(testOptions(true))
	r.Layout = manifestv2.Layout{Prefix: "other"}
	_, err := r.Run(context.Background())
	require.ErrorContains(t, err, "is not under its layout's prefix")
}

// destinedStore is a memStore that tells where its requests go, as an
// S3Objects tells from its client (Destination).
type destinedStore struct {
	*memStore
	endpoint string
}

func (s destinedStore) Endpoint(context.Context) (string, error) { return s.endpoint, nil }

// clientEndpoint answers where an S3 client with options o sends the
// requests for the fixture's bucket (ClientEndpoint).
func clientEndpoint(t *testing.T, f *fixture, o s3.Options) string {
	t.Helper()
	endpoint, err := ClientEndpoint(context.Background(), s3.New(o), f.target().Bucket)
	require.NoError(t, err)
	return endpoint
}

// TestCollectorDeletesNothingWhenItsClientIsRedirected: the deployment is
// bound to where its writers' client sends the bucket's requests, and a run
// with the same bucket and prefix, so unchanged flags, has a client sent
// elsewhere. R8-F1: an environment override (AWS_ENDPOINT_URL_S3) sends a
// client bound to the AWS endpoint to another service. R9-F1: at the same
// custom endpoint, only the addressing changes, from path-style (the
// endpoint's host, the bucket in the path) to virtual-hosted (a host named
// after the bucket), which a gateway can route to another backend. The
// run's target is where its client sends the requests, so neither the
// diagnosis's plan nor the collection deletes or forgets anything; a run
// that claims the bound target while its store sends requests elsewhere
// fails; and with the client at the bound target, the same state is
// collected.
func TestCollectorDeletesNothingWhenItsClientIsRedirected(t *testing.T) {
	for name, clients := range map[string][2]s3.Options{
		"endpoint override": {{Region: "us-east-1"}, {Region: "us-east-1", BaseEndpoint: aws.String("http://elsewhere.example:9000")}},
		"addressing": {
			{Region: "us-east-1", BaseEndpoint: aws.String("https://s3.storage.example"), UsePathStyle: true},
			{Region: "us-east-1", BaseEndpoint: aws.String("https://s3.storage.example")},
		},
	} {
		t.Run(name, func(t *testing.T) { requireRedirectedRunCollectsNothing(t, clients[0], clients[1]) })
	}
}

// requireRedirectedRunCollectsNothing binds the deployment to where a
// client of options writers sends the bucket's requests, and runs passes
// through a store whose client, of options run, sends them elsewhere.
func requireRedirectedRunCollectsNothing(t *testing.T, writers, run s3.Options) {
	t.Helper()
	f := newFixture(t)
	bound, redirected := clientEndpoint(t, f, writers), clientEndpoint(t, f, run)
	require.NotEqual(t, bound, redirected)
	ctx := context.Background()
	g3 := collectable(t, f)
	f.exec(t, "DELETE FROM %s", f.tables.Storage)
	target := f.target()
	target.Endpoint = bound
	_, err := publication.BindStorage(ctx, publication.DB(f.db), f.tables, target)
	require.NoError(t, err)
	before := f.store.keys()
	expired, err := publication.Expired(ctx, publication.DB(f.db), f.tables, testSchema, testOptions(true).RetentionGrace)
	require.NoError(t, err)
	require.Len(t, expired, 2, "generations 1 and 2 are past the grace: a collection would forget one")
	pass := func(gc bool, client, claimed string) (SchemaReport, error) {
		r := f.reconciler(testOptions(gc))
		r.Objects = destinedStore{memStore: f.store, endpoint: client}
		r.Storage = publication.StorageTarget{Endpoint: claimed, Bucket: target.Bucket, Prefix: target.Prefix}
		report, err := r.Run(ctx)
		if err != nil {
			return SchemaReport{}, err
		}
		return report.Schemas[0], nil
	}

	for _, gc := range []bool{false, true} {
		rep, err := pass(gc, redirected, redirected)
		require.NoError(t, err)
		require.NoError(t, rep.Err)
		require.Equal(t, []FindingKind{UnboundStorage}, kinds(rep.Findings), "gc=%v", gc)
		require.Equal(t, actionRebindStorage, rep.Findings[0].Action)
		require.Contains(t, rep.Findings[0].Detail, bound)
		require.Contains(t, rep.Findings[0].Detail, redirected)
		require.Empty(t, rep.Plan.Objects, "gc=%v", gc)
		require.Empty(t, rep.Plan.Manifests, "gc=%v", gc)
		require.Empty(t, rep.Plan.Forget, "gc=%v", gc)
		if gc {
			require.Empty(t, rep.Collected.Objects)
			require.Empty(t, rep.Collected.Manifests)
			require.Empty(t, rep.Collected.Forgotten)
		}
		require.Equal(t, before, f.store.keys(), "nothing is deleted, gc=%v", gc)

		_, err = pass(gc, redirected, bound)
		require.ErrorContains(t, err, "does not name the endpoint", "a target the store's client does not use, gc=%v", gc)
		require.Equal(t, before, f.store.keys(), "nothing is deleted, gc=%v", gc)
	}
	after, err := publication.Expired(ctx, publication.DB(f.db), f.tables, testSchema, testOptions(true).RetentionGrace)
	require.NoError(t, err)
	require.Equal(t, expired, after, "no history row is forgotten")

	rep, err := pass(true, bound+"/", bound)
	require.NoError(t, err)
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Findings)
	require.Equal(t, []int64{1}, rep.Collected.Forgotten, "at the bound endpoint the collection runs")
	require.Equal(t, f.keysOf(g3), f.store.keys())
}
