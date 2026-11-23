package reconcile

import (
	"bytes"
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
	"github.com/lychee-technology/forma/internal/s3endpoint"
)

// TestCollectorKeepsAnotherDeploymentsObjectsForAnUnpublishedSchema verifies
// #463: an UNPUBLISHED schema with no verified manifest in the bucket refuses
// to delete objects matching the layout until its own generation publishes.
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
		require.True(t, f.layout.Owns(testSchema, key))
		f.store.set(key, []byte("another deployment's live object"), old)
	}
	before := f.store.keys()

	for _, gc := range []bool{true, false} {
		rep := f.pass(t, testOptions(gc))
		require.NoError(t, rep.Err)
		require.Empty(t, rep.Findings, "gc=%v", gc)
		require.Contains(t, rep.Refused, "no manifest", "gc=%v", gc)
		require.True(t, rep.Residual(), "gc=%v", gc)
		var out bytes.Buffer
		Report{Schemas: []SchemaReport{rep}}.Render(&out)
		require.Contains(t, out.String(), "collection refused", "gc=%v", gc)
		require.NotContains(t, out.String(), ": clean (", "gc=%v", gc)
		require.Empty(t, rep.Plan.Objects, "gc=%v", gc)
		require.Empty(t, rep.Plan.Manifests, "gc=%v", gc)
		if gc {
			require.Empty(t, rep.Collected.Objects)
			require.Empty(t, rep.Collected.Manifests)
		}
		require.Equal(t, before, f.store.keys(), "gc=%v", gc)
	}

	g1 := f.publishInit(t, f.record(t), 1)
	rep := f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Refused)
	require.False(t, rep.Residual())
	require.ElementsMatch(t, foreign, rep.Collected.Objects)
	require.Equal(t, f.keysOf(g1), f.store.keys())
}

type truncatingStore struct{ *memStore }

func (s truncatingStore) List(ctx context.Context, prefix string) ([]publication.Object, error) {
	listed, err := s.memStore.List(ctx, prefix)
	for i := range listed {
		listed[i].LastModified -= listed[i].LastModified % time.Second.Milliseconds()
	}
	return listed, err
}

// TestCollectorKeepsStagingItsListingRoundsBelowTheHorizon verifies that S3's
// whole-second LastModified truncation cannot cause deletion of staging whose
// publication can still commit against the horizon.
func TestCollectorKeepsStagingItsListingRoundsBelowTheHorizon(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	g1 := f.publishInit(t, f.initialized(t), 1)
	f.waitIntoASecond(t)
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
			require.Less(t, o.LastModified, horizon)
		}
	}

	report, err := r.Run(ctx)
	require.NoError(t, err)
	rep := report.Schemas[0]
	require.NoError(t, rep.Err)
	require.Equal(t, horizon, rep.Horizon)
	require.Empty(t, rep.Collected.Objects)
	require.Equal(t, len(staging), rep.PendingOrphans)
	_, err = f.commit(t, inFlight)
	require.NoError(t, err)
}

func (f *fixture) waitIntoASecond(t *testing.T) {
	t.Helper()
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if ms := f.serverMillis(t) % 1000; ms >= 100 && ms < 500 {
			return
		}
	}
	t.Fatal("database clock never reached 100-500ms into a second")
}

// TestCollectorDeletesNothingInACopyOfTheWritersBucket verifies that a copied
// bucket holding a valid manifest N plus another deployment's N+1 is protected
// by the deployment's PostgreSQL storage binding.
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
	live := []string{
		f.layout.ManifestKey(testSchema, g1.rec.Epoch, g1.rec.Generation+1, uuid.New()),
		f.layout.ParquetKey(testSchema, g1.rec.Epoch, manifestv2.TierDelta, uuid.Must(uuid.NewV7())),
	}
	for _, key := range live {
		copied.set(key, []byte("other deployment's live generation"), old)
	}
	before := copied.keys()

	for _, gc := range []bool{false, true} {
		r := f.reconciler(testOptions(gc))
		r.Objects, r.Storage = copied, publication.StorageTarget{Bucket: "copy-bucket", Prefix: f.layout.Prefix}
		report, err := r.Run(ctx)
		require.NoError(t, err)
		rep := report.Schemas[0]
		require.NoError(t, rep.Err)
		require.Equal(t, []FindingKind{UnboundStorage}, kinds(rep.Findings), "gc=%v", gc)
		require.Equal(t, actionRebindStorage, rep.Findings[0].Action)
		require.Contains(t, rep.Findings[0].Detail, `"writers-bucket"`)
		require.Contains(t, rep.Findings[0].Detail, `"copy-bucket"`)
		require.NotEmpty(t, rep.Refused)
		require.Empty(t, rep.Plan.Objects)
		require.Empty(t, rep.Plan.Manifests)
		require.Empty(t, rep.Plan.Forget)
		if gc {
			require.Empty(t, rep.Collected.Objects)
			require.Empty(t, rep.Collected.Manifests)
			require.Empty(t, rep.Collected.Forgotten)
		}
		require.Equal(t, before, copied.keys())
	}

	f.store.set(live[1], []byte("orphan in writers bucket"), old)
	rep := f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Findings)
	require.Equal(t, []string{live[1]}, rep.Collected.Objects)
}

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
	require.True(t, b.Target.Same(f.target()))
	b, err = publication.BindStorage(ctx, publication.DB(f.db), f.tables, publication.StorageTarget{Bucket: "copy-bucket", Prefix: "data"})
	require.NoError(t, err)
	require.True(t, b.Target.Same(f.target()))
	rep = f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Findings)
	require.Equal(t, []string{orphan}, rep.Collected.Objects)
}

func TestARunWhoseTargetIsNotItsLayoutFails(t *testing.T) {
	f := newFixture(t)
	r := f.reconciler(testOptions(true))
	r.Layout = manifestv2.Layout{Prefix: "other"}
	_, err := r.Run(context.Background())
	require.ErrorContains(t, err, "is not under its layout's prefix")
}

type destinedStore struct {
	*memStore
	endpoint string
}

func (s destinedStore) Endpoint(context.Context) (string, error) { return s.endpoint, nil }

func clientEndpoint(t *testing.T, f *fixture, o s3.Options) string {
	t.Helper()
	endpoint, err := s3endpoint.ClientEndpoint(context.Background(), s3.New(o), f.target().Bucket)
	require.NoError(t, err)
	return endpoint
}

func TestCollectorDeletesNothingWhenItsClientIsRedirected(t *testing.T) {
	for name, clients := range map[string][2]s3.Options{
		"endpoint override": {{Region: "us-east-1"}, {Region: "us-east-1", BaseEndpoint: aws.String("http://elsewhere.example:9000")}},
		"addressing": {
			{Region: "us-east-1", BaseEndpoint: aws.String("https://s3.storage.example"), UsePathStyle: true},
			{Region: "us-east-1", BaseEndpoint: aws.String("https://s3.storage.example")},
		},
		"custom path prefix": {
			{Region: "us-east-1", BaseEndpoint: aws.String("https://s3.storage.example/tenant-a")},
			{Region: "us-east-1", BaseEndpoint: aws.String("https://s3.storage.example/tenant-b")},
		},
	} {
		t.Run(name, func(t *testing.T) { requireRedirectedRunCollectsNothing(t, clients[0], clients[1]) })
	}
}

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
	require.Len(t, expired, 2)
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
		require.Empty(t, rep.Plan.Objects)
		require.Empty(t, rep.Plan.Manifests)
		require.Empty(t, rep.Plan.Forget)
		if gc {
			require.Empty(t, rep.Collected.Objects)
			require.Empty(t, rep.Collected.Manifests)
			require.Empty(t, rep.Collected.Forgotten)
		}
		require.Equal(t, before, f.store.keys())

		_, err = pass(gc, redirected, bound)
		require.ErrorContains(t, err, "does not match the endpoint")
		require.Equal(t, before, f.store.keys())
	}
	after, err := publication.Expired(ctx, publication.DB(f.db), f.tables, testSchema, testOptions(true).RetentionGrace)
	require.NoError(t, err)
	require.Equal(t, expired, after)

	rep, err := pass(true, bound, bound)
	require.NoError(t, err)
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Findings)
	require.Equal(t, []int64{1, 2}, rep.Collected.Forgotten)
	require.Equal(t, f.keysOf(g3), f.store.keys())
}
