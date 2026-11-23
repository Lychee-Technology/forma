package cdcv2

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyendpoints "github.com/aws/smithy-go/endpoints"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/s3endpoint"
)

// s3Origin is an S3 endpoint that answers every request with a 500 and
// counts them, so a test can tell whether a writer sent anything to S3.
type s3Origin struct {
	srv  *httptest.Server
	hits atomic.Int64
}

func newS3Origin(t *testing.T) *s3Origin {
	t.Helper()
	o := &s3Origin{}
	o.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		o.hits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(o.srv.Close)
	return o
}

// client answers an AWS SDK client of the origin, path-style, with the
// options mods change.
func (o *s3Origin) client(mods ...func(*s3.Options)) *s3.Client {
	return s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(o.srv.URL),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
		Retryer:      aws.NopRetryer{},
	}, mods...)
}

// target is where a writer over the origin's bucket lake publishes.
func (o *s3Origin) target(f *fixture) publication.StorageTarget {
	return publication.StorageTarget{Endpoint: o.srv.URL + "/lake", Bucket: "lake", Prefix: f.layout.Prefix}
}

// overS3 points a writer's store at client's bucket lake.
func overS3(client *s3.Client) func(*Config) {
	return func(c *Config) {
		store := &manifestv2.S3Store{Client: client, Bucket: "lake"}
		cache, err := manifestv2.NewCache(store, 4)
		if err != nil {
			panic(err)
		}
		c.Store, c.Cache = store, cache
	}
}

func (f *fixture) binding(t *testing.T) (publication.StorageBinding, bool) {
	t.Helper()
	b, ok, err := publication.ReadStorage(context.Background(), publication.DB(f.db), f.tables)
	require.NoError(t, err)
	return b, ok
}

// TestWriterBindsTheDeploymentBeforeItStages pins #911's first half: a
// writer over an AWS SDK client binds an unbound deployment to the bucket URL
// that client sends requests to, the derivation manifest-reconcile checks
// its own client with, before it sends anything to S3. It reads the binding
// once per writer.
func TestWriterBindsTheDeploymentBeforeItStages(t *testing.T) {
	f := newFixture(t)
	f.register(t)
	origin := newS3Origin(t)
	w := f.writer(t, overS3(origin.client()))

	_, err := w.Flush(context.Background(), schemaID, 0, nil)
	require.NoError(t, err, "a flush of nothing stages nothing")
	b, ok := f.binding(t)
	require.True(t, ok, "the writer bound the deployment")
	require.True(t, b.Target.Same(origin.target(f)), "bound to %s, want %s", b.Target, origin.target(f))
	endpoint, err := s3endpoint.ClientEndpoint(context.Background(), origin.client(), "lake")
	require.NoError(t, err)
	require.Equal(t, endpoint, b.Target.Endpoint, "the writer binds what manifest-reconcile derives for the same client")
	require.Zero(t, origin.hits.Load(), "deriving the target sends nothing to S3")

	_, err = f.db.Exec("UPDATE "+f.tables.Storage+" SET bucket = $1", "pond")
	require.NoError(t, err)
	_, err = w.Flush(context.Background(), schemaID, 0, nil)
	require.NoError(t, err, "a writer checks the binding once, before its first staging")
	_, err = f.writer(t, overS3(origin.client())).Flush(context.Background(), schemaID, 0, nil)
	require.ErrorIs(t, err, publication.ErrStorageBoundElsewhere, "a new writer checks again")
}

// TestWritersRefuseADeploymentBoundElsewhere pins #911's second half: a
// flush, an init and a compaction whose target is not the bound one fail
// before they send anything to S3 or move the record, and keep the binding.
func TestWritersRefuseADeploymentBoundElsewhere(t *testing.T) {
	f := newFixture(t)
	rec := f.register(t)
	origin := newS3Origin(t)
	elsewhere := origin.target(f)
	elsewhere.Prefix = "other"
	bound, err := publication.BindStorage(context.Background(), publication.DB(f.db), f.tables, elsewhere)
	require.NoError(t, err)

	for name, run := range map[string]func(*Writer) error{
		"flush": func(w *Writer) error { _, err := w.Flush(context.Background(), schemaID, 0, nil); return err },
		"init":  func(w *Writer) error { _, err := w.Init(context.Background(), schemaID, 0, nil); return err },
		"compact": func(w *Writer) error {
			_, err := w.Compact(context.Background(), schemaID, 0, nil)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := run(f.writer(t, overS3(origin.client())))
			require.ErrorIs(t, err, publication.ErrStorageBoundElsewhere)
			require.ErrorContains(t, err, fmt.Sprintf("%q", "other"), "the error names the bound target")
			require.Zero(t, origin.hits.Load(), "nothing reached S3")
			require.Equal(t, rec, f.record(t), "the record did not move")
			b, ok := f.binding(t)
			require.True(t, ok)
			require.Equal(t, bound, b, "the binding is kept")
		})
	}
}

// TestWriterWithoutAnS3TargetBindsNothing: a store that is not an S3Store
// over an AWS SDK client names no target a collector could be bound to.
func TestWriterWithoutAnS3TargetBindsNothing(t *testing.T) {
	f := newFixture(t)
	f.register(t)
	_, err := f.writer(t).Flush(context.Background(), schemaID, 0, nil)
	require.NoError(t, err)
	_, ok := f.binding(t)
	require.False(t, ok)
}

// TestWriterRefusesAClientWhoseDestinationCannotBeTold: an AWS SDK client
// whose endpoint resolver is the caller's publishes somewhere no binding can
// name, so the writer fails before it stages.
func TestWriterRefusesAClientWhoseDestinationCannotBeTold(t *testing.T) {
	f := newFixture(t)
	f.register(t)
	origin := newS3Origin(t)
	custom := origin.client(func(o *s3.Options) { o.EndpointResolverV2 = fixedResolver{} })
	_, err := f.writer(t, overS3(custom)).Flush(context.Background(), schemaID, 0, nil)
	require.ErrorIs(t, err, s3endpoint.ErrUnknownDestination)
	_, ok := f.binding(t)
	require.False(t, ok)
	require.Zero(t, origin.hits.Load())
}

// TestWriterOnADeploymentWithoutTheBindingTable: init-db creates the table;
// a deployment whose init-db predates it fails its writers until it runs.
func TestWriterOnADeploymentWithoutTheBindingTable(t *testing.T) {
	f := newFixture(t)
	f.register(t)
	_, err := f.db.Exec("DROP TABLE " + f.tables.Storage)
	require.NoError(t, err)
	origin := newS3Origin(t)
	_, err = f.writer(t, overS3(origin.client())).Flush(context.Background(), schemaID, 0, nil)
	require.ErrorIs(t, err, publication.ErrStorageTableMissing)
	require.ErrorContains(t, err, "init-db")
	require.Zero(t, origin.hits.Load())
}

type fixedResolver struct{}

func (fixedResolver) ResolveEndpoint(context.Context, s3.EndpointParameters) (smithyendpoints.Endpoint, error) {
	return smithyendpoints.Endpoint{}, fmt.Errorf("not resolved")
}
