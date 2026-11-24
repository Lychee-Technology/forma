package cdcv2

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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

// s3Origin is a path-style S3 endpoint for bucket lake over the fixture's
// file store, so that what a writer puts through an AWS SDK client and what
// the test stagers write to the store are one bucket. A PUT writes once (412
// when the key exists), a GET answers the object or NoSuchKey. With redirect
// set, every request is answered 303 to that origin instead. hits counts the
// requests it was sent.
type s3Origin struct {
	srv      *httptest.Server
	store    *manifestv2.FSStore
	hits     atomic.Int64
	redirect atomic.Pointer[string]
}

func newS3Origin(t *testing.T, store *manifestv2.FSStore) *s3Origin {
	t.Helper()
	o := &s3Origin{store: store}
	o.srv = httptest.NewServer(http.HandlerFunc(o.serve))
	t.Cleanup(o.srv.Close)
	return o
}

func (o *s3Origin) serve(w http.ResponseWriter, r *http.Request) {
	o.hits.Add(1)
	if to := o.redirect.Load(); to != nil {
		w.Header().Set("Location", *to+r.URL.RequestURI())
		w.WriteHeader(http.StatusSeeOther)
		return
	}
	key, ok := strings.CutPrefix(r.URL.Path, "/lake/")
	if !ok || r.Header.Get("Content-Encoding") == "aws-chunked" {
		s3Error(w, http.StatusBadRequest, "InvalidRequest")
		return
	}
	switch r.Method {
	case http.MethodPut:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			s3Error(w, http.StatusInternalServerError, "InternalError")
			return
		}
		err = o.store.Put(r.Context(), key, bytes.NewReader(body))
		switch {
		case errors.Is(err, manifestv2.ErrKeyExists):
			s3Error(w, http.StatusPreconditionFailed, "PreconditionFailed")
		case err != nil:
			s3Error(w, http.StatusInternalServerError, "InternalError")
		}
	case http.MethodGet:
		raw, err := o.store.Get(r.Context(), key)
		switch {
		case errors.Is(err, manifestv2.ErrObjectNotFound):
			s3Error(w, http.StatusNotFound, "NoSuchKey")
		case err != nil:
			s3Error(w, http.StatusInternalServerError, "InternalError")
		default:
			_, _ = w.Write(raw)
		}
	default:
		s3Error(w, http.StatusMethodNotAllowed, "MethodNotAllowed")
	}
}

func s3Error(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, "<Error><Code>%s</Code><Message>%s</Message></Error>", code, code)
}

// plainClient answers an AWS SDK client of the origin, path-style, with the
// options mods change, following redirects as the SDK's default client does.
func (o *s3Origin) plainClient(mods ...func(*s3.Options)) *s3.Client {
	return s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(o.srv.URL),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
		Retryer:      aws.NopRetryer{},
	}, mods...)
}

// client is plainClient made to follow no redirect, as the tools build it.
func (o *s3Origin) client(t *testing.T, mods ...func(*s3.Options)) *s3.Client {
	t.Helper()
	c, err := s3endpoint.WithoutRedirects(o.plainClient(mods...))
	require.NoError(t, err)
	return c
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

// TestAWriterBindsOnlyWhenItPublishes pins R1-F2 of #911: a run that
// publishes nothing (an empty flush, a compaction of an unpublished schema)
// binds nothing and sends nothing to S3, and the first publication binds
// the deployment, in its own transaction, to the bucket URL the writer's
// client sends requests to, the derivation manifest-reconcile checks its own
// client with. A writer reads the binding once it is bound, and a new writer
// checks it again.
func TestAWriterBindsOnlyWhenItPublishes(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	f.register(t)
	origin := newS3Origin(t, f.store)
	w := f.writer(t, overS3(origin.client(t)))

	_, err := w.Flush(ctx, schemaID, 0, &stager{f: f})
	require.NoError(t, err, "a flush of nothing stages nothing")
	rep, err := w.Compact(ctx, schemaID, 0, &compactStager{f: f})
	require.NoError(t, err)
	require.False(t, rep.Committed, "nothing is published to compact")
	_, ok := f.binding(t)
	require.False(t, ok, "a run that publishes nothing binds nothing")
	require.Zero(t, origin.hits.Load())

	f.insert(t, 2)
	frep, err := w.Flush(ctx, schemaID, 0, &stager{f: f})
	require.NoError(t, err)
	require.Len(t, frep.Generations, 1)
	b, ok := f.binding(t)
	require.True(t, ok, "the first publication bound the deployment")
	require.True(t, b.Target.Same(origin.target(f)), "bound to %s, want %s", b.Target, origin.target(f))
	endpoint, err := s3endpoint.ClientEndpoint(ctx, origin.client(t), "lake")
	require.NoError(t, err)
	require.Equal(t, endpoint, b.Target.Endpoint, "the writer binds what manifest-reconcile derives for the same client")
	f.checkInvariants(t)

	_, err = f.db.Exec("UPDATE "+f.tables.Storage+" SET bucket = $1", "pond")
	require.NoError(t, err)
	f.insert(t, 1)
	_, err = w.Flush(ctx, schemaID, 0, &stager{f: f})
	require.NoError(t, err, "a bound writer reads the binding no more")
	f.insert(t, 1)
	_, err = f.writer(t, overS3(origin.client(t))).Flush(ctx, schemaID, 0, &stager{f: f})
	require.ErrorIs(t, err, publication.ErrStorageBoundElsewhere, "a new writer checks again")
}

// TestACompactionBindsOnlyWhatItPublishes pins R1-F2 of #911 for the
// compactor on a deployment published before writers bound it: a compactor
// with another prefix than the published one is refused before it binds or
// sends anything, so it cannot claim the deployment for its prefix; one that
// merges nothing binds nothing; and the first compaction that commits binds
// the deployment to its target.
func TestACompactionBindsOnlyWhatItPublishes(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	twoFlushes(t, f)
	origin := newS3Origin(t, f.store)
	_, ok := f.binding(t)
	require.False(t, ok, "a writer over a file store binds nothing")

	foreign := f.writer(t, overS3(origin.client(t)), func(c *Config) { c.Layout = manifestv2.Layout{Prefix: "elsewhere"} })
	_, err := foreign.Compact(ctx, schemaID, 0, &compactStager{f: f})
	require.ErrorIs(t, err, manifestv2.ErrForeignPrefix)
	_, ok = f.binding(t)
	require.False(t, ok, "a refused compaction binds nothing")
	require.Zero(t, origin.hits.Load())

	w := f.writer(t, overS3(origin.client(t)))
	nothing := &compactStager{f: f, pick: func(*manifestv2.Manifest) []manifestv2.File { return nil }}
	rep, err := w.Compact(ctx, schemaID, 0, nothing)
	require.NoError(t, err)
	require.False(t, rep.Committed)
	_, ok = f.binding(t)
	require.False(t, ok, "a compaction that merges nothing binds nothing")

	rep, err = w.Compact(ctx, schemaID, 0, &compactStager{f: f})
	require.NoError(t, err)
	require.True(t, rep.Committed)
	b, ok := f.binding(t)
	require.True(t, ok)
	require.True(t, b.Target.Same(origin.target(f)), "bound to %s", b.Target)
	f.checkInvariants(t)
}

// TestWritersRefuseADeploymentBoundElsewhere pins #911's second half: a
// flush, an init and a compaction whose target is not the bound one fail
// before they send anything to S3 or move the record, and keep the binding.
func TestWritersRefuseADeploymentBoundElsewhere(t *testing.T) {
	f := newFixture(t)
	twoFlushes(t, f)
	f.insert(t, 1)
	rec := f.record(t)
	origin := newS3Origin(t, f.store)
	elsewhere := origin.target(f)
	elsewhere.Prefix = "other"
	bound, err := publication.BindStorage(context.Background(), publication.DB(f.db), f.tables, elsewhere)
	require.NoError(t, err)

	for name, run := range map[string]func(*Writer) error{
		"flush": func(w *Writer) error { _, err := w.Flush(context.Background(), schemaID, 0, &stager{f: f}); return err },
		"init":  func(w *Writer) error { _, err := w.Init(context.Background(), schemaID, 0, &stager{f: f}); return err },
		"compact": func(w *Writer) error {
			_, err := w.Compact(context.Background(), schemaID, 0, &compactStager{f: f})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := run(f.writer(t, overS3(origin.client(t))))
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

// TestARivalBindingStopsAStagedPublication: two writers of an unbound
// deployment may both stage; the one whose rival bound another target first
// publishes nothing, and the record does not move.
func TestARivalBindingStopsAStagedPublication(t *testing.T) {
	f := newFixture(t)
	rec := f.register(t)
	f.insert(t, 1)
	origin := newS3Origin(t, f.store)
	rival := origin.target(f)
	rival.Prefix = "rival"
	w := f.writer(t, overS3(origin.client(t)), func(c *Config) {
		c.Failpoint = func(ctx context.Context, p Point) error {
			if p != PointStageObjects {
				return nil
			}
			_, err := publication.BindStorage(ctx, publication.DB(f.db), f.tables, rival)
			return err
		}
	})
	_, err := w.Flush(context.Background(), schemaID, 0, &stager{f: f})
	require.ErrorIs(t, err, publication.ErrStorageBoundElsewhere)
	require.Equal(t, rec, f.record(t), "the record did not move")
	b, ok := f.binding(t)
	require.True(t, ok)
	require.True(t, b.Target.Same(rival), "the rival's binding is kept")
	require.Len(t, f.dirty(t), 1, "the entry stays dirty")
}

// TestARedirectedWriteNeverAdvancesTheRecord pins R1-F3 of #911 on the
// writer's own path: a writer's S3 client follows no redirect, so a
// redirected PUT of its manifest reaches no other route, fails the flush,
// binds nothing and leaves the record and the entries as they were. A client
// that may follow redirects is refused when the writer is built.
func TestARedirectedWriteNeverAdvancesTheRecord(t *testing.T) {
	f := newFixture(t)
	rec := f.register(t)
	f.insert(t, 1)
	var elsewhere atomic.Int64
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		elsewhere.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(other.Close)
	origin := newS3Origin(t, f.store)
	origin.redirect.Store(&other.URL)

	_, err := New(Config{DB: f.db, EntityMain: f.entityMain, Store: &manifestv2.S3Store{Client: origin.plainClient(), Bucket: "lake"}, Cache: &manifestv2.Cache{}})
	require.ErrorIs(t, err, s3endpoint.ErrUnknownDestination, "a client that may follow redirects is refused")

	_, err = f.writer(t, overS3(origin.client(t))).Flush(context.Background(), schemaID, 0, &stager{f: f})
	require.ErrorIs(t, err, s3endpoint.ErrRedirectRefused)
	require.NotZero(t, origin.hits.Load())
	require.Zero(t, elsewhere.Load(), "no request reached the other route")
	require.Equal(t, rec, f.record(t), "the record did not move")
	require.Len(t, f.dirty(t), 1, "the entry stays dirty")
	_, ok := f.binding(t)
	require.False(t, ok, "a write that failed binds nothing")
}

// TestWriterWithoutAnS3TargetBindsNothing: a store that is not an S3Store
// over an AWS SDK client names no target a collector could be bound to.
func TestWriterWithoutAnS3TargetBindsNothing(t *testing.T) {
	f := newFixture(t)
	f.register(t)
	f.insert(t, 1)
	rep, err := f.writer(t).Flush(context.Background(), schemaID, 0, &stager{f: f})
	require.NoError(t, err)
	require.Len(t, rep.Generations, 1)
	_, ok := f.binding(t)
	require.False(t, ok)
}

// TestWriterRefusesAClientWhoseDestinationCannotBeTold: an AWS SDK client
// whose endpoint resolver is the caller's publishes somewhere no binding can
// name, so the writer fails before it stages.
func TestWriterRefusesAClientWhoseDestinationCannotBeTold(t *testing.T) {
	f := newFixture(t)
	rec := f.register(t)
	f.insert(t, 1)
	origin := newS3Origin(t, f.store)
	custom := origin.client(t, func(o *s3.Options) { o.EndpointResolverV2 = fixedResolver{} })
	_, err := f.writer(t, overS3(custom)).Flush(context.Background(), schemaID, 0, &stager{f: f})
	require.ErrorIs(t, err, s3endpoint.ErrUnknownDestination)
	_, ok := f.binding(t)
	require.False(t, ok)
	require.Zero(t, origin.hits.Load())
	require.Equal(t, rec, f.record(t))
}

// TestWriterOnADeploymentWithoutTheBindingTable: init-db creates the table on
// PostgreSQL; a deployment without it fails its writers before they stage,
// with what to run on either engine.
func TestWriterOnADeploymentWithoutTheBindingTable(t *testing.T) {
	f := newFixture(t)
	f.register(t)
	f.insert(t, 1)
	_, err := f.db.Exec("DROP TABLE " + f.tables.Storage)
	require.NoError(t, err)
	origin := newS3Origin(t, f.store)
	_, err = f.writer(t, overS3(origin.client(t))).Flush(context.Background(), schemaID, 0, &stager{f: f})
	require.ErrorIs(t, err, publication.ErrStorageTableMissing)
	require.ErrorContains(t, err, "on PostgreSQL run init-db")
	require.ErrorContains(t, err, "on Aurora DSQL")
	require.ErrorContains(t, err, "manifest-reconcile --bind-storage")
	require.Zero(t, origin.hits.Load())
}

type fixedResolver struct{}

func (fixedResolver) ResolveEndpoint(context.Context, s3.EndpointParameters) (smithyendpoints.Endpoint, error) {
	return smithyendpoints.Endpoint{}, fmt.Errorf("not resolved")
}
