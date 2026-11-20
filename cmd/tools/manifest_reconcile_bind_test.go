package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyendpoints "github.com/aws/smithy-go/endpoints"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/reconcile"
	"github.com/lychee-technology/forma/internal/testdb"
)

// isolateAWSConfig keeps the developer's AWS configuration and endpoint
// overrides out of a test that resolves an S3 client's endpoint.
func isolateAWSConfig(t *testing.T) {
	t.Helper()
	none := filepath.Join(t.TempDir(), "none")
	t.Setenv("AWS_CONFIG_FILE", none)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", none)
	for _, v := range []string{"AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3",
		"AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "AWS_REGION", "AWS_DEFAULT_REGION"} {
		t.Setenv(v, "")
	}
}

// reconcileRun parses args and builds the run's S3 client and reconciler as
// runManifestReconcile does, without connecting anywhere.
func reconcileRun(t *testing.T, args ...string) (*reconcileOptions, *reconcile.S3Objects, *reconcile.Reconciler) {
	t.Helper()
	opts, err := parseReconcileFlags(reconcileArgs(args...))
	require.NoError(t, err)
	objects, err := buildReconcileObjects(context.Background(), opts)
	require.NoError(t, err)
	r, err := newReconciler(opts, objects, &sql.DB{}, zap.NewNop())
	require.NoError(t, err)
	return opts, objects, r
}

// TestNewReconciler_WiresTheStorageTarget: the run's storage target, which
// it deletes in only when it is the deployment's bound one, is the URL its
// S3 client sends the bucket's requests to, the bucket and the data prefix
// (R7-F1, R8-F1, R9-F1).
func TestNewReconciler_WiresTheStorageTarget(t *testing.T) {
	isolateAWSConfig(t)
	_, objects, r := reconcileRun(t, "--s3-endpoint", "http://rustfs:9000", "--s3-use-path", "--data-prefix", "lake")
	require.Equal(t, publication.StorageTarget{Endpoint: "http://rustfs:9000/bkt", Bucket: "bkt", Prefix: "lake"}, r.Storage)
	require.Same(t, objects, r.Objects, "the run lists and deletes through the client its endpoint was told from")
}

// TestReconcileTargetIncludesTheAddressing is R9-F1: with the same
// endpoint, bucket and prefix, --s3-use-path alone moves the client's LIST
// and DELETE from the endpoint's host, the bucket in the path, to a host
// named after the bucket, which a gateway can route to another backend.
// The run's target follows, so the two runs are not each other's target.
func TestReconcileTargetIncludesTheAddressing(t *testing.T) {
	isolateAWSConfig(t)
	_, _, path := reconcileRun(t, "--s3-endpoint", "https://s3.storage.example", "--s3-bucket", "lake", "--s3-use-path")
	_, _, virtual := reconcileRun(t, "--s3-endpoint", "https://s3.storage.example", "--s3-bucket", "lake")
	require.Equal(t, "https://s3.storage.example/lake", path.Storage.Endpoint)
	require.Equal(t, "https://lake.s3.storage.example", virtual.Storage.Endpoint)
	require.False(t, path.Storage.Same(virtual.Storage), "the addressing alone makes another target")
}

// TestClientEndpointIsWhereTheClientSendsListAndDelete checks
// reconcile.ClientEndpoint against the requests the client really builds:
// a transport records the URL of a LIST and of a DELETE, for either
// addressing, at a custom endpoint and at AWS.
func TestClientEndpointIsWhereTheClientSendsListAndDelete(t *testing.T) {
	isolateAWSConfig(t)
	for _, o := range []s3.Options{
		{Region: "us-east-1", BaseEndpoint: aws.String("https://s3.storage.example"), UsePathStyle: true},
		{Region: "us-east-1", BaseEndpoint: aws.String("https://s3.storage.example")},
		{Region: "eu-west-1"},
		{Region: "eu-west-1", UsePathStyle: true},
	} {
		rec := &recordingTransport{}
		o.HTTPClient = rec
		o.Credentials = aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
		})
		o.Retryer = aws.NopRetryer{}
		c := s3.New(o)
		endpoint, err := reconcile.ClientEndpoint(context.Background(), c, "lake")
		require.NoError(t, err)
		_, _ = c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String("lake"), Prefix: aws.String("data/7/")})
		_, _ = c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String("lake"), Key: aws.String("data/7/k")})
		require.Len(t, rec.urls, 2, "only the LIST and the DELETE reached the transport, nothing of ClientEndpoint's: %s", endpoint)
		require.Equal(t, endpoint, rec.urls[0].Scheme+"://"+rec.urls[0].Host+strings.TrimSuffix(rec.urls[0].Path, "/"), "the LIST")
		require.Equal(t, endpoint+"/data/7/k", rec.urls[1].Scheme+"://"+rec.urls[1].Host+rec.urls[1].Path, "the DELETE")
	}
}

// recordingTransport records the URL of every request and sends none.
type recordingTransport struct{ urls []url.URL }

func (r *recordingTransport) Do(req *http.Request) (*http.Response, error) {
	r.urls = append(r.urls, *req.URL)
	return nil, errors.New("recorded, not sent")
}

// TestReconcileTargetIsTheEndpointTheClientUses is R8-F1: with the flags
// unchanged, an SDK endpoint override in the environment redirects the
// run's client, and the run's target follows the client, not the flag, so
// it is no longer the target a run without the override binds. Without an
// override the target names the region's AWS endpoint, and --s3-endpoint
// wins over the environment, as it does for the client.
func TestReconcileTargetIsTheEndpointTheClientUses(t *testing.T) {
	isolateAWSConfig(t)
	_, _, plain := reconcileRun(t)
	require.Equal(t, publication.StorageTarget{Endpoint: "https://bkt.s3.us-east-1.amazonaws.com", Bucket: "bkt", Prefix: "data"}, plain.Storage)
	_, _, china := reconcileRun(t, "--s3-region", "cn-north-1")
	require.Equal(t, "https://bkt.s3.cn-north-1.amazonaws.com.cn", china.Storage.Endpoint, "another partition is another target")

	for _, v := range []string{"AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL"} {
		t.Run(v, func(t *testing.T) {
			t.Setenv(v, "http://elsewhere.example:9000")
			opts, objects, redirected := reconcileRun(t)
			require.Empty(t, opts.s3.endpoint, "the flag is unchanged")
			endpoint, err := objects.Endpoint(context.Background())
			require.NoError(t, err)
			require.Equal(t, "http://bkt.elsewhere.example:9000", endpoint, "the SDK sends the client's requests to the override")
			require.Equal(t, endpoint, redirected.Storage.Endpoint)
			require.False(t, redirected.Storage.Same(plain.Storage), "the redirected run is not the plain run's target")

			_, _, flagged := reconcileRun(t, "--s3-endpoint", "http://rustfs:9000", "--s3-use-path")
			require.Equal(t, "http://rustfs:9000/bkt", flagged.Storage.Endpoint, "--s3-endpoint wins over the environment")
		})
	}
}

// TestReconcileObjectsFollowNoRedirect is R10-F2 at the run's wiring: the
// store buildReconcileObjects answers, which the run is bound by and
// deletes through, refuses a redirect instead of following it. The bound
// server redirects a DELETE with a 307, which the SDK's own client would
// send on to the other server with the same method; here the delete fails
// and nothing reaches the other server.
func TestReconcileObjectsFollowNoRedirect(t *testing.T) {
	isolateAWSConfig(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	var reached atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(other.Close)
	bound := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+r.URL.RequestURI(), http.StatusTemporaryRedirect)
	}))
	t.Cleanup(bound.Close)

	_, objects, r := reconcileRun(t, "--s3-endpoint", bound.URL, "--s3-use-path")
	require.Equal(t, bound.URL+"/bkt", r.Storage.Endpoint, "the run is bound by the URL its requests are built for")
	err := objects.Delete(context.Background(), "data/7/k")
	require.ErrorIs(t, err, reconcile.ErrRedirectRefused)
	_, err = objects.List(context.Background(), "data/7/")
	require.ErrorIs(t, err, reconcile.ErrRedirectRefused)
	require.Zero(t, reached.Load(), "no request reached the other server")
}

// TestS3ObjectsEndpointRefusesAClientItCannotName: a client that is not an
// *s3.Client, one that may follow a redirect (not built by
// reconcile.NewS3Objects), or one whose endpoint a resolver of the caller's
// chooses, cannot tell where its requests go, and a run through it fails.
func TestS3ObjectsEndpointRefusesAClientItCannotName(t *testing.T) {
	isolateAWSConfig(t)
	_, err := (&reconcile.S3Objects{Bucket: "bkt"}).Endpoint(context.Background())
	require.ErrorContains(t, err, "destination cannot be told")
	following := s3.New(s3.Options{Region: "us-east-1"})
	_, err = (&reconcile.S3Objects{Client: following, Bucket: "bkt"}).Endpoint(context.Background())
	require.ErrorContains(t, err, "may follow a redirect")
	custom := s3.New(s3.Options{Region: "us-east-1", EndpointResolverV2: customResolver{}})
	_, err = reconcile.ClientEndpoint(context.Background(), custom, "bkt")
	require.ErrorContains(t, err, "destination cannot be told")
}

type customResolver struct{}

func (customResolver) ResolveEndpoint(context.Context, s3.EndpointParameters) (smithyendpoints.Endpoint, error) {
	return smithyendpoints.Endpoint{}, errors.New("never called")
}

func TestParseReconcileFlags_BindStorage(t *testing.T) {
	opts, err := parseReconcileFlags(reconcileArgs("--bind-storage"))
	require.NoError(t, err)
	require.True(t, opts.bindStorage)
	require.False(t, opts.run.GC)
	_, err = parseReconcileFlags(reconcileArgs("--bind-storage", "--gc"))
	require.ErrorContains(t, err, "--bind-storage binds and exits")
}

// TestBindReconcileStorage creates the binding table on first use, binds
// the configured target once, accepts the same target again in another
// spelling, and keeps the binding against a run configured with another
// bucket, which fails.
func TestBindReconcileStorage(t *testing.T) {
	ctx := context.Background()
	testdb.Connect(t, ctx) // skips when Postgres is absent locally
	db, err := sql.Open("pgx", testdb.ResolveDSN())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	entityMain := fmt.Sprintf("bind_test_%d", time.Now().UnixNano())
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS " + publication.TablesFor(entityMain).Storage) })
	isolateAWSConfig(t)
	bind := func(bucket, prefix string) (string, error) {
		opts, err := parseReconcileFlags([]string{"--s3-bucket", bucket, "--schema-registry-table", "schema_registry",
			"--entity-main-table", entityMain, "--data-prefix", prefix, "--bind-storage"})
		require.NoError(t, err)
		_, err = buildReconcileObjects(ctx, opts)
		require.NoError(t, err)
		var out bytes.Buffer
		err = bindReconcileStorage(ctx, db, opts, &out)
		return out.String(), err
	}

	out, err := bind("writers", "data")
	require.NoError(t, err)
	require.Contains(t, out, `bucket "writers", data prefix "data", at https://writers.s3.us-east-1.amazonaws.com`)
	_, err = bind("writers", "data/")
	require.NoError(t, err, "the same target, spelled with a trailing slash")
	_, err = bind("copy", "data")
	require.ErrorIs(t, err, errStorageBoundElsewhere)
	t.Setenv("AWS_ENDPOINT_URL_S3", "http://elsewhere.example:9000")
	_, err = bind("writers", "data")
	require.ErrorIs(t, err, errStorageBoundElsewhere, "unchanged flags, but the client is redirected (R8-F1)")

	b, ok, err := publication.ReadStorage(ctx, publication.DB(db), publication.TablesFor(entityMain))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, publication.StorageTarget{Endpoint: "https://writers.s3.us-east-1.amazonaws.com", Bucket: "writers", Prefix: "data"}, b.Target)
}
