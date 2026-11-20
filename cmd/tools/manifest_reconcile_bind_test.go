package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

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
// it deletes in only when it is the deployment's bound one, is the
// endpoint its S3 client sends requests to, the bucket and the data prefix
// (R7-F1, R8-F1).
func TestNewReconciler_WiresTheStorageTarget(t *testing.T) {
	isolateAWSConfig(t)
	_, objects, r := reconcileRun(t, "--s3-endpoint", "http://rustfs:9000", "--data-prefix", "lake")
	require.Equal(t, publication.StorageTarget{Endpoint: "http://rustfs:9000", Bucket: "bkt", Prefix: "lake"}, r.Storage)
	require.Same(t, objects, r.Objects, "the run lists and deletes through the client its endpoint was told from")
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
	require.Equal(t, publication.StorageTarget{Endpoint: "https://s3.us-east-1.amazonaws.com", Bucket: "bkt", Prefix: "data"}, plain.Storage)
	_, _, china := reconcileRun(t, "--s3-region", "cn-north-1")
	require.Equal(t, "https://s3.cn-north-1.amazonaws.com.cn", china.Storage.Endpoint, "another partition is another target")

	for _, v := range []string{"AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL"} {
		t.Run(v, func(t *testing.T) {
			t.Setenv(v, "http://elsewhere.example:9000")
			opts, objects, redirected := reconcileRun(t)
			require.Empty(t, opts.s3.endpoint, "the flag is unchanged")
			endpoint, err := objects.Endpoint(context.Background())
			require.NoError(t, err)
			require.Equal(t, "http://elsewhere.example:9000", endpoint, "the SDK sends the client's requests to the override")
			require.Equal(t, endpoint, redirected.Storage.Endpoint)
			require.False(t, redirected.Storage.Same(plain.Storage), "the redirected run is not the plain run's target")

			_, _, flagged := reconcileRun(t, "--s3-endpoint", "http://rustfs:9000")
			require.Equal(t, "http://rustfs:9000", flagged.Storage.Endpoint, "--s3-endpoint wins over the environment")
		})
	}
}

// TestS3ObjectsEndpointRefusesAClientItCannotName: a client that is not an
// *s3.Client, or one whose endpoint a resolver of the caller's chooses,
// cannot tell where its requests go, and a run through it fails.
func TestS3ObjectsEndpointRefusesAClientItCannotName(t *testing.T) {
	isolateAWSConfig(t)
	_, err := (&reconcile.S3Objects{Bucket: "bkt"}).Endpoint(context.Background())
	require.ErrorContains(t, err, "destination cannot be told")
	custom := s3.New(s3.Options{Region: "us-east-1", EndpointResolverV2: customResolver{}})
	_, err = reconcile.ClientEndpoint(context.Background(), custom)
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
	require.Contains(t, out, `bucket "writers", data prefix "data", at https://s3.us-east-1.amazonaws.com`)
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
	require.Equal(t, publication.StorageTarget{Endpoint: "https://s3.us-east-1.amazonaws.com", Bucket: "writers", Prefix: "data"}, b.Target)
}
