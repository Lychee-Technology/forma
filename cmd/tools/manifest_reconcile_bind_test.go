package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/reconcile"
	"github.com/lychee-technology/forma/internal/testdb"
)

// TestNewReconciler_WiresTheStorageTarget: the run's storage target, which
// it deletes in only when it is the deployment's bound one, is the
// configured endpoint, bucket and data prefix (R7-F1).
func TestNewReconciler_WiresTheStorageTarget(t *testing.T) {
	opts, err := parseReconcileFlags(reconcileArgs("--s3-endpoint", "http://rustfs:9000", "--data-prefix", "lake"))
	require.NoError(t, err)
	r, err := newReconciler(opts, &reconcile.S3Objects{Bucket: "bkt"}, &sql.DB{}, zap.NewNop())
	require.NoError(t, err)
	require.Equal(t, publication.StorageTarget{Endpoint: "http://rustfs:9000", Bucket: "bkt", Prefix: "lake"}, r.Storage)
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
	bind := func(bucket, prefix string) (string, error) {
		opts, err := parseReconcileFlags([]string{"--s3-bucket", bucket, "--schema-registry-table", "schema_registry",
			"--entity-main-table", entityMain, "--data-prefix", prefix, "--bind-storage"})
		require.NoError(t, err)
		var out bytes.Buffer
		err = bindReconcileStorage(ctx, db, opts, &out)
		return out.String(), err
	}

	out, err := bind("writers", "data")
	require.NoError(t, err)
	require.Contains(t, out, `bucket "writers", data prefix "data"`)
	_, err = bind("writers", "data/")
	require.NoError(t, err, "the same target, spelled with a trailing slash")
	_, err = bind("copy", "data")
	require.ErrorIs(t, err, errStorageBoundElsewhere)

	b, ok, err := publication.ReadStorage(ctx, publication.DB(db), publication.TablesFor(entityMain))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, publication.StorageTarget{Bucket: "writers", Prefix: "data"}, b.Target)
}
