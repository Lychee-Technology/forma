package production

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/reconcile"
)

// reconcileOptions are the time parameters the manifest-reconcile scenarios
// run under. They hold no federated pass across a retention grace, so they
// shorten the reader deadline R_max the grace must cover, to wait out an
// expiry in seconds; the in-flight reader scenario runs garbage collection
// while its pass is held, and the pass is released before any grace ends.
func reconcileOptions(gc bool) reconcile.Options {
	return reconcile.Options{
		GC: gc, RetentionGrace: 1500 * time.Millisecond, ReaderDeadline: time.Second,
		ClockMargin: 500 * time.Millisecond, OrphanAge: time.Second,
	}
}

// reconcileGrace is how long a scenario waits for a retired generation to
// expire under reconcileOptions.
const reconcileGrace = 2500 * time.Millisecond

// reconciler wires a reconcile.Reconciler over the Env the way the
// manifest-reconcile tool does: the S3 bucket, a database/sql handle on the
// Env's database, the schema lock cdc-flush and cdc-init take, the
// publication tables named after the Env's entity_main and its data prefix,
// restricted to schema. The handle closes when the test ends.
func (e *Env) reconciler(t *testing.T, schema SchemaRef, opts reconcile.Options) *reconcile.Reconciler {
	t.Helper()
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		e.CDC.PGHost, e.CDC.PGPort, e.CDC.PGUser, e.CDC.PGPassword, e.CDC.PGDB, e.CDC.PGSSLMode)
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		t.Fatalf("open the publication database for manifest-reconcile: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	locker, err := cdc.NewSchemaLocker(db, e.CDC.Engine, e.CDC.EntityMainTable)
	if err != nil {
		t.Fatalf("schema lock for manifest-reconcile: %v", err)
	}
	tables := publication.TablesFor(e.CDC.EntityMainTable)
	return &reconcile.Reconciler{
		DB:      db,
		Tables:  tables,
		Layout:  e.manifestLayout(),
		Objects: &reconcile.S3Objects{Client: e.Cluster.S3, Bucket: e.Cluster.Bucket},
		Locker:  locker,
		Schemas: &reconcile.Schemas{DB: db, Registry: e.Tables.SchemaRegistry, Tables: tables, Only: schema.ID},
		Logger:  e.logger,
		Opts:    opts,
	}
}
