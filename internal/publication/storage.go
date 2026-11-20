package publication

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/lychee-technology/forma/internal/textcollation"
)

// StorageTarget is where a deployment's writers stage their objects and
// manifests: the S3 endpoint (empty for the AWS default), the bucket and
// the data prefix of the V2 layout. The record and the history name keys,
// never a bucket, and every V2 deployment builds the same keys for a schema
// id, so neither they nor a manifest's bytes, which a copy of the bucket
// carries as well, tell which bucket is this database's.
type StorageTarget struct {
	Endpoint, Bucket, Prefix string
}

// canonical answers t with the spellings that name the same target made
// one: no trailing slash on the endpoint or the prefix.
func (t StorageTarget) canonical() StorageTarget {
	return StorageTarget{Endpoint: strings.TrimRight(t.Endpoint, "/"), Bucket: t.Bucket, Prefix: strings.TrimRight(t.Prefix, "/")}
}

// Same answers whether t and o name the same target.
func (t StorageTarget) Same(o StorageTarget) bool { return t.canonical() == o.canonical() }

func (t StorageTarget) String() string {
	c := t.canonical()
	endpoint := c.Endpoint
	if endpoint == "" {
		endpoint = "the default endpoint"
	}
	return fmt.Sprintf("bucket %q, data prefix %q, at %s", c.Bucket, c.Prefix, endpoint)
}

// StorageBinding is the storage target a deployment's operator bound its
// publication database to, and when (epoch milliseconds, database clock).
type StorageBinding struct {
	Target  StorageTarget
	BoundAt int64
}

// storageDDL is the deployment's storage binding: at most one row (binding
// 1), naming the target the operator declared this database's writers
// publish into (manifest-reconcile --bind-storage). Garbage collection
// deletes from S3 only under the bound target, so a collector configured
// with another bucket, endpoint or prefix (a copy of the bucket, another
// deployment's) deletes nothing there, whatever the bucket holds. It is
// kept apart from the record, written once and never by a writer, and
// created by the binding itself, not by init-db: only the collector reads it.
const storageDDL = `CREATE TABLE IF NOT EXISTS %s (
		binding     SMALLINT NOT NULL,
		endpoint    ` + textcollation.TextType + ` NOT NULL,
		bucket      ` + textcollation.TextType + ` NOT NULL,
		data_prefix ` + textcollation.TextType + ` NOT NULL,
		bound_at    BIGINT   NOT NULL,
		PRIMARY KEY (binding)
	)`

// StorageStatement answers the storage binding table's DDL for entityMain
// on engine.
func StorageStatement(engine forma.DatabaseEngine, entityMain string) string {
	return textcollation.ForEngine(engine, fmt.Sprintf(storageDDL, TablesFor(entityMain).Storage))
}

const (
	// bindStorageSQL writes the binding unless one exists: a binding is
	// changed only by an operator who deletes the row (docs/manifest-
	// reconcile.md, "Storage binding").
	bindStorageSQL = `INSERT INTO %s (binding, endpoint, bucket, data_prefix, bound_at)
	VALUES (1, $1, $2, $3, ` + sqlgen.ServerTimeMillisSQL + `)
	ON CONFLICT (binding) DO NOTHING`
	readStorageSQL = `SELECT endpoint, bucket, data_prefix, bound_at FROM %s WHERE binding = 1`
)

// BindStorage binds the deployment to target unless it is bound already,
// and answers the binding the table then holds: target's, or the one an
// earlier binding wrote, which the caller compares with target. The table
// must exist (StorageStatement).
func BindStorage(ctx context.Context, q Querier, t Tables, target StorageTarget) (StorageBinding, error) {
	c := target.canonical()
	if c.Bucket == "" {
		return StorageBinding{}, errors.New("bind the publication storage: the bucket is empty")
	}
	if _, err := q.runner().exec(ctx, fmt.Sprintf(bindStorageSQL, t.Storage), c.Endpoint, c.Bucket, c.Prefix); err != nil {
		return StorageBinding{}, fmt.Errorf("bind the publication storage in %s to %s: %w", t.Storage, c, err)
	}
	b, ok, err := ReadStorage(ctx, q, t)
	if err != nil {
		return StorageBinding{}, err
	}
	if !ok {
		return StorageBinding{}, fmt.Errorf("bind the publication storage in %s to %s: the binding is not there after its write", t.Storage, c)
	}
	return b, nil
}

// sqlstateUndefinedTable is PostgreSQL's (and Aurora DSQL's) SQLSTATE for a
// relation that does not exist.
const sqlstateUndefinedTable = "42P01"

// ReadStorage answers the deployment's storage binding, and false when it
// has none: no row, or no binding table yet, since the first binding
// creates it. Either way nothing binds the deployment to a bucket.
func ReadStorage(ctx context.Context, q Querier, t Tables) (StorageBinding, bool, error) {
	var b StorageBinding
	err := q.runner().queryRow(ctx, fmt.Sprintf(readStorageSQL, t.Storage)).
		Scan(&b.Target.Endpoint, &b.Target.Bucket, &b.Target.Prefix, &b.BoundAt)
	var pgErr *pgconn.PgError
	if errors.Is(err, sql.ErrNoRows) || (errors.As(err, &pgErr) && pgErr.Code == sqlstateUndefinedTable) {
		return StorageBinding{}, false, nil
	}
	if err != nil {
		return StorageBinding{}, false, fmt.Errorf("read the publication storage binding in %s: %w", t.Storage, err)
	}
	return b, true, nil
}
