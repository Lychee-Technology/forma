package publication

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/lychee-technology/forma/internal/textcollation"
)

// ErrStorageTargetInvalid reports a StorageTarget whose endpoint, bucket or
// path cannot uniquely identify one S3 destination (for example an empty
// bucket, embedded credentials, query/fragment, or an ambiguous path with
// duplicate or dot segments such as "/tenant//").
var ErrStorageTargetInvalid = errors.New("publication storage target is invalid or ambiguous")

// StorageTarget identifies where a deployment's writers stage objects and
// manifests: the bucket request URL the S3 client sends requests to (carrying
// endpoint host/port, custom path prefix and path-style vs virtual-hosted
// addressing), the bucket name, and the data prefix of the V2 layout.
type StorageTarget struct {
	Endpoint, Bucket, Prefix string
}

// Normalize validates t and returns its canonical form.
//
// Only the layout Prefix strips trailing slashes (matching manifestv2.Layout).
// Endpoint preserves its exact path without trimming arbitrary trailing
// slashes: only a bare root slash "/" is normalized to "", while duplicate
// slashes ("/tenant//"), dot segments ("/./", "/../"), query strings,
// fragments and userinfo are rejected so distinct SDK request destinations
// can never collapse into the same binding.
func (t StorageTarget) Normalize() (StorageTarget, error) {
	if t.Bucket == "" {
		return StorageTarget{}, fmt.Errorf("bucket is empty: %w", ErrStorageTargetInvalid)
	}
	ep, err := normalizeEndpointURL(t.Endpoint)
	if err != nil {
		return StorageTarget{}, err
	}
	return StorageTarget{
		Endpoint: ep,
		Bucket:   t.Bucket,
		Prefix:   strings.TrimRight(t.Prefix, "/"),
	}, nil
}

// NormalizeEndpoint validates and normalizes an endpoint bucket URL using the
// same rules as StorageTarget.Normalize.
func NormalizeEndpoint(raw string) (string, error) {
	return normalizeEndpointURL(raw)
}

func normalizeEndpointURL(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("parse storage endpoint %q: %v: %w", raw, err, ErrStorageTargetInvalid)
	}
	if u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("storage endpoint %q must include scheme and host: %w", raw, ErrStorageTargetInvalid)
	}
	if u.User != nil {
		return "", fmt.Errorf("storage endpoint must not carry userinfo: %w", ErrStorageTargetInvalid)
	}
	if u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return "", fmt.Errorf("storage endpoint must not carry query or fragment: %w", ErrStorageTargetInvalid)
	}
	if u.RawPath != "" && u.RawPath != u.Path {
		return "", fmt.Errorf("storage endpoint path %q contains escaped path characters: %w", u.RawPath, ErrStorageTargetInvalid)
	}
	if err := validateEndpointPath(u.Path); err != nil {
		return "", err
	}
	path := u.Path
	if path == "/" {
		path = ""
	}
	out := url.URL{
		Scheme: strings.ToLower(u.Scheme),
		Host:   strings.ToLower(u.Host),
		Path:   path,
	}
	return out.String(), nil
}

func validateEndpointPath(p string) error {
	if p == "" || p == "/" {
		return nil
	}
	if strings.Contains(p, "//") {
		return fmt.Errorf("storage endpoint path %q contains consecutive slashes: %w", p, ErrStorageTargetInvalid)
	}
	if strings.HasSuffix(p, "/") {
		return fmt.Errorf("storage endpoint path %q must not end with a trailing slash: %w", p, ErrStorageTargetInvalid)
	}
	for _, seg := range strings.Split(strings.TrimPrefix(p, "/"), "/") {
		if seg == "." || seg == ".." {
			return fmt.Errorf("storage endpoint path %q contains dot segment %q: %w", p, seg, ErrStorageTargetInvalid)
		}
	}
	return nil
}

// Same reports whether t and o are both valid and identify the exact same
// canonical storage target.
func (t StorageTarget) Same(o StorageTarget) bool {
	nt, err1 := t.Normalize()
	no, err2 := o.Normalize()
	if err1 != nil || err2 != nil {
		return false
	}
	return nt == no
}

func (t StorageTarget) String() string {
	display := t
	if nt, err := t.Normalize(); err == nil {
		display = nt
	}
	endpoint := display.Endpoint
	if endpoint == "" {
		endpoint = "the default endpoint"
	}
	return fmt.Sprintf("bucket %q, data prefix %q, at %s", display.Bucket, display.Prefix, endpoint)
}

// StorageBinding is the storage target a deployment's operator bound its
// publication database to, and when (epoch milliseconds, database clock).
type StorageBinding struct {
	Target  StorageTarget
	BoundAt int64
}

// storageDDL is the deployment's storage binding: at most one row (binding 1),
// naming the target this database's writers publish into. The V2 writers bind
// it in their first publication's transaction (BindStorageTx), and manifest-reconcile
// --bind-storage binds or checks it explicitly. init-db creates it with the
// other publication tables (Statements); --bind-storage also creates it, for
// a deployment whose init-db predates the table.
const storageDDL = `CREATE TABLE IF NOT EXISTS %s (
		binding     SMALLINT NOT NULL,
		endpoint    ` + textcollation.TextType + ` NOT NULL,
		bucket      ` + textcollation.TextType + ` NOT NULL,
		data_prefix ` + textcollation.TextType + ` NOT NULL,
		bound_at    BIGINT   NOT NULL,
		PRIMARY KEY (binding)
	)`

// StorageStatement answers the storage binding table's DDL for entityMain on
// engine.
func StorageStatement(engine forma.DatabaseEngine, entityMain string) string {
	return textcollation.ForEngine(engine, fmt.Sprintf(storageDDL, TablesFor(entityMain).Storage))
}

const (
	bindStorageSQL = `INSERT INTO %s (binding, endpoint, bucket, data_prefix, bound_at)
	VALUES (1, $1, $2, $3, ` + sqlgen.ServerTimeMillisSQL + `)
	ON CONFLICT (binding) DO NOTHING`
	readStorageSQL = `SELECT endpoint, bucket, data_prefix, bound_at FROM %s WHERE binding = 1`
)

// BindStorage validates target and binds the deployment to it unless a binding
// already exists, answering the binding the table holds afterwards.
func BindStorage(ctx context.Context, q Querier, t Tables, target StorageTarget) (StorageBinding, error) {
	c, err := target.Normalize()
	if err != nil {
		return StorageBinding{}, fmt.Errorf("bind the publication storage: %w", err)
	}
	if _, err := q.runner().exec(ctx, fmt.Sprintf(bindStorageSQL, t.Storage), c.Endpoint, c.Bucket, c.Prefix); err != nil {
		return StorageBinding{}, fmt.Errorf("bind the publication storage in %s to %s: %w", t.Storage, c, err)
	}
	b, ok, err := ReadStorage(ctx, q, t)
	if err != nil {
		return StorageBinding{}, fmt.Errorf("bind the publication storage in %s to %s: read it back: %w", t.Storage, c, err)
	}
	if !ok {
		return StorageBinding{}, fmt.Errorf("bind the publication storage in %s to %s: the binding is not there after its write", t.Storage, c)
	}
	return b, nil
}

// ErrStorageBoundElsewhere reports a deployment bound to another storage
// target than the one a writer publishes into or a run targets. The binding
// is kept: garbage collection deletes only under the bound target, so a
// writer publishing elsewhere would put objects where the collector never
// looks, and a collector bound elsewhere would judge the writers' objects
// against the wrong bucket.
var ErrStorageBoundElsewhere = errors.New("the deployment is bound to another storage target")

// ErrStorageTableMissing reports a deployment without the storage binding
// table: its init-db ran before the table existed, or, on Aurora DSQL, where
// init-db cannot run yet, nobody created it.
var ErrStorageTableMissing = errors.New("the publication storage binding table does not exist")

// storageTableAction is what an operator does about ErrStorageTableMissing.
const storageTableAction = "on PostgreSQL run init-db, which creates it; on Aurora DSQL, where init-db cannot run yet (#690), " +
	"create it with its CREATE TABLE statement (docs/aurora-dsql.md, \"Operating the CDC tools on DSQL\") or run " +
	"manifest-reconcile --bind-storage, which creates it and also binds the deployment to that run's target"

// CheckStorage checks target against the deployment's binding without
// binding anything: it answers true when the deployment is bound to target,
// and false when it has no binding yet. A binding to another target fails with
// ErrStorageBoundElsewhere, and a missing binding table with
// ErrStorageTableMissing. A writer runs it before it stages, and binds with
// BindStorageTx only in its publication transaction. Run it on a pool (DB).
func CheckStorage(ctx context.Context, q Querier, t Tables, target StorageTarget) (bool, error) {
	c, err := target.Normalize()
	if err != nil {
		return false, fmt.Errorf("check the publication storage binding: %w", err)
	}
	b, ok, err := readBinding(ctx, q, t)
	if err != nil {
		return false, fmt.Errorf("check the publication storage binding against %s: %w", c, err)
	}
	if !ok {
		return false, nil
	}
	if !b.Target.Same(c) {
		return false, boundElsewhere(b, c)
	}
	return true, nil
}

// BindStorageTx binds the deployment to target inside tx, a writer's
// publication transaction, unless a binding exists, and checks the binding tx
// reads afterwards: a binding to another target fails with
// ErrStorageBoundElsewhere, a missing binding table with
// ErrStorageTableMissing (which, on PostgreSQL, aborts tx). The binding is
// therefore durable exactly when tx commits, and so when the generation tx
// publishes commits: a publication that fails, is rejected or rolls back binds
// nothing.
//
// Two first publishers of an unbound deployment both insert the one row, so
// at most one of them commits it. On PostgreSQL the second waits for the
// first's transaction: if the first rolled back it inserts the row itself,
// otherwise it reads the first's row and checks it (or, at REPEATABLE READ,
// fails with SQLSTATE 40001). On Aurora DSQL the second fails at COMMIT with
// SQLSTATE 40001. A writer sends a publication that failed with 40001 again,
// and the new transaction reads the committed binding.
func BindStorageTx(ctx context.Context, tx Tx, t Tables, target StorageTarget) (StorageBinding, error) {
	c, err := target.Normalize()
	if err != nil {
		return StorageBinding{}, fmt.Errorf("bind the publication storage in the publication transaction: %w", err)
	}
	if _, err := tx.runner().exec(ctx, fmt.Sprintf(bindStorageSQL, t.Storage), c.Endpoint, c.Bucket, c.Prefix); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == sqlstateUndefinedTable {
			return StorageBinding{}, fmt.Errorf("bind the publication storage in %s to %s: %s: %w: %w",
				t.Storage, c, storageTableAction, ErrStorageTableMissing, err)
		}
		return StorageBinding{}, fmt.Errorf("bind the publication storage in %s to %s: %w", t.Storage, c, err)
	}
	b, ok, err := readBinding(ctx, tx, t)
	if err != nil {
		return StorageBinding{}, fmt.Errorf("bind the publication storage in %s to %s: read it back: %w", t.Storage, c, err)
	}
	if !ok {
		return StorageBinding{}, fmt.Errorf("bind the publication storage in %s to %s: the binding is not there after its write", t.Storage, c)
	}
	if !b.Target.Same(c) {
		return b, boundElsewhere(b, c)
	}
	return b, nil
}

func boundElsewhere(b StorageBinding, c StorageTarget) error {
	return fmt.Errorf("the deployment is bound to %s since %d, not to %s: %w", b.Target, b.BoundAt, c, ErrStorageBoundElsewhere)
}

const sqlstateUndefinedTable = "42P01"

// ReadStorage answers the deployment's storage binding, and false when no
// binding exists (no row, or the binding table has not been created yet).
//
// Run it on a pool (DB) when the binding table may not exist yet: on
// PostgreSQL, querying a missing table inside a Tx aborts the enclosing
// transaction with SQLSTATE 42P01, so ReadStorage only translates 42P01 to
// ok == false on a pool and returns the error, wrapping
// ErrStorageTableMissing, on a Tx.
func ReadStorage(ctx context.Context, q Querier, t Tables) (StorageBinding, bool, error) {
	b, ok, err := readBinding(ctx, q, t)
	if _, inTx := q.(Tx); !inTx && errors.Is(err, ErrStorageTableMissing) {
		return StorageBinding{}, false, nil
	}
	return b, ok, err
}

// readBinding is ReadStorage that fails a missing binding table with
// ErrStorageTableMissing on a pool too.
func readBinding(ctx context.Context, q Querier, t Tables) (StorageBinding, bool, error) {
	var b StorageBinding
	err := q.runner().queryRow(ctx, fmt.Sprintf(readStorageSQL, t.Storage)).
		Scan(&b.Target.Endpoint, &b.Target.Bucket, &b.Target.Prefix, &b.BoundAt)
	if errors.Is(err, sql.ErrNoRows) {
		return StorageBinding{}, false, nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == sqlstateUndefinedTable {
		return StorageBinding{}, false, fmt.Errorf("read the publication storage binding in %s: %s: %w: %w",
			t.Storage, storageTableAction, ErrStorageTableMissing, err)
	}
	if err != nil {
		return StorageBinding{}, false, fmt.Errorf("read the publication storage binding in %s: %w", t.Storage, err)
	}
	return b, true, nil
}
