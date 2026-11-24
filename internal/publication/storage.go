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
		return "", fmt.Errorf("the storage endpoint is not a URL (not shown: it may carry credentials): %w", ErrStorageTargetInvalid)
	}
	if u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("the storage endpoint must include scheme and host (not shown: it may carry credentials): %w", ErrStorageTargetInvalid)
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
// naming the target this database's writers publish into. The first
// publication binds it in its own transaction (BindStorageTx), unless an
// operator bound it ahead with manifest-reconcile --bind-storage. init-db
// creates it on PostgreSQL (Statements); on Aurora DSQL, where init-db cannot
// run yet (#690), --bind-storage creates it.
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
	if err := insertStorage(ctx, q, t, c); err != nil {
		return StorageBinding{}, err
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

func insertStorage(ctx context.Context, q Querier, t Tables, c StorageTarget) error {
	if _, err := q.runner().exec(ctx, fmt.Sprintf(bindStorageSQL, t.Storage), c.Endpoint, c.Bucket, c.Prefix); err != nil {
		return fmt.Errorf("bind the publication storage in %s to %s: %w", t.Storage, c, err)
	}
	return nil
}

const sqlstateUndefinedTable = "42P01"

// ReadStorage answers the deployment's storage binding, and false when no
// binding exists (no row, or the binding table has not been created yet).
//
// Run it on a pool (DB) when the binding table may not exist yet: on
// PostgreSQL, querying a missing table inside a Tx aborts the enclosing
// transaction with SQLSTATE 42P01, so ReadStorage only translates 42P01 to
// ok == false on a pool and returns the error on a Tx.
func ReadStorage(ctx context.Context, q Querier, t Tables) (StorageBinding, bool, error) {
	var b StorageBinding
	err := q.runner().queryRow(ctx, fmt.Sprintf(readStorageSQL, t.Storage)).
		Scan(&b.Target.Endpoint, &b.Target.Bucket, &b.Target.Prefix, &b.BoundAt)
	if errors.Is(err, sql.ErrNoRows) {
		return StorageBinding{}, false, nil
	}
	var pgErr *pgconn.PgError
	if _, inTx := q.(Tx); !inTx && errors.As(err, &pgErr) && pgErr.Code == sqlstateUndefinedTable {
		return StorageBinding{}, false, nil
	}
	if err != nil {
		return StorageBinding{}, false, fmt.Errorf("read the publication storage binding in %s: %w", t.Storage, err)
	}
	return b, true, nil
}

// ErrStorageBoundElsewhere reports a writer whose storage target is not the
// one the deployment is bound to: garbage collection runs against the bound
// target, so a generation published elsewhere would be one it never sees.
// The writer publishes nothing, and the binding is kept.
var ErrStorageBoundElsewhere = errors.New("the deployment is bound to another storage target")

// ErrStorageTableMissing reports a deployment without the storage binding
// table. A writer never creates it: a publication transaction runs no DDL.
var ErrStorageTableMissing = errors.New("the publication storage binding table does not exist")

// CheckStorage is a writer's check before it stages anything: it fails with
// ErrStorageBoundElsewhere when the deployment is bound to another target than
// target, and with ErrStorageTableMissing when the binding table is missing.
// An unbound deployment passes, and nothing is bound here: only a publication
// transaction binds (BindStorageTx). Run it on a pool (DB).
func CheckStorage(ctx context.Context, q Querier, t Tables, target StorageTarget, engine forma.DatabaseEngine) error {
	var b StorageBinding
	err := q.runner().queryRow(ctx, fmt.Sprintf(readStorageSQL, t.Storage)).
		Scan(&b.Target.Endpoint, &b.Target.Bucket, &b.Target.Prefix, &b.BoundAt)
	var pgErr *pgconn.PgError
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case errors.As(err, &pgErr) && pgErr.Code == sqlstateUndefinedTable:
		return missingStorageTable(t, engine)
	case err != nil:
		return fmt.Errorf("read the publication storage binding in %s: %w", t.Storage, err)
	}
	return sameStorage(b, target)
}

// BindStorageTx binds an unbound deployment to target in the publication
// transaction tx, so the binding commits exactly when the generation does:
// a publication that rolls back, fails or never commits binds nothing. A
// deployment bound to another target fails tx with ErrStorageBoundElsewhere
// before it moves anything. A bound deployment is only read: concurrent
// publishers of a bound deployment never write the binding's row, so it is no
// write conflict between them on Aurora DSQL.
//
// Concurrent first publishers serialize on the row. On PostgreSQL a second
// insert waits for the first's transaction, and the read after it, a new
// statement under READ COMMITTED, sees the committed binding, or none if the
// first rolled back and this insert bound. At REPEATABLE READ (MiniStack's
// PostgreSQL) the waiting insert fails with 40001 once the first commits. On
// DSQL the loser's COMMIT fails with 40001. A writer sends a publication
// failing with 40001 again (Conflict), and the resend reads the committed
// binding.
func BindStorageTx(ctx context.Context, tx Tx, t Tables, target StorageTarget) error {
	b, ok, err := ReadStorage(ctx, tx, t)
	if err != nil {
		return fmt.Errorf("check the storage binding in the publication transaction: %w", err)
	}
	if ok {
		return sameStorage(b, target)
	}
	c, err := target.Normalize()
	if err != nil {
		return fmt.Errorf("bind the publication storage in the publication transaction: %w", err)
	}
	if err := insertStorage(ctx, tx, t, c); err != nil {
		return fmt.Errorf("bind the publication storage in the publication transaction: %w", err)
	}
	if b, ok, err = ReadStorage(ctx, tx, t); err != nil {
		return fmt.Errorf("check the storage binding in the publication transaction: %w", err)
	}
	if !ok {
		return fmt.Errorf("bind the publication storage in %s to %s: the binding is not there after its write", t.Storage, c)
	}
	return sameStorage(b, target)
}

func sameStorage(b StorageBinding, target StorageTarget) error {
	if b.Target.Same(target) {
		return nil
	}
	return fmt.Errorf("this writer publishes into %s, and the deployment is bound to %s since %d; "+
		"check the writer's S3 flags and environment (AWS_ENDPOINT_URL_S3, AWS_ENDPOINT_URL, region, path-style addressing, "+
		"FIPS and dual-stack settings), bucket and data prefix against the binding; "+
		"if the binding itself is wrong, see docs/manifest-reconcile.md, \"Changing the binding\": %w",
		target, b.Target, b.BoundAt, ErrStorageBoundElsewhere)
}

func missingStorageTable(t Tables, engine forma.DatabaseEngine) error {
	remedy := "rerun init-db, which creates it with the other publication tables"
	if engine.Resolved() == forma.EngineAuroraDSQL {
		remedy = "init-db cannot create Forma's tables on Aurora DSQL yet (#690): run manifest-reconcile --bind-storage " +
			"with the writers' S3 flags and environment, which creates it and also binds that target, or create it with " +
			"the statement in docs/manifest-reconcile.md, \"Storage binding\""
	}
	return fmt.Errorf("%s: %s, before the writers publish: %w", t.Storage, remedy, ErrStorageTableMissing)
}
