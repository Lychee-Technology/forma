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
// naming the target the operator declared this database's writers publish into
// (manifest-reconcile --bind-storage). Created by --bind-storage itself, not
// by init-db, so only the collector depends on it.
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
		return StorageBinding{}, err
	}
	if !ok {
		return StorageBinding{}, fmt.Errorf("bind the publication storage in %s to %s: the binding is not there after its write", t.Storage, c)
	}
	return b, nil
}

const sqlstateUndefinedTable = "42P01"

// ReadStorage answers the deployment's storage binding, and false when no
// binding exists (no row, or the binding table has not been created yet).
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
