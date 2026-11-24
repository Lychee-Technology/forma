package cdcv2

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/s3endpoint"
)

// bucket is a path-style S3 endpoint holding one bucket in memory: a
// conditional PUT writes a key once, and a GET reads it. While redirect is
// set, it answers every request with a 307 to other instead.
type bucket struct {
	*httptest.Server
	mu       sync.Mutex
	objects  map[string][]byte
	redirect atomic.Bool
	other    *httptest.Server
	reached  atomic.Int32 // requests the other route was sent
}

func newBucket(t *testing.T) *bucket {
	t.Helper()
	b := &bucket{objects: map[string][]byte{}}
	b.other = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		b.reached.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(b.other.Close)
	b.Server = httptest.NewServer(http.HandlerFunc(b.serve))
	t.Cleanup(b.Close)
	return b
}

func (b *bucket) serve(w http.ResponseWriter, r *http.Request) {
	if b.redirect.Load() {
		w.Header().Set("Location", b.other.URL+r.URL.RequestURI())
		w.WriteHeader(http.StatusTemporaryRedirect)
		return
	}
	key := strings.TrimPrefix(r.URL.Path, "/lake/")
	b.mu.Lock()
	defer b.mu.Unlock()
	switch r.Method {
	case http.MethodPut:
		if _, ok := b.objects[key]; ok && r.Header.Get("If-None-Match") == "*" {
			w.WriteHeader(http.StatusPreconditionFailed)
			_, _ = io.WriteString(w, `<Error><Code>PreconditionFailed</Code></Error>`)
			return
		}
		raw, _ := io.ReadAll(r.Body)
		b.objects[key] = raw
		w.Header().Set("ETag", `"etag"`)
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		raw, ok := b.objects[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `<Error><Code>NoSuchKey</Code></Error>`)
			return
		}
		_, _ = w.Write(raw)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// client answers an SDK client of the bucket, following redirects unless
// bound.
func (b *bucket) client(t *testing.T, bound bool) *s3.Client {
	t.Helper()
	c := s3.New(s3.Options{
		Region: "us-east-1", BaseEndpoint: aws.String(b.URL), UsePathStyle: true, Retryer: aws.NopRetryer{},
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
		}),
	})
	if !bound {
		return c
	}
	c, err := s3endpoint.WithoutRedirects(c)
	require.NoError(t, err)
	return c
}

// TestNewDerivesTheStorageTargetFromItsStore: a writer over an S3 store over
// an SDK client binds the bucket URL that client sends requests to, its
// bucket and the layout's prefix; one whose client may follow a redirect is
// refused; a store that is not an S3 store over an SDK client names none.
func TestNewDerivesTheStorageTargetFromItsStore(t *testing.T) {
	b := newBucket(t)
	db, err := sql.Open("pgx", "postgres://nowhere.invalid/forma") // New connects to nothing
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	cfg := func(store manifestv2.Store) Config {
		cache, err := manifestv2.NewCache(store, 4)
		require.NoError(t, err)
		return Config{DB: db, EntityMain: "entity_main", Layout: manifestv2.Layout{Prefix: "data/"}, Store: store, Cache: cache}
	}
	w, err := New(cfg(&manifestv2.S3Store{Client: b.client(t, true), Bucket: "lake"}))
	require.NoError(t, err)
	target, ok := w.StorageTarget()
	require.True(t, ok)
	require.Equal(t, publication.StorageTarget{Endpoint: b.URL + "/lake", Bucket: "lake", Prefix: "data"}, target)

	_, err = New(cfg(&manifestv2.S3Store{Client: b.client(t, false), Bucket: "lake"}))
	require.ErrorIs(t, err, ErrRedirectsFollowed)

	for name, store := range map[string]manifestv2.Store{
		"a local directory":   &manifestv2.FSStore{Dir: t.TempDir()},
		"another client type": &manifestv2.S3Store{Client: ownClient{}, Bucket: "lake"},
	} {
		w, err := New(cfg(store))
		require.NoError(t, err, name)
		_, ok := w.StorageTarget()
		require.False(t, ok, name)
	}
}

type ownClient struct{ manifestv2.S3Client }

// s3Stager stages a flush's delta object through the writer's own store.
type s3Stager struct{ store manifestv2.Store }

func (s s3Stager) StageDelta(ctx context.Context, req DeltaRequest) ([]manifestv2.File, error) {
	key, err := req.Layout.NewParquetKey(req.SchemaID, req.Epoch, manifestv2.TierDelta)
	if err != nil {
		return nil, err
	}
	copies := make([]rowCopy, len(req.Entries))
	for i, e := range req.Entries {
		copies[i] = rowCopy{Row: e.RowID, Version: e.Version}
	}
	raw, err := json.Marshal(copies)
	if err != nil {
		return nil, err
	}
	if err := s.store.Put(ctx, key, bytes.NewReader(raw)); err != nil {
		return nil, err
	}
	return []manifestv2.File{{
		Tier: manifestv2.TierDelta, Key: key, SizeBytes: int64(len(raw)), SHA256: manifestv2.Digest(raw), RowCount: int64(len(copies)),
		RowIDMin: req.Entries[0].RowID, RowIDMax: req.Entries[0].RowID,
		Columns: map[string]string{"ltbase_row_id": "UUID", "ltbase_updated_at": "BIGINT", "ltbase_deleted_at": "BIGINT"},
	}}, nil
}

// TestAWriterOverS3BindsWhereItsPutsGoAndFollowsNoRedirect runs the real
// writer path over an S3 store: a first flush whose PUT is redirected fails
// with ErrRedirectRefused, reaches no other route, publishes nothing and
// binds nothing; the next, unredirected, binds the bucket URL its requests go
// to.
func TestAWriterOverS3BindsWhereItsPutsGoAndFollowsNoRedirect(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	before := f.register(t)
	f.put(t, uuid.Must(uuid.NewV7()), 1000, false)
	b := newBucket(t)
	store := &manifestv2.S3Store{Client: b.client(t, true), Bucket: "lake"}
	w := f.writer(t, func(c *Config) {
		c.Store = store
		cache, err := manifestv2.NewCache(store, 4)
		require.NoError(t, err)
		c.Cache = cache
	})

	b.redirect.Store(true)
	_, err := w.Flush(ctx, schemaID, 1, s3Stager{store})
	require.ErrorIs(t, err, s3endpoint.ErrRedirectRefused)
	require.Zero(t, b.reached.Load(), "no request reached the other route")
	require.Equal(t, before, f.record(t), "a redirected PUT publishes nothing")
	f.requireUnbound(t, "a redirected first publication")

	b.redirect.Store(false)
	rep, err := w.Flush(ctx, schemaID, 1, s3Stager{store})
	require.NoError(t, err)
	require.Len(t, rep.Generations, 1)
	f.requireBound(t, publication.StorageTarget{Endpoint: b.URL + "/lake", Bucket: "lake", Prefix: "data"})
}
