package manifestv2

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/stretchr/testify/require"
)

// stored puts raw under ref's key in a fresh fake bucket and answers a cache
// over it.
func stored(t *testing.T, raw []byte, ref Ref, capacity int) (*Cache, *fakeS3) {
	t.Helper()
	client := newFakeS3()
	client.objects[ref.ManifestKey] = raw
	c, err := NewCache(&S3Store{Client: client, Bucket: "bkt"}, capacity)
	require.NoError(t, err)
	return c, client
}

func TestCacheAnswersAVerifiedManifestAndKeepsIt(t *testing.T) {
	ctx := context.Background()
	raw, ref := encoded(t, testManifest(t))
	c, client := stored(t, raw, ref, 4)

	m, err := c.Get(ctx, ref)
	require.NoError(t, err)
	want, err := Verify(raw, ref)
	require.NoError(t, err)
	require.Equal(t, want, m)

	again, err := c.Get(ctx, ref)
	require.NoError(t, err)
	require.Same(t, m, again)
	require.Equal(t, 1, client.gets, "an object never changes, so the cache never fetches it again")
	require.Equal(t, 1, c.Len())
}

func TestCacheRefusesACorruptedManifestAndNeverCachesTheRefusal(t *testing.T) {
	ctx := context.Background()
	raw, ref := encoded(t, testManifest(t))
	corrupted := bytes.Replace(raw, []byte(`"size_bytes":1024`), []byte(`"size_bytes":1025`), 1)
	c, client := stored(t, corrupted, ref, 4)

	for range 2 {
		_, err := c.Get(ctx, ref)
		requireInvalid(t, err, ref)
	}
	require.Equal(t, 2, client.gets, "a refused fetch is fetched again, not answered from the cache")
	require.Zero(t, c.Len())

	// Were the object repaired, the next Get would read it.
	client.objects[ref.ManifestKey] = raw
	_, err := c.Get(ctx, ref)
	require.NoError(t, err)
}

func TestCacheRefusesAMisidentifiedReferenceOnAHit(t *testing.T) {
	ctx := context.Background()
	raw, ref := encoded(t, testManifest(t))
	c, client := stored(t, raw, ref, 4)
	_, err := c.Get(ctx, ref)
	require.NoError(t, err)

	for name, edit := range map[string]func(*Ref){
		"another digest":     func(r *Ref) { r.ManifestSHA256 = Digest([]byte("x")) },
		"another token":      func(r *Ref) { r.Token = uuid.New() },
		"another generation": func(r *Ref) { r.Generation = 8 },
		"another lineage":    func(r *Ref) { r.Lineage = uuid.New() },
		"another kind":       func(r *Ref) { r.Kind = publication.KindInit },
	} {
		r := ref
		edit(&r)
		_, err := c.Get(ctx, r)
		requireInvalid(t, err, r)
		require.Equal(t, 1, client.gets, name)
	}
	m, err := c.Get(ctx, ref)
	require.NoError(t, err, "a refused reference leaves the verified entry in place")
	require.NotNil(t, m)
}

func TestCacheTellsAMissingManifestFromAFailedStoreCall(t *testing.T) {
	ctx := context.Background()
	raw, ref := encoded(t, testManifest(t))
	c, client := stored(t, raw, ref, 4)
	delete(client.objects, ref.ManifestKey)

	_, err := c.Get(ctx, ref)
	require.ErrorIs(t, err, ErrManifestMissing)
	require.ErrorIs(t, err, ErrObjectNotFound)
	var ce *ConsistencyError
	require.ErrorAs(t, err, &ce)
	require.NotErrorIs(t, err, publication.ErrManifestInvalid)

	outage := errors.New("503 slow down")
	client.getErr = outage
	_, err = c.Get(ctx, ref)
	require.ErrorIs(t, err, outage)
	require.False(t, errors.As(err, &ce), "a failed store call is not a consistency error")

	client.getErr = nil
	client.objects[ref.ManifestKey] = raw
	_, err = c.Get(ctx, ref)
	require.NoError(t, err)
	require.Equal(t, 3, client.gets)
}

// gatedStore holds every Get until release is closed, to line up concurrent
// fetches of one key.
type gatedStore struct {
	Store
	release chan struct{}
	mu      sync.Mutex
	gets    int
}

func (g *gatedStore) Get(ctx context.Context, key string) ([]byte, error) {
	g.mu.Lock()
	g.gets++
	g.mu.Unlock()
	<-g.release
	return g.Store.Get(ctx, key)
}

func TestCacheFetchesAKeyOnceForConcurrentReaders(t *testing.T) {
	ctx := context.Background()
	raw, ref := encoded(t, testManifest(t))
	client := newFakeS3()
	client.objects[ref.ManifestKey] = raw
	st := &gatedStore{Store: &S3Store{Client: client, Bucket: "bkt"}, release: make(chan struct{})}
	c, err := NewCache(st, 4)
	require.NoError(t, err)

	const readers = 16
	got := make([]*Manifest, readers)
	errs := make([]error, readers)
	var wg sync.WaitGroup
	for i := range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got[i], errs[i] = c.Get(ctx, ref)
		}()
	}
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.inflight) == 1
	}, time.Second, time.Millisecond)
	close(st.release)
	wg.Wait()
	for i := range readers {
		require.NoError(t, errs[i])
		require.Same(t, got[0], got[i])
	}
	require.Equal(t, 1, st.gets)

	// A waiter gives up with its own context, not the fetch's.
	other := testManifest(t)
	other.Generation = 8
	raw8, ref8 := encoded(t, other)
	client.objects[ref8.ManifestKey] = raw8
	st.release = make(chan struct{})
	done := make(chan error, 1)
	go func() { _, err := c.Get(ctx, ref8); done <- err }()
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.inflight) == 1
	}, time.Second, time.Millisecond)
	short, cancel := context.WithCancel(ctx)
	cancel()
	_, err = c.Get(short, ref8)
	require.ErrorIs(t, err, context.Canceled)
	close(st.release)
	require.NoError(t, <-done)
}

func TestCacheEvictsTheLeastRecentlyUsedBeyondItsCapacity(t *testing.T) {
	ctx := context.Background()
	client := newFakeS3()
	c, err := NewCache(&S3Store{Client: client, Bucket: "bkt"}, 2)
	require.NoError(t, err)
	refs := make([]Ref, 3)
	for i := range refs {
		m := testManifest(t)
		m.Generation = int64(7 + i)
		var raw []byte
		raw, refs[i] = encoded(t, m)
		client.objects[refs[i].ManifestKey] = raw
	}
	for _, r := range []Ref{refs[0], refs[1], refs[0], refs[2]} {
		_, err := c.Get(ctx, r)
		require.NoError(t, err)
	}
	require.Equal(t, 2, c.Len())
	require.Equal(t, 3, client.gets)
	_, err = c.Get(ctx, refs[0])
	require.NoError(t, err)
	require.Equal(t, 3, client.gets, "the most recently used entry stays")
	_, err = c.Get(ctx, refs[1])
	require.NoError(t, err)
	require.Equal(t, 4, client.gets, "the least recently used entry was evicted, and is fetched again")

	_, err = NewCache(nil, 1)
	require.Error(t, err)
	_, err = NewCache(&FSStore{Dir: t.TempDir()}, 0)
	require.Error(t, err)
}
