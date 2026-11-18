package manifestv2

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/stretchr/testify/require"
)

// fakeS3 is an in-memory bucket that honours If-None-Match: * the way S3
// does, answering 412 PreconditionFailed when the key exists.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string][]byte
	puts    []*s3.PutObjectInput
	putErr  error
	getErr  error
	gets    int
}

func newFakeS3() *fakeS3 { return &fakeS3{objects: map[string][]byte{}} }

func (f *fakeS3) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts = append(f.puts, in)
	if f.putErr != nil {
		return nil, f.putErr
	}
	if _, ok := f.objects[*in.Key]; ok && aws.ToString(in.IfNoneMatch) == "*" {
		return nil, &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "At least one of the pre-conditions you specified did not hold"}
	}
	body, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	f.objects[*in.Key] = body
	return &s3.PutObjectOutput{}, nil
}

func (f *fakeS3) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets++
	if f.getErr != nil {
		return nil, f.getErr
	}
	body, ok := f.objects[*in.Key]
	if !ok {
		return nil, &types.NoSuchKey{}
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(body))}, nil
}

// contractLayouts are the prefixes every Store holds the keys of: the empty
// one (keys with a leading "/"), a doubled trailing slash, an inner doubled
// slash.
var contractLayouts = []Layout{{Prefix: "data"}, {Prefix: ""}, {Prefix: "data//"}, {Prefix: "a//b"}}

// storeContract is the write-once contract every Store keeps, for the keys
// of each contractLayouts prefix.
func storeContract(t *testing.T, newStore func() Store) {
	for _, l := range contractLayouts {
		t.Run("prefix "+strconv.Quote(l.Prefix), func(t *testing.T) { layoutContract(t, newStore(), l) })
	}
}

func layoutContract(t *testing.T, st Store, l Layout) {
	ctx := context.Background()
	key := l.ParquetKey(100, 1, TierDelta, uuid.MustParse("0190a000-0000-7000-8000-000000000001"))

	_, err := st.Get(ctx, key)
	require.ErrorIs(t, err, ErrObjectNotFound)

	require.NoError(t, st.Put(ctx, key, bytes.NewReader([]byte("first"))))
	got, err := st.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, "first", string(got))

	err = st.Put(ctx, key, bytes.NewReader([]byte("second")))
	require.ErrorIs(t, err, ErrKeyExists, "a second put to a key is refused, never an overwrite")
	got, err = st.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, "first", string(got), "the refused put leaves the first object unchanged")

	race := l.ManifestKey(100, 1, 1, testToken)
	const writers = 8
	errs := make([]error, writers)
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = st.Put(ctx, race, bytes.NewReader([]byte{byte('a' + i)}))
		}()
	}
	wg.Wait()
	winner := -1
	for i, err := range errs {
		if err == nil {
			require.Equal(t, -1, winner, "two racing puts both wrote")
			winner = i
			continue
		}
		require.ErrorIs(t, err, ErrKeyExists)
	}
	require.NotEqual(t, -1, winner)
	got, err = st.Get(ctx, race)
	require.NoError(t, err)
	require.Equal(t, []byte{byte('a' + winner)}, got)
}

func TestS3StoreKeepsTheWriteOnceContract(t *testing.T) {
	var clients []*fakeS3
	storeContract(t, func() Store {
		client := newFakeS3()
		clients = append(clients, client)
		return &S3Store{Client: client, Bucket: "bkt"}
	})
	require.Len(t, clients, len(contractLayouts))
	for _, client := range clients {
		for _, in := range client.puts {
			require.Equal(t, "*", aws.ToString(in.IfNoneMatch), "every put is conditional")
			require.Nil(t, in.IfMatch)
			require.Equal(t, "bkt", aws.ToString(in.Bucket))
		}
	}
}

func TestS3StoreTellsACollisionFromAnUnknownOutcome(t *testing.T) {
	ctx := context.Background()
	client := newFakeS3()
	st := &S3Store{Client: client, Bucket: "bkt"}

	client.putErr = &awshttp.ResponseError{ResponseError: &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusPreconditionFailed}},
		Err:      errors.New("412"),
	}}
	require.ErrorIs(t, st.Put(ctx, "k", bytes.NewReader(nil)), ErrKeyExists)

	for _, err := range []error{
		&smithy.GenericAPIError{Code: "ConditionalRequestConflict"},
		errors.New("connection reset by peer"),
	} {
		client.putErr = err
		got := st.Put(ctx, "k", bytes.NewReader(nil))
		require.ErrorIs(t, got, err)
		require.NotErrorIs(t, got, ErrKeyExists, "only a 412 proves the key holds another object")
	}

	client.getErr = &smithy.GenericAPIError{Code: "NoSuchBucket"}
	_, err := st.Get(ctx, "k")
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrObjectNotFound, "a missing bucket is a failed call, not a missing object")
	client.getErr = &smithy.GenericAPIError{Code: "NoSuchKey"}
	_, err = st.Get(ctx, "k")
	require.ErrorIs(t, err, ErrObjectNotFound)

	require.Error(t, (&S3Store{}).Put(ctx, "k", bytes.NewReader(nil)))
	_, err = (*S3Store)(nil).Get(ctx, "k")
	require.Error(t, err)
}

func TestFSStoreKeepsTheWriteOnceContract(t *testing.T) {
	var dirs []string
	storeContract(t, func() Store {
		dirs = append(dirs, t.TempDir())
		return &FSStore{Dir: dirs[len(dirs)-1]}
	})
	require.Len(t, dirs, len(contractLayouts))
	for _, dir := range dirs {
		var names []string
		require.NoError(t, filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
			if err == nil && !d.IsDir() {
				names = append(names, filepath.Base(path))
			}
			return err
		}))
		require.Len(t, names, 2, "no temporary file is left behind: %v", names)
	}
}

// TestFSStoreHoldsEveryKeyUnderItsOwnName pins that FSStore, like S3, holds
// any non-empty key, an empty prefix's leading "/" included, and never maps
// two keys to one file or a key outside its directory.
func TestFSStoreHoldsEveryKeyUnderItsOwnName(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st := &FSStore{Dir: dir}
	keys := []string{
		"/100/e1/base/x.parquet", "100/e1/base/x.parquet", "//100/e1/base/x.parquet",
		"a//b", "a/b", "a/./b", "a/%/b", "a/%25/b", "a/%2E/b",
		"../x", "a/../../x", "%2E%2E/x", "%/x", "%25/x",
		".", "y/..", ".x", "%2Ex", "y/.k", "y/k",
	}
	for i, key := range keys {
		require.NoError(t, st.Put(ctx, key, bytes.NewReader([]byte{byte(i)})), key)
	}
	for i, key := range keys {
		got, err := st.Get(ctx, key)
		require.NoError(t, err, key)
		require.Equal(t, []byte{byte(i)}, got, "key %q reads back its own object", key)
	}
	var files int
	require.NoError(t, filepath.WalkDir(dir, func(_ string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			files++
		}
		return err
	}))
	require.Equal(t, len(keys), files, "one file per key, all of them inside the store's directory")
	entries, err := os.ReadDir(filepath.Dir(dir))
	require.NoError(t, err)
	require.Len(t, entries, 1, "nothing is written beside the store's directory")

	require.Error(t, st.Put(ctx, "", bytes.NewReader([]byte("x"))))
	_, err = st.Get(ctx, "")
	require.Error(t, err)
	require.Error(t, (&FSStore{}).Put(ctx, "k", bytes.NewReader(nil)))
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error)       { return 0, errors.New("disk on fire") }
func (failingReader) Seek(int64, int) (int64, error) { return 0, nil }

func TestFSStoreLeavesNothingOfAFailedPut(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st := &FSStore{Dir: dir}
	require.ErrorContains(t, st.Put(ctx, "a/k", failingReader{}), "disk on fire")
	_, err := st.Get(ctx, "a/k")
	require.ErrorIs(t, err, ErrObjectNotFound)
	entries, err := os.ReadDir(filepath.Join(dir, "a"))
	require.NoError(t, err)
	require.Empty(t, entries)
	require.NoError(t, st.Put(ctx, "a/k", bytes.NewReader([]byte("x"))), "a failed put does not take the key")
}

func TestPutManifestStagesTheEncodedBytesUnderTheGenerationsKey(t *testing.T) {
	ctx := context.Background()
	st := &FSStore{Dir: t.TempDir()}
	m := testManifest(t)
	key, digest, err := PutManifest(ctx, st, testLayout, m)
	require.NoError(t, err)
	raw, ref := encoded(t, m)
	require.Equal(t, ref.ManifestKey, key)
	require.Equal(t, ref.ManifestSHA256, digest)
	stored, err := st.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, raw, stored)

	_, _, err = PutManifest(ctx, st, testLayout, m)
	require.ErrorIs(t, err, ErrKeyExists)
	m.Format = ""
	_, _, err = PutManifest(ctx, st, testLayout, m)
	require.Error(t, err)
}

// inLayout answers f with its key moved under l, the same object id.
func inLayout(t *testing.T, f File, l Layout) File {
	t.Helper()
	p, err := parseParquetKey(f.Key)
	require.NoError(t, err)
	f.Key = l.ParquetKey(p.schemaID, p.epoch, p.tier, p.id)
	return f
}

// TestPutManifestStagesNothingAReaderWouldRefuse pins that a manifest listing
// a file under another prefix, which Encode alone accepts, is refused before
// anything is written, so no commit can name a generation every reader
// refuses.
func TestPutManifestStagesNothingAReaderWouldRefuse(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st := &FSStore{Dir: dir}
	m := testManifest(t)
	m.Files[1] = inLayout(t, m.Files[1], Layout{Prefix: "other"})
	_, _, err := m.Encode()
	require.NoError(t, err, "each file's key is otherwise valid")

	_, _, err = PutManifest(ctx, st, testLayout, m)
	require.ErrorIs(t, err, publication.ErrManifestInvalid)
	require.ErrorContains(t, err, "is not under the manifest's prefix")
	var ce *ConsistencyError
	require.False(t, errors.As(err, &ce), "a refused staging is the writer's error, not the read path's")
	_, err = st.Get(ctx, testLayout.ManifestKey(m.SchemaID, m.Epoch, m.Generation, m.Token))
	require.ErrorIs(t, err, ErrObjectNotFound)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries, "the store is untouched")

	_, _, err = PutManifest(ctx, st, Layout{Prefix: "other"}, m)
	require.ErrorIs(t, err, publication.ErrManifestInvalid, "nor is it staged under the other prefix")
	entries, err = os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

// TestPutManifestAndFetchAgreeOnEveryPrefix stages a manifest whose files are
// under each contract prefix, the empty one included, and fetches it back
// from both stores.
func TestPutManifestAndFetchAgreeOnEveryPrefix(t *testing.T) {
	ctx := context.Background()
	for _, l := range contractLayouts {
		for name, st := range map[string]Store{"fs": &FSStore{Dir: t.TempDir()}, "s3": &S3Store{Client: newFakeS3(), Bucket: "bkt"}} {
			m := testManifest(t)
			for i := range m.Files {
				m.Files[i] = inLayout(t, m.Files[i], l)
			}
			key, digest, err := PutManifest(ctx, st, l, m)
			require.NoError(t, err, "%s store, prefix %q", name, l.Prefix)
			require.Equal(t, l.ManifestKey(100, 1, 7, testToken), key)
			got, err := Fetch(ctx, st, Ref{
				SchemaID: 100, Epoch: 1, Generation: 7, ParentGeneration: 6, Token: testToken, Lineage: testLineage,
				Kind: publication.KindFlush, ManifestKey: key, ManifestSHA256: digest,
			})
			require.NoError(t, err, "%s store, prefix %q", name, l.Prefix)
			require.Len(t, got.Files, 3)
		}
	}
}

func TestFetchVerifiesWithoutCaching(t *testing.T) {
	ctx := context.Background()
	st := &FSStore{Dir: t.TempDir()}
	m := testManifest(t)
	_, ref := encoded(t, m)
	_, err := Fetch(ctx, st, ref)
	require.ErrorIs(t, err, ErrManifestMissing)
	_, _, err = PutManifest(ctx, st, testLayout, m)
	require.NoError(t, err)
	got, err := Fetch(ctx, st, ref)
	require.NoError(t, err)
	require.Equal(t, int64(7), got.Generation)
	ref.Token = testLineage
	_, err = Fetch(ctx, st, ref)
	requireInvalid(t, err, ref)
}
