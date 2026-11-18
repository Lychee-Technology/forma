package e2e_harness

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// TestManifestV2PutIsWriteOnceOnS3 pins the V2 write-once put against a real
// S3-compatible server (#898): S3 itself refuses a second put to a key under
// If-None-Match: *, whether the puts are sequential or race, and the refused
// put leaves the first object unchanged. The fs store's unit tests pin the
// same contract without a server.
func TestManifestV2PutIsWriteOnceOnS3(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping the S3 write-once check in -short mode")
	}
	ctx := context.Background()
	h := &TestHarness{}
	endpoint, err := h.StartS3(ctx)
	require.NoError(t, err)
	defer func() { _ = h.StopS3(ctx) }()
	client, err := NewS3Client(ctx, endpoint, "", RustFSAccessKey, RustFSSecretKey)
	require.NoError(t, err)
	const bucket = "manifest-v2"
	// RustFS answers 503 for a moment after its HTTP port first answers.
	require.Eventually(t, func() bool {
		_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
		return err == nil
	}, 60*time.Second, 500*time.Millisecond, "create bucket %s", bucket)
	st := &manifestv2.S3Store{Client: client, Bucket: bucket}
	layout := manifestv2.Layout{Prefix: "data"}

	key, err := layout.NewParquetKey(100, 1, manifestv2.TierDelta)
	require.NoError(t, err)
	require.NoError(t, st.Put(ctx, key, bytes.NewReader([]byte("first"))))
	require.ErrorIs(t, st.Put(ctx, key, bytes.NewReader([]byte("second"))), manifestv2.ErrKeyExists)
	got, err := st.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, "first", string(got))
	_, err = st.Get(ctx, key+".absent")
	require.ErrorIs(t, err, manifestv2.ErrObjectNotFound)

	race, err := layout.NewParquetKey(100, 1, manifestv2.TierBase)
	require.NoError(t, err)
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
	var won []int
	for i, err := range errs {
		if err == nil {
			won = append(won, i)
			continue
		}
		require.ErrorIs(t, err, manifestv2.ErrKeyExists)
	}
	require.Len(t, won, 1, "exactly one racing put writes")
	got, err = st.Get(ctx, race)
	require.NoError(t, err)
	require.Equal(t, []byte{byte('a' + won[0])}, got)

	requireManifestRoundTrip(t, st, layout, key)
}

// requireManifestRoundTrip stages a manifest listing file, refuses a second
// staging of it, and reads it back through the verifying cache.
func requireManifestRoundTrip(t *testing.T, st manifestv2.Store, layout manifestv2.Layout, file string) {
	t.Helper()
	ctx := context.Background()
	row := uuid.Must(uuid.NewV7())
	h := manifestv2.Header{SchemaID: 100, Epoch: 1, Generation: 1, Lineage: uuid.New(), Token: uuid.New(), Kind: publication.KindFlush}
	m, err := manifestv2.NewFlush(nil, h, []manifestv2.File{{
		Tier: manifestv2.TierDelta, Key: file, SizeBytes: 5, SHA256: manifestv2.Digest([]byte("first")),
		RowCount: 1, RowIDMin: row, RowIDMax: row, Columns: map[string]string{"ltbase_row_id": "UUID"},
	}})
	require.NoError(t, err)
	mkey, digest, err := manifestv2.PutManifest(ctx, st, layout, m)
	require.NoError(t, err)
	_, _, err = manifestv2.PutManifest(ctx, st, layout, m)
	require.ErrorIs(t, err, manifestv2.ErrKeyExists)

	cache, err := manifestv2.NewCache(st, 4)
	require.NoError(t, err)
	got, err := cache.Get(ctx, manifestv2.Ref{
		SchemaID: 100, Epoch: 1, Generation: 1, Token: h.Token, Lineage: h.Lineage,
		ManifestKey: mkey, ManifestSHA256: digest,
	})
	require.NoError(t, err)
	require.Equal(t, []string{file}, got.Keys())
}
