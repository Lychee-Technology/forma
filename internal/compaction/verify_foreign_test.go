package compaction

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/manifest"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// The #417 ruling: a source in another bucket is not a supported rewrite
// input. The compactor cannot verify, stat or delete it through its own
// client, yet MergeToTmp would fold its bytes into a new base in THIS bucket
// and spliceManifest would drop its listed name — so the rewrite refuses,
// stamped or not, with a matchable error and one ERROR naming the path.
func TestRejectForeignSources_ForeignPathRefused(t *testing.T) {
	core, logs := observer.New(zap.ErrorLevel)
	c, s3c := newVerifyFixture(zap.New(core))

	err := c.rejectForeignSources(1, []manifest.FileEntry{
		{Tier: "base", Path: "p/1/aaa.parquet"},
		{Tier: "delta", Path: "s3://other-bkt/p/1/foreign.parquet"}, // unstamped: still refused
	})
	require.ErrorIs(t, err, ErrForeignSource)
	require.ErrorContains(t, err, "s3://other-bkt/p/1/foreign.parquet", "the error must name the object")
	require.Empty(t, s3c.gets, "a scope check reads nothing")

	entries := logs.All()
	require.Len(t, entries, 1)
	require.Equal(t, "s3://other-bkt/p/1/foreign.parquet", entries[0].ContextMap()["path"])
	require.Equal(t, int16(1), entries[0].ContextMap()["schema_id"])
}

// The scope check is not a checksum check: neither the --skip-input-checksum-
// verify escape hatch nor an unwired reader may let a foreign source through.
func TestRejectForeignSources_IgnoresChecksumOptOut(t *testing.T) {
	c, _ := newVerifyFixture(zap.NewNop())
	c.Config.SkipInputChecksumVerify = true
	c.ObjectReader = nil

	err := c.rejectForeignSources(1, []manifest.FileEntry{
		stampedEntry("s3://other-bkt/p/1/foreign.parquet", sourcePayload),
	})
	require.ErrorIs(t, err, ErrForeignSource)
}

// Both spellings of an in-bucket path pass: the bucket-relative key every
// writer produces, and an absolute URI naming this compactor's own bucket.
func TestRejectForeignSources_OwnBucketPathsPass(t *testing.T) {
	c, _ := newVerifyFixture(zap.NewNop())

	err := c.rejectForeignSources(1, []manifest.FileEntry{
		{Tier: "base", Path: "p/1/aaa.parquet"},
		{Tier: "base", Path: "/p/1/bbb.parquet"},
		{Tier: "delta", Path: "s3://bkt/p/1/ddd.parquet"},
	})
	require.NoError(t, err)
}

// A prefix collision is not membership: s3://bktX/... is a different bucket.
func TestRejectForeignSources_BucketPrefixCollisionRefused(t *testing.T) {
	c, _ := newVerifyFixture(zap.NewNop())

	err := c.rejectForeignSources(1, []manifest.FileEntry{
		{Tier: "base", Path: "s3://bktX/p/1/aaa.parquet"},
	})
	require.ErrorIs(t, err, ErrForeignSource)
}

// A path that resolves to no key at all is out of scope too: the compactor
// could not merge, hash or delete "s3://bkt/" or "/", so it refuses rather
// than handing an empty key downstream.
func TestRejectForeignSources_EmptyKeyRefused(t *testing.T) {
	c, _ := newVerifyFixture(zap.NewNop())

	for _, path := range []string{"s3://bkt/", "/", ""} {
		err := c.rejectForeignSources(1, []manifest.FileEntry{{Tier: "base", Path: path}})
		require.ErrorIs(t, err, ErrForeignSource, "path %q", path)
		require.ErrorContains(t, err, fmt.Sprintf("rewrite source %q", path), "the refusal quotes the path so an empty one stays legible")
	}
}

// deleteObjects shares bucketRelativeKey: an own-bucket URI is deleted by its
// relative key, a foreign or empty one is skipped with a WARN, never deleted.
func TestDeleteObjects_UsesBucketRelativeKey(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	c, s3c := newVerifyFixture(zap.New(core))
	c.S3 = s3c

	c.deleteObjects(context.Background(), 1, []string{
		"s3://bkt/p/1/aaa.parquet",
		"/p/1/bbb.parquet",
		"s3://other-bkt/p/1/ccc.parquet",
		"s3://bkt/",
	})
	require.Equal(t, []string{"p/1/aaa.parquet", "p/1/bbb.parquet"}, s3c.deletes)
	require.Len(t, logs.All(), 2, "one WARN per skipped path")
	require.Equal(t, "s3://other-bkt/p/1/ccc.parquet", logs.All()[0].ContextMap()["path"])
	require.Equal(t, "s3://bkt/", logs.All()[1].ContextMap()["path"])
}

// foreignSourceManifest is rewrite-eligible (10% dirty ratio, sub-threshold
// delta bytes) but lists its delta in another bucket, unstamped — the shape
// the pre-#417 gate let straight through to MergeToTmp.
func foreignSourceManifest() *manifest.Manifest {
	return &manifest.Manifest{
		SchemaID:    1,
		Version:     3,
		UpdatedAtMs: time.Now().Add(-time.Minute).UnixMilli(),
		Files: []manifest.FileEntry{
			{Tier: "base", Path: "p/1/aaa_bbb.parquet", RowCount: 1000, SizeBytes: 10},
			{Tier: "delta", Path: "s3://other-bkt/p/1/ddd.parquet", RowCount: 100, SizeBytes: 5},
		},
	}
}

// End to end through RunOnce: the refusal lands before anything is merged,
// promoted or committed, even with the checksum gate opted out, and it is
// terminal for the pass (one LoadManifest, no retry under backoff).
func TestCompactor_Rewrite_ForeignSourceRefusesTheMerge(t *testing.T) {
	provider := &loadCountingProvider{mockProvider: &mockProvider{manifest: foreignSourceManifest(), etag: "etag-1"}}
	s3c := &fakeObjectS3{size: 4096}
	merger := &fakeMerger{stats: MergeStats{RowsIn: 1100, RowsOut: 950}}
	c := newRewireableCompactor(provider, merger, s3c)
	c.Config.SkipInputChecksumVerify = true

	_, err := c.RunOnce(context.Background())
	require.ErrorIs(t, err, ErrForeignSource)
	require.ErrorContains(t, err, "s3://other-bkt/p/1/ddd.parquet")

	require.Empty(t, merger.sources, "the merge must never read a foreign source")
	require.Empty(t, s3c.copies, "nothing may be promoted")
	require.Empty(t, s3c.deletes, "nothing may be deleted")
	require.Equal(t, int64(3), provider.mockProvider.manifest.Version, "the manifest must not be swapped")
	require.Equal(t, 1, provider.loads, "a foreign source is refused once, not re-probed under backoff")
}
