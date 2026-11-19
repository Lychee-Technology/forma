package compaction

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/manifest"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestManifestPathContract_ConsumersAgreeOnKey pins the #516 contract for the
// V1 consumers that remain until #890 and #891: a manifest entry's relative
// path IS the bucket-relative key, verbatim. One leading-slash entry (what
// cdc.Build*Path emits under an empty prefix) must resolve to the same key
// through manifest-reconcile (cdc.NormalizeObjectKey, which reconcile.normalizeKey
// delegates to) and compaction (objectURI / bucketRelativeKey). A plain
// relative key and two own-bucket absolute URIs ride along as controls.
func TestManifestPathContract_ConsumersAgreeOnKey(t *testing.T) {
	cases := []struct {
		path    string
		wantKey string
		wantURI string
	}{
		{"/1/lead.parquet", "/1/lead.parquet", "s3://bkt//1/lead.parquet"},
		{"p/1/plain.parquet", "p/1/plain.parquet", "s3://bkt/p/1/plain.parquet"},
		{"s3://bkt//1/abs.parquet", "/1/abs.parquet", "s3://bkt//1/abs.parquet"},
		// An own-bucket URI whose key is a bare "/" is a valid S3 key and
		// passes through like any absolute entry; only the relative "/" is
		// refused as empty (TestRejectForeignSources_EmptyKeyRefused).
		{"s3://bkt//", "/", "s3://bkt//"},
	}

	c, _ := newVerifyFixture(zap.NewNop())
	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			key, ok := cdc.NormalizeObjectKey("bkt", tc.path)
			require.True(t, ok)
			require.Equal(t, tc.wantKey, key, "manifest-reconcile (cdc.NormalizeObjectKey)")

			require.Equal(t, tc.wantURI, c.objectURI(tc.path), "compaction merge (objectURI)")
			key, ok = c.bucketRelativeKey(tc.path)
			require.True(t, ok)
			require.Equal(t, tc.wantKey, key, "compaction gate/delete (bucketRelativeKey)")
		})
	}
}

// The checksum gate hashes a leading-slash relative source at its verbatim
// key — the object the flush wrote and the reader scans — not at a
// slash-trimmed sibling that was never in the manifest (#516).
func TestVerifySourceChecksums_LeadingSlashRelativeKeyVerbatim(t *testing.T) {
	c, s3c := newVerifyFixture(zap.NewNop())
	s3c.putObject("/1/aaa.parquet", sourcePayload)

	err := c.verifySourceChecksums(context.Background(), 1, []manifest.FileEntry{
		stampedEntry("/1/aaa.parquet", sourcePayload),
	})
	require.NoError(t, err)
	require.Equal(t, []string{"/1/aaa.parquet"}, s3c.gets)
}

// A full rewrite over leading-slash sources (an empty-prefix flush's
// manifest) hashes and merges the listed keys verbatim: the gate GETs
// "/1/..." and DuckDB is handed s3://bkt//1/..., the same URI
// QuerySource.Paths renders for the read path. The merged base is minted
// under the compactor's own DataPrefix as usual.
func TestCompactor_Rewrite_LeadingSlashSourcesResolveVerbatim(t *testing.T) {
	provider := &mockProvider{etag: "etag-1", manifest: &manifest.Manifest{
		SchemaID:    1,
		Version:     3,
		UpdatedAtMs: time.Now().Add(-time.Minute).UnixMilli(),
		Files: []manifest.FileEntry{
			{Tier: "base", Path: "/1/aaa_bbb.parquet", RowCount: 1000, SizeBytes: 10, Checksum: expectedChecksum(sourcePayload)},
			{Tier: "delta", Path: "/1/ddd.parquet", RowCount: 100, SizeBytes: 5, Checksum: expectedChecksum(sourcePayload)},
		},
	}}
	s3c := &fakeObjectS3{size: 4096}
	s3c.putObject("/1/aaa_bbb.parquet", sourcePayload)
	s3c.putObject("/1/ddd.parquet", sourcePayload)
	merger := &fakeMerger{stats: MergeStats{RowsIn: 1100, RowsOut: 950}, stage: s3c, payload: mergedPayload}

	c := newRewireableCompactor(provider, merger, s3c)
	c.ObjectReader = s3c
	result, err := c.RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, RewriteApplied, result.Outcome)

	require.GreaterOrEqual(t, len(s3c.gets), 2)
	require.Equal(t, []string{"/1/aaa_bbb.parquet", "/1/ddd.parquet"}, s3c.gets[:2], "the gate hashes the listed keys verbatim")
	require.Len(t, merger.sources, 1)
	require.Equal(t, []string{"s3://bkt//1/aaa_bbb.parquet", "s3://bkt//1/ddd.parquet"}, merger.sources[0])
	require.Contains(t, merger.tmpURIs[0], fmt.Sprintf("s3://bkt/%s/1/_tmp/", c.DataPrefix))
	require.Equal(t, []string{result.NewBaseKey}, s3c.copies)
}
