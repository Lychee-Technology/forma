package compaction

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// mergedPayload is the byte stream the fake merge produces; the rewrite copies
// it from tmp to the final base key, so its hash is what the manifest entry
// must carry.
var mergedPayload = []byte("merged base parquet bytes")

// expectedChecksum is an independent oracle: sha256 over the payload, computed
// here rather than through cdc.ObjectSHA256, so a change in the primitive
// cannot agree with itself.
func expectedChecksum(payload []byte) string {
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// newChecksumRewriteFixture wires a rewrite-eligible compactor whose merge
// stages real bytes in the fake store, with the supplied logger observing.
func newChecksumRewriteFixture(logger *zap.Logger) (*Compactor, *mockProvider, *fakeObjectS3) {
	provider := &mockProvider{manifest: rewriteEligibleManifest(), etag: "etag-1"}
	s3c := &fakeObjectS3{size: 4096}
	merger := &fakeMerger{
		stats:   MergeStats{RowsIn: 1100, RowsOut: 950, RowIDMin: "aaa", RowIDMax: "zzz"},
		stage:   s3c,
		payload: mergedPayload,
	}

	c := newRewireableCompactor(provider, merger, s3c)
	c.Logger = logger
	return c, provider, s3c
}

// lastEntry returns the base entry the splice appended.
func lastEntry(t *testing.T, provider *mockProvider) string {
	t.Helper()
	files := provider.manifest.Files
	require.NotEmpty(t, files)
	return files[len(files)-1].Checksum
}

// The merged base's content hash is stamped into the spliced manifest entry so
// a later verification pass can detect mutation without re-reading the tier
// (#347). The hash must be of the FINAL object's bytes: the tmp key is already
// deleted by then, so stamping the wrong key cannot silently pass.
func TestCompactor_Rewrite_StampsMergedBaseChecksum(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	c, provider, s3c := newChecksumRewriteFixture(zap.New(core))
	c.ObjectReader = s3c

	result, err := c.RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, RewriteApplied, result.Outcome)

	// The bytes the fake holds under the final key are the ones we hashed.
	require.Equal(t, mergedPayload, s3c.objects[result.NewBaseKey])
	require.Equal(t, expectedChecksum(mergedPayload), lastEntry(t, provider))
	require.Zero(t, logs.Len(), "nothing to report when the hash succeeds")
}

// Deployments without a reader wired keep the pre-#347 behavior: the entry is
// spliced in, just unstamped — and silently. Not having opted into stamping is
// not a fault, so it must not warn on every pass; only the guard in
// stampChecksum keeps ObjectSHA256's own "s3 client is nil" error off the log.
func TestCompactor_Rewrite_WithoutObjectReaderLeavesEntryUnstamped(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	c, provider, _ := newChecksumRewriteFixture(zap.New(core))
	c.ObjectReader = nil

	result, err := c.RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, RewriteApplied, result.Outcome)
	require.Empty(t, lastEntry(t, provider))
	require.Zero(t, logs.Len(), "an unstamped-by-configuration entry is not a fault to report")
}

// A failed hash must not fail the rewrite and must leave the entry unstamped —
// verification skips an empty Checksum, the SizeBytes precedent. Under a nop
// logger a swallowed error and a reported one look identical, so the warning
// is observed too.
func TestCompactor_Rewrite_ChecksumFailureLeavesEntryUnstampedAndWarns(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	c, provider, s3c := newChecksumRewriteFixture(zap.New(core))
	c.ObjectReader = s3c
	s3c.getErr = errors.New("get object failed")

	result, err := c.RunOnce(context.Background())
	require.NoError(t, err, "a failed checksum must never fail the rewrite")
	require.Equal(t, RewriteApplied, result.Outcome)
	require.Empty(t, lastEntry(t, provider), "the entry stays unstamped")

	entries := logs.All()
	require.Len(t, entries, 1, "the discarded checksum error must be reported exactly once")
	require.Equal(t, "failed to checksum merged base; manifest entry stays unstamped", entries[0].Message)

	fields := entries[0].ContextMap()
	require.Equal(t, int16(1), fields["schema_id"], "the warning must name the schema whose entry went unstamped")
	require.Equal(t, result.NewBaseKey, fields["final_key"], "the warning must name the object that went unstamped")
	require.Contains(t, fmt.Sprint(fields["error"]), "get object failed",
		"the cause must survive into the log, not just the fact of failure")
}
