package compaction

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/manifest"
	"github.com/lychee-technology/forma/internal/telemetry"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// sourcePayload is the byte stream a rewrite source holds while healthy;
// corruptPayload is what an out-of-band mutation leaves behind.
var (
	sourcePayload  = []byte("source parquet bytes")
	corruptPayload = []byte("source parquet bytes, tampered")
)

// newVerifyFixture wires a compactor with only what the input gate needs: a
// bucket, a logger and an object reader holding real bytes.
func newVerifyFixture(logger *zap.Logger) (*Compactor, *fakeObjectS3) {
	s3c := &fakeObjectS3{}
	c := &Compactor{
		Logger:       logger,
		Config:       cdc.CompactionConfig{SchemaID: 1},
		Bucket:       "bkt",
		DataPrefix:   "p",
		ObjectReader: s3c,
	}
	return c, s3c
}

// stampedEntry builds a base entry stamped with the checksum of payload.
func stampedEntry(path string, payload []byte) manifest.FileEntry {
	return manifest.FileEntry{Tier: "base", Path: path, Checksum: expectedChecksum(payload)}
}

// A source whose stored bytes still hash to its stamp passes the gate, and the
// gate proves it read the bytes rather than trusting the manifest.
func TestVerifySourceChecksums_MatchingStampPasses(t *testing.T) {
	c, s3c := newVerifyFixture(zap.NewNop())
	s3c.putObject("p/1/aaa.parquet", sourcePayload)

	err := c.verifySourceChecksums(context.Background(), 1, []manifest.FileEntry{
		stampedEntry("p/1/aaa.parquet", sourcePayload),
	})
	require.NoError(t, err)
	require.Equal(t, []string{"p/1/aaa.parquet"}, s3c.gets, "the gate must re-hash the object, not trust the stamp")
}

// The core #347 deliverable: bytes that no longer hash to the stamp refuse the
// merge with an attributable, matchable error, one telemetry count and one
// operator-visible ERROR naming both hashes.
func TestVerifySourceChecksums_MutatedBytesRefuseMerge(t *testing.T) {
	t.Cleanup(func() { telemetry.RegisterTelemetryEmitter(nil) })
	var events []telemetryEvent
	telemetry.RegisterTelemetryEmitter(func(_ context.Context, name string, labels map[string]string, value any) {
		events = append(events, telemetryEvent{name: name, labels: labels, value: value})
	})

	core, logs := observer.New(zap.ErrorLevel)
	c, s3c := newVerifyFixture(zap.New(core))
	s3c.putObject("p/1/aaa.parquet", corruptPayload)

	err := c.verifySourceChecksums(context.Background(), 1, []manifest.FileEntry{
		stampedEntry("p/1/aaa.parquet", sourcePayload),
	})
	require.ErrorIs(t, err, ErrSourceChecksumMismatch)
	require.ErrorContains(t, err, "p/1/aaa.parquet", "the error must name the object")
	require.ErrorContains(t, err, expectedChecksum(sourcePayload), "the error must carry the stamped hash")
	require.ErrorContains(t, err, expectedChecksum(corruptPayload), "the error must carry the recomputed hash")

	require.Len(t, events, 1)
	require.Equal(t, "parquet_checksum_mismatch_total", events[0].name)
	require.Equal(t, map[string]string{"schema_id": "1"}, events[0].labels)

	entries := logs.All()
	require.Len(t, entries, 1, "a corrupt source must be reported exactly once")
	fields := entries[0].ContextMap()
	require.Equal(t, "p/1/aaa.parquet", fields["key"])
	require.Equal(t, expectedChecksum(sourcePayload), fields["stamped"])
	require.Equal(t, expectedChecksum(corruptPayload), fields["actual"])
	require.Equal(t, int16(1), fields["schema_id"])
}

// Legacy entries written before stamping carry no checksum. Field presence is
// the format version signal, so they are skipped — not probed, not failed.
func TestVerifySourceChecksums_UnstampedEntrySkipped(t *testing.T) {
	c, s3c := newVerifyFixture(zap.NewNop())
	s3c.putObject("p/1/legacy.parquet", sourcePayload)

	err := c.verifySourceChecksums(context.Background(), 1, []manifest.FileEntry{
		{Tier: "base", Path: "p/1/legacy.parquet"},
	})
	require.NoError(t, err)
	require.Empty(t, s3c.gets, "an unstamped entry has nothing to compare against")
}

// Probe failure is not a verdict: a failed GET is transient infrastructure, so
// it must never be reported as corruption.
func TestVerifySourceChecksums_ProbeFailureIsNotAVerdict(t *testing.T) {
	t.Cleanup(func() { telemetry.RegisterTelemetryEmitter(nil) })
	var events []telemetryEvent
	telemetry.RegisterTelemetryEmitter(func(_ context.Context, name string, labels map[string]string, value any) {
		events = append(events, telemetryEvent{name: name, labels: labels, value: value})
	})

	c, s3c := newVerifyFixture(zap.NewNop())
	s3c.getErr = errors.New("connection reset by peer")

	err := c.verifySourceChecksums(context.Background(), 1, []manifest.FileEntry{
		stampedEntry("p/1/aaa.parquet", sourcePayload),
	})
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrSourceChecksumMismatch, "an unreadable object is not a corrupt one")
	require.ErrorContains(t, err, "p/1/aaa.parquet")
	require.ErrorContains(t, err, "connection reset by peer", "the cause must survive the wrap")
	require.Empty(t, events, "a probe failure must not count as a mismatch")
}

// The opt-out (D2) and an unwired reader both disable the gate outright: no
// object is read at all, so neither costs a GET per source.
func TestVerifySourceChecksums_DisabledIssuesNoReads(t *testing.T) {
	sources := []manifest.FileEntry{stampedEntry("p/1/aaa.parquet", sourcePayload)}

	t.Run("SkipInputChecksumVerify", func(t *testing.T) {
		c, s3c := newVerifyFixture(zap.NewNop())
		s3c.putObject("p/1/aaa.parquet", corruptPayload)
		c.Config.SkipInputChecksumVerify = true

		require.NoError(t, c.verifySourceChecksums(context.Background(), 1, sources))
		require.Empty(t, s3c.gets)
	})

	t.Run("nil ObjectReader", func(t *testing.T) {
		c, s3c := newVerifyFixture(zap.NewNop())
		s3c.putObject("p/1/aaa.parquet", corruptPayload)
		c.ObjectReader = nil

		require.NoError(t, c.verifySourceChecksums(context.Background(), 1, sources))
		require.Empty(t, s3c.gets)
	})
}

// Defence in depth for the #417 ruling: even when called on its own, the
// checksum gate refuses a stamped source in another bucket instead of
// skipping it — it cannot hash bytes it cannot reach, and an unverifiable
// stamped source must not merge.
func TestVerifySourceChecksums_ForeignBucketPathRefused(t *testing.T) {
	core, logs := observer.New(zap.ErrorLevel)
	c, s3c := newVerifyFixture(zap.New(core))

	err := c.verifySourceChecksums(context.Background(), 1, []manifest.FileEntry{
		stampedEntry("s3://other-bkt/p/1/foreign.parquet", sourcePayload),
	})
	require.ErrorIs(t, err, ErrForeignSource)
	require.ErrorContains(t, err, "s3://other-bkt/p/1/foreign.parquet")
	require.Empty(t, s3c.gets)

	entries := logs.All()
	require.Len(t, entries, 1, "the standalone gate logs its own refusal")
	require.Equal(t, "s3://other-bkt/p/1/foreign.parquet", entries[0].ContextMap()["path"])
	require.Equal(t, "bkt", entries[0].ContextMap()["bucket"])
}

// An absolute path inside the compactor's own bucket is verified against the
// bucket-relative key, exactly as objectURI/deleteObjects resolve it.
func TestVerifySourceChecksums_OwnBucketAbsolutePathVerified(t *testing.T) {
	c, s3c := newVerifyFixture(zap.NewNop())
	s3c.putObject("p/1/aaa.parquet", corruptPayload)

	err := c.verifySourceChecksums(context.Background(), 1, []manifest.FileEntry{
		stampedEntry("s3://bkt/p/1/aaa.parquet", sourcePayload),
	})
	require.ErrorIs(t, err, ErrSourceChecksumMismatch)
	require.Equal(t, []string{"p/1/aaa.parquet"}, s3c.gets)
}

// stampedRewriteManifest is rewrite-eligible (10% dirty ratio, sub-threshold
// delta bytes) with both merge sources stamped.
func stampedRewriteManifest() *manifest.Manifest {
	return &manifest.Manifest{
		SchemaID:    1,
		Version:     3,
		UpdatedAtMs: time.Now().Add(-time.Minute).UnixMilli(),
		Files: []manifest.FileEntry{
			{Tier: "base", Path: "p/1/aaa_bbb.parquet", RowCount: 1000, SizeBytes: 10, Checksum: expectedChecksum(sourcePayload)},
			{Tier: "delta", Path: "p/1/ddd.parquet", RowCount: 100, SizeBytes: 5, Checksum: expectedChecksum(sourcePayload)},
		},
	}
}

// loadCountingProvider counts LoadManifest calls; one call means one attempt,
// so it measures whether the retry loop ran again.
type loadCountingProvider struct {
	*mockProvider
	loads int
}

func (p *loadCountingProvider) LoadManifest(ctx context.Context, schemaID int16) (*manifest.Manifest, string, error) {
	p.loads++
	return p.mockProvider.LoadManifest(ctx, schemaID)
}

// newGateRewriteFixture stages both sources' bytes; the caller decides which
// key gets corrupted before the pass runs.
func newGateRewriteFixture() (*Compactor, *loadCountingProvider, *fakeMerger, *fakeObjectS3) {
	provider := &loadCountingProvider{mockProvider: &mockProvider{manifest: stampedRewriteManifest(), etag: "etag-1"}}
	s3c := &fakeObjectS3{size: 4096}
	s3c.putObject("p/1/aaa_bbb.parquet", sourcePayload)
	s3c.putObject("p/1/ddd.parquet", sourcePayload)
	merger := &fakeMerger{stats: MergeStats{RowsIn: 1100, RowsOut: 950}, stage: s3c, payload: mergedPayload}

	c := newRewireableCompactor(provider, merger, s3c)
	c.ObjectReader = s3c
	return c, provider, merger, s3c
}

// The gate's reason for existing: runRewrite merges the sources and then
// DELETES them, so a corrupt source must stop the pass before MergeToTmp —
// nothing merged, nothing committed, nothing deleted, and the corrupt object
// still on hand for the operator.
func TestCompactor_Rewrite_MismatchingSourceRefusesTheMerge(t *testing.T) {
	c, provider, merger, s3c := newGateRewriteFixture()
	s3c.putObject("p/1/ddd.parquet", corruptPayload)

	_, err := c.RunOnce(context.Background())
	require.ErrorIs(t, err, ErrSourceChecksumMismatch)
	require.ErrorContains(t, err, "p/1/ddd.parquet")

	require.Empty(t, merger.sources, "the merge must never start over a corrupt source")
	require.Empty(t, s3c.copies, "nothing may be promoted")
	require.Empty(t, s3c.deletes, "the corrupt source must survive for diagnosis")
	require.Equal(t, int64(3), provider.mockProvider.manifest.Version, "the manifest must not be swapped")
}

// A healthy pass still rewrites: the gate verifies both sources and gets out of
// the way, so its presence is not simply a rewrite kill switch.
func TestCompactor_Rewrite_HealthySourcesPassTheGate(t *testing.T) {
	c, _, merger, s3c := newGateRewriteFixture()

	result, err := c.RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, RewriteApplied, result.Outcome)
	require.Len(t, merger.sources, 1)
	require.Contains(t, s3c.gets, "p/1/aaa_bbb.parquet")
	require.Contains(t, s3c.gets, "p/1/ddd.parquet")
}

// A mismatch is terminal for the pass: isRetryable's allowlist admits only
// ErrConcurrentModification, so a corrupt object is never re-probed under
// backoff. MaxRetries=1 would show as a second LoadManifest.
func TestCompactor_Rewrite_MismatchDoesNotConsumeRetries(t *testing.T) {
	c, provider, _, s3c := newGateRewriteFixture()
	s3c.putObject("p/1/ddd.parquet", corruptPayload)

	_, err := c.RunOnce(context.Background())
	require.ErrorIs(t, err, ErrSourceChecksumMismatch)
	require.Equal(t, 1, provider.loads, "a corrupt source must not be retried")
	require.Equal(t, 1, countGets(s3c.gets, "p/1/ddd.parquet"), "the corrupt object must be probed once, not hammered")
}

func countGets(gets []string, key string) int {
	n := 0
	for _, k := range gets {
		if k == key {
			n++
		}
	}
	return n
}

// The retry loop's own message must not be what an operator sees for a
// corrupt source; the sentinel wrap has to survive to the top.
func TestCompactor_Rewrite_MismatchSurfacesUnwrappedByRetryLoop(t *testing.T) {
	c, _, _, s3c := newGateRewriteFixture()
	s3c.putObject("p/1/ddd.parquet", corruptPayload)

	_, err := c.RunOnce(context.Background())
	require.ErrorIs(t, err, ErrSourceChecksumMismatch)
	require.NotContains(t, fmt.Sprint(err), "compaction failed after", "a mismatch is terminal, not a retry exhaustion")
}
