package compaction

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/manifest"
	"github.com/lychee-technology/forma/internal/telemetry"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// fakeMerger records the requested merge and returns canned stats. When stage
// is set it also writes payload at the tmp key, modeling a merge that produces
// real bytes for the rewrite to promote.
type fakeMerger struct {
	stats   MergeStats
	sources [][]string
	tmpURIs []string

	stage   *fakeObjectS3
	payload []byte
}

func (f *fakeMerger) MergeToTmp(_ context.Context, sourceURIs []string, tmpURI string) (MergeStats, error) {
	f.sources = append(f.sources, sourceURIs)
	f.tmpURIs = append(f.tmpURIs, tmpURI)
	if f.stage != nil {
		f.stage.putObject(keyFromURI(tmpURI), f.payload)
	}
	return f.stats, nil
}

// keyFromURI strips the s3://<bucket>/ prefix, yielding the bucket-relative
// key the S3 client would have been called with.
func keyFromURI(uri string) string {
	rest := strings.TrimPrefix(uri, "s3://")
	if idx := strings.Index(rest, "/"); idx >= 0 {
		return rest[idx+1:]
	}
	return rest
}

// fakeObjectS3 records copy/delete keys; HeadObject reports a fixed size. It
// also keeps a tiny object store so the checksum tests can hash what the
// rewrite actually published: CopyObject moves bytes from the CopySource key
// to the destination key, DeleteObject drops them, and GetObject serves them.
type fakeObjectS3 struct {
	copies  []string // final keys
	deletes []string
	size    int64

	objects map[string][]byte // bucket-relative key -> stored bytes
	getErr  error             // when set, every GetObject fails with it
	gets    []string          // keys GetObject was called with, in order
}

// putObject stages bytes under a bucket-relative key (what a writer would have
// produced before the rewrite promotes it).
func (f *fakeObjectS3) putObject(key string, body []byte) {
	if f.objects == nil {
		f.objects = map[string][]byte{}
	}
	f.objects[key] = body
}

func (f *fakeObjectS3) CopyObject(_ context.Context, in *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	key := aws.ToString(in.Key)
	f.copies = append(f.copies, key)
	// CopySource is "<bucket>/<key>"; carry the bytes over when we hold them.
	src := strings.TrimPrefix(aws.ToString(in.CopySource), aws.ToString(in.Bucket)+"/")
	if body, ok := f.objects[src]; ok {
		f.putObject(key, body)
	}
	return &s3.CopyObjectOutput{}, nil
}

func (f *fakeObjectS3) DeleteObject(_ context.Context, in *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	key := aws.ToString(in.Key)
	f.deletes = append(f.deletes, key)
	delete(f.objects, key)
	return &s3.DeleteObjectOutput{}, nil
}

func (f *fakeObjectS3) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	f.gets = append(f.gets, aws.ToString(in.Key))
	if f.getErr != nil {
		return nil, f.getErr
	}
	key := aws.ToString(in.Key)
	body, ok := f.objects[key]
	if !ok {
		return nil, fmt.Errorf("fake s3: no such key %q", key)
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(body))}, nil
}

func (f *fakeObjectS3) HeadObject(_ context.Context, _ *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return &s3.HeadObjectOutput{ContentLength: aws.Int64(f.size)}, nil
}

// conflictOnceProvider fails the first SaveManifest with a CONFIRMED 412
// (smithy PreconditionFailed — the only shape saveManifestChecked classifies
// as ErrConcurrentModification), then delegates. LoadManifest returns a
// FRESH manifest copy per call, mirroring the real provider: the retry must
// recompute from a clean load.
type conflictOnceProvider struct {
	inner    *mockProvider
	failures int
	saves    int
}

func (p *conflictOnceProvider) LoadManifest(ctx context.Context, schemaID int16) (*manifest.Manifest, string, error) {
	m, etag, err := p.inner.LoadManifest(ctx, schemaID)
	if err != nil || m == nil {
		return m, etag, err
	}
	cp := *m
	cp.Files = append([]manifest.FileEntry(nil), m.Files...)
	return &cp, etag, nil
}

func (p *conflictOnceProvider) SaveManifest(ctx context.Context, schemaID int16, m *manifest.Manifest, etag string) (string, error) {
	p.saves++
	if p.saves <= p.failures {
		return "", errRewriteTestConflict
	}
	return p.inner.SaveManifest(ctx, schemaID, m, etag)
}

var errRewriteTestConflict = &conflictErr{}

// conflictErr implements smithy.APIError with code PreconditionFailed — the
// confirmed-412 shape rustfs/S3 return for a stale If-Match.
type conflictErr struct{}

func (*conflictErr) Error() string                 { return "api error PreconditionFailed (test)" }
func (*conflictErr) ErrorCode() string             { return "PreconditionFailed" }
func (*conflictErr) ErrorMessage() string          { return "precondition failed (test)" }
func (*conflictErr) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

// nonTmpDeletes filters out CopyTmpToFinal's own _tmp cleanup, leaving only
// the deletions the rewrite orchestration decided on.
func nonTmpDeletes(keys []string) []string {
	var out []string
	for _, k := range keys {
		if !strings.Contains(k, "/_tmp/") {
			out = append(out, k)
		}
	}
	return out
}

func rewriteEligibleManifest() *manifest.Manifest {
	return &manifest.Manifest{
		SchemaID:    1,
		Version:     3,
		UpdatedAtMs: time.Now().Add(-time.Minute).UnixMilli(),
		Files: []manifest.FileEntry{
			{Tier: "base", Path: "p/1/aaa_bbb.parquet", RowCount: 1000, SizeBytes: 10},
			{Tier: "delta", Path: "p/1/ddd.parquet", RowCount: 100, SizeBytes: 5},
			{Tier: "sidecar", Path: "p/1/keep.parquet", RowCount: 1}, // not base/delta: must survive the splice
		},
	}
}

func newRewireableCompactor(provider FileProvider, merger Merger, s3c cdc.S3ObjectClient) *Compactor {
	return &Compactor{
		Logger:     zap.NewNop(),
		Config:     cdc.CompactionConfig{SchemaID: 1, MaxRetries: 1, BaseBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond},
		Provider:   provider,
		Merger:     merger,
		S3:         s3c,
		Bucket:     "bkt",
		DataPrefix: "p",
	}
}

func TestCompactor_Rewrite_MergesAndSplicesManifest(t *testing.T) {
	t.Cleanup(func() { telemetry.RegisterTelemetryEmitter(nil) })
	var applied int
	telemetry.RegisterTelemetryEmitter(func(_ context.Context, name string, _ map[string]string, _ any) {
		if name == "compaction_rewrite_applied_total" {
			applied++
		}
	})

	provider := &mockProvider{manifest: rewriteEligibleManifest(), etag: "etag-1"}
	merger := &fakeMerger{stats: MergeStats{
		RowsIn: 1100, RowsOut: 950,
		RowIDMin: "aaa", RowIDMax: "zzz", CreatedMin: 100, CreatedMax: 900,
		Columns: map[string]string{"row_id": "UUID", "changed_at": "BIGINT", "deleted_at": "BIGINT"},
	}}
	s3c := &fakeObjectS3{size: 4096}

	c := newRewireableCompactor(provider, merger, s3c)
	result, err := c.RunOnce(context.Background())
	require.NoError(t, err)

	require.Equal(t, RewriteApplied, result.Outcome)
	require.Equal(t, 2, result.FilesMerged)
	require.Equal(t, int64(1100), result.RowsIn)
	require.Equal(t, int64(950), result.RowsOut)
	require.True(t, strings.HasPrefix(result.NewBaseKey, "p/1/base-"), result.NewBaseKey)
	require.Equal(t, int64(4), result.Version)

	// The merge read exactly the base+delta set as bucket URIs and staged
	// into _tmp before the copy promoted it to the final key.
	require.Len(t, merger.sources, 1)
	require.Equal(t, []string{"s3://bkt/p/1/aaa_bbb.parquet", "s3://bkt/p/1/ddd.parquet"}, merger.sources[0])
	require.Contains(t, merger.tmpURIs[0], "s3://bkt/p/1/_tmp/")
	require.Equal(t, []string{result.NewBaseKey}, s3c.copies)

	// Splice: merged entries out, new base in, foreign-tier entry preserved.
	m := provider.manifest
	require.Len(t, m.Files, 2)
	require.Equal(t, "sidecar", m.Files[0].Tier)
	newBase := m.Files[1]
	require.Equal(t, "base", newBase.Tier)
	require.Equal(t, result.NewBaseKey, newBase.Path)
	require.Equal(t, int64(950), newBase.RowCount)
	require.Equal(t, "aaa", newBase.RowIDMin)
	require.Equal(t, "zzz", newBase.RowIDMax)
	require.Equal(t, int64(100), newBase.CreatedMin)
	require.Equal(t, int64(900), newBase.CreatedMax)
	require.Equal(t, int64(4096), newBase.SizeBytes)
	require.Equal(t, map[string]string{"row_id": "UUID", "changed_at": "BIGINT", "deleted_at": "BIGINT"}, newBase.Columns)

	// Sources deleted only AFTER the successful commit; staged key kept.
	require.Equal(t, []string{"p/1/aaa_bbb.parquet", "p/1/ddd.parquet"}, nonTmpDeletes(s3c.deletes))
	require.Equal(t, 1, applied)
}

func TestCompactor_Rewrite_ConflictRetryRecomputesFromFreshManifest(t *testing.T) {
	provider := &conflictOnceProvider{inner: &mockProvider{manifest: rewriteEligibleManifest(), etag: "etag-1"}, failures: 1}
	merger := &fakeMerger{stats: MergeStats{RowsIn: 1100, RowsOut: 950, RowIDMin: "aaa", RowIDMax: "zzz"}}
	s3c := &fakeObjectS3{size: 1}

	c := newRewireableCompactor(provider, merger, s3c)
	result, err := c.RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, RewriteApplied, result.Outcome)

	// Two full attempts: the retry re-merged from a fresh manifest load.
	require.Equal(t, 2, provider.saves)
	require.Len(t, merger.sources, 2)
	require.Equal(t, merger.sources[0], merger.sources[1])

	// Attempt 1's staged base (never listed) was cleaned up before the retry;
	// the committed sources were deleted exactly once, after the commit.
	require.Len(t, s3c.copies, 2)
	firstStaged := s3c.copies[0]
	require.Equal(t, []string{firstStaged, "p/1/aaa_bbb.parquet", "p/1/ddd.parquet"}, nonTmpDeletes(s3c.deletes))
	require.Equal(t, s3c.copies[1], result.NewBaseKey)
	require.NotContains(t, s3c.deletes, result.NewBaseKey)
}

// ambiguousSaveProvider fails SaveManifest with a plain transport-shaped
// error (NOT a 412). Optionally it COMMITS the save to the inner provider
// first, modeling a conditional put that landed before the response was
// lost.
type ambiguousSaveProvider struct {
	inner       *mockProvider
	commitFirst bool
	saves       int
}

func (p *ambiguousSaveProvider) LoadManifest(ctx context.Context, schemaID int16) (*manifest.Manifest, string, error) {
	m, etag, err := p.inner.LoadManifest(ctx, schemaID)
	if err != nil || m == nil {
		return m, etag, err
	}
	cp := *m
	cp.Files = append([]manifest.FileEntry(nil), m.Files...)
	return &cp, etag, nil
}

func (p *ambiguousSaveProvider) SaveManifest(ctx context.Context, schemaID int16, m *manifest.Manifest, etag string) (string, error) {
	p.saves++
	if p.commitFirst {
		if _, err := p.inner.SaveManifest(ctx, schemaID, m, etag); err != nil {
			return "", err
		}
	}
	return "", errors.New("dial tcp: i/o timeout (test)")
}

// TestCompactor_Rewrite_AmbiguousSaveRetainsStagedBase pins the durability
// contract from the PR #253 review: a save failure that is NOT a confirmed
// 412 may have committed, so the staged base must never be deleted, the
// error must not be classified as a retryable CAS conflict, and it must
// carry the retained key as the operator's pointer.
func TestCompactor_Rewrite_AmbiguousSaveRetainsStagedBase(t *testing.T) {
	provider := &ambiguousSaveProvider{inner: &mockProvider{manifest: rewriteEligibleManifest(), etag: "etag-1"}}
	merger := &fakeMerger{stats: MergeStats{RowsIn: 1, RowsOut: 1}}
	s3c := &fakeObjectS3{}

	c := newRewireableCompactor(provider, merger, s3c)
	_, err := c.RunOnce(context.Background())
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrConcurrentModification)
	require.Equal(t, 1, provider.saves, "ambiguous outcome must not be retried")

	require.Len(t, s3c.copies, 1)
	staged := s3c.copies[0]
	require.NotContains(t, nonTmpDeletes(s3c.deletes), staged, "possibly-committed base must be retained")
	require.ErrorContains(t, err, staged)
	require.ErrorContains(t, err, "ambiguous")
}

// TestCompactor_Rewrite_CommittedSaveWithLostResponse is the review's
// requested regression: the conditional put COMMITS but the response is
// lost. The staged base is now manifest-listed; deleting it would break the
// committed manifest. The next pass must read the committed state as
// healthy (Noop — no deltas remain) with the new base still listed.
func TestCompactor_Rewrite_CommittedSaveWithLostResponse(t *testing.T) {
	inner := &mockProvider{manifest: rewriteEligibleManifest(), etag: "etag-1"}
	provider := &ambiguousSaveProvider{inner: inner, commitFirst: true}
	merger := &fakeMerger{stats: MergeStats{RowsIn: 1100, RowsOut: 950, RowIDMin: "aaa", RowIDMax: "zzz"}}
	s3c := &fakeObjectS3{size: 1}

	c := newRewireableCompactor(provider, merger, s3c)
	_, err := c.RunOnce(context.Background())
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrConcurrentModification)

	require.Len(t, s3c.copies, 1)
	committedKey := s3c.copies[0]
	require.NotContains(t, nonTmpDeletes(s3c.deletes), committedKey, "must never delete the listed finalKey")

	// The commit landed: the manifest lists exactly the new base (+ the
	// foreign-tier entry) and points at a live object.
	var listed []string
	for _, f := range inner.manifest.Files {
		listed = append(listed, f.Path)
	}
	require.Contains(t, listed, committedKey)

	// Self-heal: the next pass sees no deltas and reports Noop.
	second := newRewireableCompactor(provider, merger, s3c)
	result, err := second.RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, Noop, result.Outcome)
	require.NotContains(t, nonTmpDeletes(s3c.deletes), committedKey)
}

func TestCompactor_Rewrite_ExhaustedConflictsSurfaceRetryableError(t *testing.T) {
	provider := &conflictOnceProvider{inner: &mockProvider{manifest: rewriteEligibleManifest(), etag: "etag-1"}, failures: 10}
	merger := &fakeMerger{stats: MergeStats{RowsIn: 1, RowsOut: 1}}
	s3c := &fakeObjectS3{}

	c := newRewireableCompactor(provider, merger, s3c)
	_, err := c.RunOnce(context.Background())
	require.ErrorIs(t, err, ErrConcurrentModification)

	// Every attempt's staged base was dropped; no source was ever deleted.
	require.Len(t, s3c.copies, 2) // MaxRetries=1 -> two attempts
	require.ElementsMatch(t, s3c.copies, nonTmpDeletes(s3c.deletes))
}
