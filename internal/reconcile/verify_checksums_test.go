package reconcile

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/manifest"
)

// fakeObjects serves object bytes by key and records every GET, so skip rules
// can be asserted as "never read" rather than only "no divergence reported".
type fakeObjects struct {
	bodies map[string][]byte
	errFor map[string]error
	gets   []string
}

func (f *fakeObjects) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	key := aws.ToString(in.Key)
	f.gets = append(f.gets, key)
	if err := f.errFor[key]; err != nil {
		return nil, err
	}
	body, ok := f.bodies[key]
	if !ok {
		return nil, fmt.Errorf("no such object %s", key)
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(body))}, nil
}

// wantChecksum hashes bytes here rather than through cdc.ObjectSHA256, so the
// expectation is an independent oracle: a change in the primitive's encoding
// breaks these tests instead of moving with them.
func wantChecksum(b []byte) string {
	sum := sha256.Sum256(b)
	return cdc.ChecksumSHA256Prefix + hex.EncodeToString(sum[:])
}

// listedChecksumEntry wires one object that is both present in S3 and listed
// in the manifest — the only shape --verify-checksums reads.
func listedChecksumEntry(key, checksum string) (*fakeLister, *fakeManifests) {
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {{Key: key, Size: 10, LastModified: testClock()}},
	}}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "delta", Path: key, Checksum: checksum},
	}})
	return lister, manifests
}

func checksumReconciler(t *testing.T, lister *fakeLister, manifests *fakeManifests, objects ObjectReader) *Reconciler {
	t.Helper()
	r := newTestReconciler(lister, manifests, &fakeDeleter{}, &fakeLocker{}, &fakeEnum{ids: []int16{7}})
	r.Objects = objects
	r.Opts = Options{VerifyChecksums: true, MaxETagRetries: 3}
	return r
}

func TestVerifyChecksums_MatchingBytesAreClean(t *testing.T) {
	key := "data/7/" + uuidA + ".parquet"
	body := []byte("healthy parquet bytes")
	lister, manifests := listedChecksumEntry(key, wantChecksum(body))
	objects := &fakeObjects{bodies: map[string][]byte{key: body}}
	r := checksumReconciler(t, lister, manifests, objects)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{key}, objects.gets, "the listed stamped entry must be re-hashed")
	s := report.Schemas[0]
	require.Empty(t, s.ChecksumDivergences)
	require.Zero(t, s.SkippedUnstamped)
	require.False(t, s.Residual())
	require.False(t, report.HasResidualDiscrepancies())

	var out bytes.Buffer
	report.Render(&out)
	require.Equal(t, "schema 7: clean\n", out.String())
}

func TestVerifyChecksums_MutatedBytesAreDivergence(t *testing.T) {
	// The #347 failure mode: the object's bytes were rewritten out of band.
	// The footer shape can stay perfectly legal, so --verify-stamps sees
	// nothing; only the content hash catches it.
	key := "data/7/" + uuidA + ".parquet"
	stamped := wantChecksum([]byte("the blessed bytes"))
	mutated := []byte("silently rewritten bytes")
	lister, manifests := listedChecksumEntry(key, stamped)
	objects := &fakeObjects{bodies: map[string][]byte{key: mutated}}
	r := checksumReconciler(t, lister, manifests, objects)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	s := report.Schemas[0]
	require.Equal(t, []string{fmt.Sprintf("%s: checksum stamp %s vs bytes %s", key, stamped, wantChecksum(mutated))},
		s.ChecksumDivergences)
	require.True(t, s.Residual(), "a byte-integrity breach is actionable")
	require.True(t, report.HasResidualDiscrepancies(), "must reach the exit-2 path")

	var out bytes.Buffer
	report.Render(&out)
	require.Contains(t, out.String(), "checksum divergence: "+s.ChecksumDivergences[0])
}

func TestVerifyChecksums_LegacyUnstampedEntrySkippedAndCounted(t *testing.T) {
	// Stamping is not backfilled (#347): an entry written before it carries
	// no Checksum and there is nothing to compare against. The count is the
	// stamping-coverage signal — how much of the manifest this scrub cannot
	// cover yet — so it must reach the report even on an otherwise clean run.
	key := "data/7/" + uuidA + ".parquet"
	lister, manifests := listedChecksumEntry(key, "")
	objects := &fakeObjects{bodies: map[string][]byte{key: []byte("anything")}}
	r := checksumReconciler(t, lister, manifests, objects)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Empty(t, objects.gets, "a legacy entry must not be read")
	s := report.Schemas[0]
	require.Empty(t, s.ChecksumDivergences)
	require.Equal(t, 1, s.SkippedUnstamped)
	require.False(t, report.HasResidualDiscrepancies(), "missing coverage is not a discrepancy")

	var out bytes.Buffer
	report.Render(&out)
	require.Equal(t, "schema 7: clean (1 unstamped entry skipped)\n", out.String())
}

func TestVerifyChecksums_UnprovenDanglingCandidateNotRead(t *testing.T) {
	// The skip set is the candidates this run did NOT prove present. Here a
	// concurrent compactor splices the entry out (and deletes the object)
	// mid-run, so confirmDangling clears the candidate without probing it,
	// while the pre-run manifest this pass iterates still carries it. A GET
	// would fail and escalate the schema to a spurious exit 1.
	key := "data/7/" + uuidA + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{"data/7/": nil}}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "delta", Path: key, Checksum: wantChecksum([]byte("gone"))},
	}})
	manifests.onLoad = func(f *fakeManifests) {
		if f.loads >= 2 { // confirmDangling's reload sees the splice
			f.manifests[7] = &manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}}
		}
	}
	objects := &fakeObjects{bodies: map[string][]byte{}}
	r := checksumReconciler(t, lister, manifests, objects)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	s := report.Schemas[0]
	require.NoError(t, s.Err)
	require.Empty(t, s.Dangling, "the candidate was spliced out, so it is not confirmed dangling")
	require.Empty(t, objects.gets, "an unproven dangling candidate must not be read")
	require.Empty(t, s.ChecksumDivergences)
	require.False(t, report.HasResidualDiscrepancies())
}

func TestVerifyChecksums_OutOfPrefixEntryNotRead(t *testing.T) {
	// An entry outside this schema's data prefix is the unverifiable subclass
	// diffSchema reports: the listing does not cover it, so a failed GET could
	// not be distinguished from drift.
	key := "data/7/" + uuidA + ".parquet"
	outOfPrefix := "data/9/" + uuidB + ".parquet"
	body := []byte("in prefix")
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {{Key: key, Size: 10, LastModified: testClock()}},
	}}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{
		{Tier: "delta", Path: key, Checksum: wantChecksum(body)},
		{Tier: "base", Path: outOfPrefix, Checksum: wantChecksum([]byte("elsewhere"))},
	}})
	objects := &fakeObjects{bodies: map[string][]byte{key: body}}
	r := checksumReconciler(t, lister, manifests, objects)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	s := report.Schemas[0]
	require.Equal(t, []string{outOfPrefix}, s.Unverifiable)
	require.Equal(t, []string{key}, objects.gets, "only the in-prefix entry is read")
	require.Empty(t, s.ChecksumDivergences)
}

func TestVerifyChecksums_GetFailureFailsSchemaOnly(t *testing.T) {
	// A storage outage is a tool failure (exit 1), never a corruption verdict
	// — the same rule confirmDangling and verifyStamps follow.
	keyA := "data/1/" + uuidA + ".parquet"
	keyB := "data/2/" + uuidB + ".parquet"
	bodyB := []byte("schema 2 bytes")
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/1/": {{Key: keyA, Size: 10, LastModified: testClock()}},
		"data/2/": {{Key: keyB, Size: 10, LastModified: testClock()}},
	}}
	manifests := newFakeManifests(
		&manifest.Manifest{SchemaID: 1, Files: []manifest.FileEntry{{Tier: "delta", Path: keyA, Checksum: wantChecksum([]byte("a"))}}},
		&manifest.Manifest{SchemaID: 2, Files: []manifest.FileEntry{{Tier: "delta", Path: keyB, Checksum: wantChecksum(bodyB)}}},
	)
	objects := &fakeObjects{
		bodies: map[string][]byte{keyB: bodyB},
		errFor: map[string]error{keyA: fmt.Errorf("s3 unavailable")},
	}
	r := checksumReconciler(t, lister, manifests, objects)
	r.Schemas = &fakeEnum{ids: []int16{1, 2}}

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Error(t, report.Schemas[0].Err)
	require.Contains(t, report.Schemas[0].Err.Error(), "checksum")
	require.Empty(t, report.Schemas[0].ChecksumDivergences, "a read failure is never a corruption verdict")
	require.NoError(t, report.Schemas[1].Err, "other schemas still reconcile")
	require.Empty(t, report.Schemas[1].ChecksumDivergences)
}

func TestVerifyChecksums_MissingObjectReaderFailsSchema(t *testing.T) {
	key := "data/7/" + uuidA + ".parquet"
	lister, manifests := listedChecksumEntry(key, wantChecksum([]byte("x")))
	r := checksumReconciler(t, lister, manifests, nil)
	r.Objects = nil

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Error(t, report.Schemas[0].Err)
	require.Contains(t, report.Schemas[0].Err.Error(), "object reader is not configured")
}

func TestVerifyChecksums_DisabledByDefault(t *testing.T) {
	key := "data/7/" + uuidA + ".parquet"
	lister, manifests := listedChecksumEntry(key, wantChecksum([]byte("the blessed bytes")))
	objects := &fakeObjects{bodies: map[string][]byte{key: []byte("silently rewritten")}}
	r := checksumReconciler(t, lister, manifests, objects)
	r.Opts.VerifyChecksums = false

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Empty(t, objects.gets, "the scrub is opt-in")
	require.Empty(t, report.Schemas[0].ChecksumDivergences)
	require.False(t, report.HasResidualDiscrepancies())
}
