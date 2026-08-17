package reconcile

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/lychee-technology/forma/internal/compaction"
	"github.com/lychee-technology/forma/internal/manifest"
)

// Reconcile creates manifest entries for objects it did not write — a
// --repair adopted delta orphan and a #292 promoted init orphan — so those
// entries are the two places outside the write path where a #347 checksum can
// be stamped at all. Stamping is best-effort like the write-side paths: these
// tests pin that a hash failure costs the entry its stamp and nothing else,
// and that the discarded error is reported rather than swallowed (under
// zap.NewNop() the two are indistinguishable).

const (
	repairStampWarn    = "failed to checksum adopted orphan; entry stays unstamped"
	promotionStampWarn = "failed to checksum promoted orphan; entry stays unstamped"
)

// observeWarns swaps in a log observer for the reconciler's nop logger.
func observeWarns(r *Reconciler) *observer.ObservedLogs {
	core, logs := observer.New(zap.WarnLevel)
	r.Logger = zap.New(core)
	return logs
}

// requireStampWarn asserts the single WARN a failed checksum stamp emits,
// including the cause — the fact of failure alone does not tell an operator
// whether the store is unreachable or the object is gone.
func requireStampWarn(t *testing.T, logs *observer.ObservedLogs, message, key, cause string) {
	t.Helper()
	found := logs.FilterMessage(message).All()
	require.Len(t, found, 1, "the discarded checksum error must be reported exactly once")
	fields := found[0].ContextMap()
	require.Equal(t, int16(7), fields["schema_id"], "the warning must name the schema whose entry went unstamped")
	require.Equal(t, key, fields["key"], "the warning must name the object that went unstamped")
	require.Contains(t, fmt.Sprint(fields["error"]), cause, "the cause must survive into the log")
}

// repairOrphanFixture wires one delta orphan the repair pass will adopt.
func repairOrphanFixture(t *testing.T, objects ObjectReader) (*Reconciler, *fakeManifests, string) {
	t.Helper()
	orphanKey := "data/7/" + uuidA + ".parquet"
	lister := &fakeLister{objects: map[string][]ObjectInfo{
		"data/7/": {{Key: orphanKey, Size: 10, LastModified: testClock()}},
	}}
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}})
	stats := &fakeStats{stats: map[string]compaction.MergeStats{
		orphanKey: {RowsOut: 1, RowIDMin: uuidA, RowIDMax: uuidA, CreatedMin: 1, CreatedMax: 1},
	}}
	r := repairReconciler(t, lister, manifests, &fakeEnum{ids: []int16{7}}, stats)
	r.Objects = objects
	return r, manifests, orphanKey
}

func TestRepairChecksum_StampsAdoptedOrphan(t *testing.T) {
	body := []byte("adopted delta bytes")
	objects := &fakeObjects{bodies: map[string][]byte{"data/7/" + uuidA + ".parquet": body}}
	r, manifests, orphanKey := repairOrphanFixture(t, objects)
	logs := observeWarns(r)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{orphanKey}, report.Schemas[0].Repaired)

	require.Len(t, manifests.saves, 1)
	require.Len(t, manifests.saves[0].m.Files, 1)
	require.Equal(t, wantChecksum(body), manifests.saves[0].m.Files[0].Checksum,
		"the adopted entry must carry the hash of the object's bytes")
	require.Equal(t, []string{orphanKey}, objects.gets, "the adopted object is read exactly once")
	require.Zero(t, logs.Len(), "nothing to report when the hash succeeds")
}

func TestRepairChecksum_ProbeFailureLeavesEntryUnstamped(t *testing.T) {
	orphanKey := "data/7/" + uuidA + ".parquet"
	objects := &fakeObjects{errFor: map[string]error{orphanKey: fmt.Errorf("s3 unavailable")}}
	r, manifests, _ := repairOrphanFixture(t, objects)
	logs := observeWarns(r)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{orphanKey}, report.Schemas[0].Repaired,
		"a failed hash must not cost the orphan its adoption")
	require.NoError(t, report.Schemas[0].Err)
	require.Len(t, manifests.saves, 1)
	require.Empty(t, manifests.saves[0].m.Files[0].Checksum, "a failed hash leaves the entry unstamped")
	requireStampWarn(t, logs, repairStampWarn, orphanKey, "s3 unavailable")
}

func TestRepairChecksum_NilObjectReaderSkipsStamping(t *testing.T) {
	// --repair does not require the reader (only --verify-checksums does), so
	// a library caller may wire none: the entry is published unstamped and the
	// scrub counts it as missing coverage.
	r, manifests, orphanKey := repairOrphanFixture(t, nil)
	logs := observeWarns(r)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{orphanKey}, report.Schemas[0].Repaired)
	require.Len(t, manifests.saves, 1)
	require.Empty(t, manifests.saves[0].m.Files[0].Checksum)
	require.Zero(t, logs.Len(), "an absent reader is a configuration choice, not a failure")
}

// promoteOrphanFixture wires the two-file init orphan set promoteReconciler
// describes, with the given object reader.
func promoteOrphanFixture(t *testing.T, objects ObjectReader) (*Reconciler, *fakeManifests, string, string) {
	t.Helper()
	lister, stats, live, file1, file2 := completeInitSet()
	manifests := newFakeManifests(&manifest.Manifest{SchemaID: 7, Files: []manifest.FileEntry{}})
	r := promoteReconciler(t, lister, manifests, stats, live)
	r.Objects = objects
	return r, manifests, file1, file2
}

func TestPromoteChecksum_StampsPromotedOrphans(t *testing.T) {
	body1, body2 := []byte("init file one"), []byte("init file two")
	objects := &fakeObjects{bodies: map[string][]byte{
		initKey(rid1, rid2): body1,
		initKey(rid3, rid3): body2,
	}}
	r, manifests, file1, file2 := promoteOrphanFixture(t, objects)
	logs := observeWarns(r)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{file1, file2}, report.Schemas[0].PromotedBase)

	require.Len(t, manifests.saves, 1)
	stamped := map[string]string{}
	for _, f := range manifests.saves[0].m.Files {
		stamped[f.Path] = f.Checksum
	}
	require.Equal(t, wantChecksum(body1), stamped[file1])
	require.Equal(t, wantChecksum(body2), stamped[file2])
	require.ElementsMatch(t, []string{file1, file2}, objects.gets, "each promoted object is read once")
	require.Zero(t, logs.Len(), "nothing to report when the hashes succeed")
}

func TestPromoteChecksum_ProbeFailureLeavesEntryUnstamped(t *testing.T) {
	body2 := []byte("init file two")
	objects := &fakeObjects{
		bodies: map[string][]byte{initKey(rid3, rid3): body2},
		errFor: map[string]error{initKey(rid1, rid2): fmt.Errorf("s3 unavailable")},
	}
	r, manifests, file1, file2 := promoteOrphanFixture(t, objects)
	logs := observeWarns(r)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{file1, file2}, report.Schemas[0].PromotedBase,
		"a failed hash must not block the promotion of a proven-complete set")
	require.Empty(t, report.Schemas[0].InitPromotionRefusal)

	require.Len(t, manifests.saves, 1)
	stamped := map[string]string{}
	for _, f := range manifests.saves[0].m.Files {
		stamped[f.Path] = f.Checksum
	}
	require.Empty(t, stamped[file1], "a failed hash leaves that entry unstamped")
	require.Equal(t, wantChecksum(body2), stamped[file2], "its sibling still gets stamped")
	requireStampWarn(t, logs, promotionStampWarn, file1, "s3 unavailable")
}

func TestPromoteChecksum_NilObjectReaderSkipsStamping(t *testing.T) {
	r, manifests, file1, file2 := promoteOrphanFixture(t, nil)
	logs := observeWarns(r)

	report, err := r.Run(context.Background())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{file1, file2}, report.Schemas[0].PromotedBase)
	require.Len(t, manifests.saves, 1)
	for _, f := range manifests.saves[0].m.Files {
		require.Empty(t, f.Checksum, "no reader wired, so nothing to stamp with")
	}
	require.Zero(t, logs.Len(), "an absent reader is a configuration choice, not a failure")
}
