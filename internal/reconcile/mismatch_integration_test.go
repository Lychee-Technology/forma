package reconcile

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAHistoryRowThatDisagreesWithTheRecordIsNotAMissingRow is R8-F3: only
// the current history row's digest is changed. The row is still there, so
// it is reported as history_mismatch, not as the missing row whose recovery
// takes two publications and an expiry, and the intact manifest is not to
// be restored to match the row: it verifies against the record, and reads
// succeed. The rendered action tells the operator to leave collection
// refused, and it is right: after the missing row's recovery (two
// publications, a retirement and the grace) the row's manifest still fails
// against the row, collection is still refused, and nothing of the
// generation is deleted or forgotten.
func TestAHistoryRowThatDisagreesWithTheRecordIsNotAMissingRow(t *testing.T) {
	f := newFixture(t)
	g1 := f.publishInit(t, f.initialized(t), 1)
	f.exec(t, "UPDATE %s SET manifest_sha256 = '"+strings.Repeat("0", 64)+"' WHERE generation = 1", f.tables.Generation)

	rep := f.pass(t, testOptions(false))
	require.NoError(t, rep.Err)
	require.Equal(t, []FindingKind{HistoryMismatch, InvalidManifest}, kinds(rep.Findings))
	require.Equal(t, actionHistoryMismatchRefuse, rep.Findings[0].Action)
	require.Contains(t, rep.Findings[0].Detail, "present but disagrees with the record")
	require.Equal(t, actionMismatchedManifest, rep.Findings[1].Action)
	require.Contains(t, rep.Findings[1].Detail, "verifies against the record, as readers verify it, so reads succeed")
	var out bytes.Buffer
	Report{Schemas: []SchemaReport{rep}}.Render(&out)
	for _, step := range []string{
		"history_mismatch",
		"Do not rewrite either to make them agree, and do not restore the row from a backup",
		"re-initializing with cdc-init restores them",
		"A re-init does not by itself resume garbage collection",
		"no publication and no expiry clears the refusal",
		"Leave garbage collection refused for the schema",
		"do not restore the object to match the generation's history row",
	} {
		require.Contains(t, out.String(), step)
	}
	require.NotContains(t, out.String(), "takes two publications", "the missing row's recovery is not promised")

	reinit := f.publishInit(t, g1.rec, 1)
	f.publish(t, f.stageFlush(t, reinit, 1))
	rep = f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.ElementsMatch(t, []int64{g1.rec.Generation, reinit.rec.Generation}, rep.Collected.Retired)
	waitOutListingPrecision() // past the grace: both replaced generations expired

	rep = f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Equal(t, 2, rep.Expired)
	require.Equal(t, []FindingKind{InvalidManifest}, kinds(rep.Findings), "the expired row's manifest still fails against it")
	require.Equal(t, g1.rec.Generation, rep.Findings[0].Generation)
	require.NotEmpty(t, rep.Refused, "expiry does not clear the refusal")
	require.Empty(t, rep.Collected.Objects)
	require.Empty(t, rep.Collected.Manifests)
	require.Empty(t, rep.Collected.Forgotten)
	require.Subset(t, f.store.keys(), f.keysOf(g1), "nothing of the generation is deleted")
}

// TestAHistoryRowStampedSupersededWhileCurrent is R8-F3's other side: the
// current row agrees with the record and its manifest but carries a
// superseded_at no transition wrote. Its manifest verifies against it, so
// the action says collection resumes once a later publication replaces the
// generation; the collector never retires the current generation's row,
// whose grace would otherwise run while readers still choose it, and once a
// publication replaced it the row is retired then and expires like any
// other.
func TestAHistoryRowStampedSupersededWhileCurrent(t *testing.T) {
	f := newFixture(t)
	g1 := f.publishInit(t, f.initialized(t), 1)
	f.exec(t, "UPDATE %s SET superseded_at = 1 WHERE generation = 1", f.tables.Generation)

	rep := f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Equal(t, []FindingKind{HistoryMismatch}, kinds(rep.Findings))
	require.Equal(t, actionHistoryMismatchReplace, rep.Findings[0].Action)
	require.Empty(t, rep.Collected.Retired, "the current generation is not retired")
	waitOutGrace()
	require.Empty(t, f.pass(t, testOptions(true)).Collected.Retired)

	f.publish(t, f.stageFlush(t, g1, 1))
	rep = f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Findings)
	require.Equal(t, []int64{g1.rec.Generation}, rep.Collected.Retired, "retired once a publication replaced it")
	require.Empty(t, rep.Collected.Manifests, "kept for the grace after its retirement")
	require.Subset(t, f.store.keys(), f.keysOf(g1))

	waitOutGrace()
	rep = f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Findings)
	require.Equal(t, []string{g1.rec.ManifestKey}, rep.Collected.Manifests, "collection resumed")
}
