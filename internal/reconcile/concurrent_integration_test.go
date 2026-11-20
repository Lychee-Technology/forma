package reconcile

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/sqlgen"
)

// TestDiagnosisLeavesOutACurrentGenerationCollectedBesideIt: the generation
// the diagnosis's snapshot saw current is, while the diagnosis still reads,
// replaced by a compaction, retired by one collection and, a grace later,
// collected by another, which deletes its manifest and every object only it
// listed. The diagnosis reports neither the missing manifest nor the lost
// objects of that generation, says how many findings it left out, and
// recommends no re-init.
func TestDiagnosisLeavesOutACurrentGenerationCollectedBesideIt(t *testing.T) {
	for _, tc := range []struct {
		name string
		// checksums hashes the objects, so the collection runs between
		// the manifest's read and the objects' hashes; otherwise it runs
		// before the manifest's read.
		checksums bool
		left      int
	}{
		{"before its manifest is read", false, 1},
		{"before its objects are hashed", true, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			g1 := f.publishInit(t, f.initialized(t), 2)
			fired := false
			var collected SchemaReport
			expire := func(string) {
				if fired {
					return
				}
				fired = true
				f.publishCompact(t, g1)
				require.Equal(t, []int64{1}, f.pass(t, testOptions(true)).Collected.Retired)
				waitOutGrace()
				collected = f.pass(t, testOptions(true))
			}
			opts := testOptions(false)
			opts.VerifyChecksums = tc.checksums
			if tc.checksums {
				f.store.beforeHash = expire
			} else {
				f.store.beforeGet = expire
			}

			rep := f.pass(t, opts)
			require.True(t, fired)
			require.NoError(t, collected.Err)
			require.Equal(t, []string{g1.rec.ManifestKey}, collected.Collected.Manifests,
				"the collection beside the diagnosis deleted the snapshot-current generation's manifest")
			require.ElementsMatch(t, g1.manifest.Keys(), collected.Collected.Objects, "and the objects only it listed")
			require.NoError(t, rep.Err)
			require.Equal(t, g1.rec.Generation, rep.Generation, "the diagnosis's snapshot saw generation 1 current")
			require.Empty(t, rep.Findings, "what the collection deleted is not reported as loss")
			require.Equal(t, tc.left, rep.ConcurrentlyExpired)
			require.Contains(t, rep.Refused, "run the diagnosis again")
			require.True(t, rep.Inconclusive())
			require.True(t, rep.Residual(), "an inconclusive diagnosis is not clean")
			var out bytes.Buffer
			Report{Schemas: []SchemaReport{rep}}.Render(&out)
			require.NotContains(t, out.String(), "cdc-init", "no re-init is recommended")
			require.Contains(t, out.String(), "finding(s) left out: the generation they concerned expired while the diagnosis ran")
			require.Contains(t, out.String(), "inconclusive (run the diagnosis again)")
			require.NotContains(t, out.String(), ": clean (")
		})
	}
}

// TestDiagnosisReportsACurrentLossNoCollectionCaused: the recheck leaves out
// only what a collection could have deleted. A current generation that is
// still current after the reads keeps its missing manifest, and a missing
// history row, which the snapshot read with the record, is always reported.
func TestDiagnosisReportsACurrentLossNoCollectionCaused(t *testing.T) {
	f := newFixture(t)
	g1 := f.publishInit(t, f.initialized(t), 1)
	f.store.remove(g1.rec.ManifestKey)
	rep := f.pass(t, testOptions(false))
	require.NoError(t, rep.Err)
	require.Equal(t, []FindingKind{MissingGeneration}, kinds(rep.Findings))
	require.Equal(t, actionMissingGeneration, rep.Findings[0].Action)
	require.Zero(t, rep.ConcurrentlyExpired)

	f = newFixture(t)
	g1 = f.publishInit(t, f.initialized(t), 1)
	f.exec(t, "DELETE FROM %s WHERE generation = 1", f.tables.Generation)
	rep = f.pass(t, testOptions(false))
	require.NoError(t, rep.Err)
	require.Equal(t, []FindingKind{MissingGeneration}, kinds(rep.Findings))
	require.Equal(t, actionMissingHistory, rep.Findings[0].Action)
	require.True(t, rep.Findings[0].Current)
	require.Equal(t, g1.rec.Generation, rep.Findings[0].Generation)
	require.Zero(t, rep.ConcurrentlyExpired)
}

// TestDiagnosisReportsCurrentLossWhateverItsHistoryRow is R10-F3: the
// diagnosis checks what the record publishes independently of the history
// row of its generation, which readers never read. The row is either
// stamped retired long ago, outside the protocol, or deleted, and no
// collection runs. With the manifest intact, the row's finding says reads
// succeed, and an object the current manifest lists that S3 lost is reported
// against the current generation. With the manifest deleted too, its loss is
// reported beside the row's finding, each with its own recovery, and neither
// is left out as concurrent collection: the record still names the
// generation, so nothing could have collected it.
func TestDiagnosisReportsCurrentLossWhateverItsHistoryRow(t *testing.T) {
	for _, tc := range []struct {
		name      string
		damage    string
		rowKind   FindingKind
		rowAction string
	}{
		{"row stamped retired", "UPDATE %s SET retired_at = " + sqlgen.ServerTimeMillisSQL + " WHERE generation = 1", HistoryMismatch, actionHistoryMismatch},
		{"row deleted", "DELETE FROM %s WHERE generation = 1", MissingGeneration, actionMissingHistory},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			g1 := f.publishInit(t, f.initialized(t), 2)
			f.exec(t, tc.damage, f.tables.Generation)
			f.ageStamps(t, 2*time.Hour) // a retired_at stamped so is past the grace
			lost := g1.manifest.Keys()[0]
			f.store.remove(lost)

			rep := f.pass(t, testOptions(false))
			require.NoError(t, rep.Err)
			require.Zero(t, rep.ConcurrentlyExpired, "no collection ran")
			require.Equal(t, []FindingKind{tc.rowKind, LostObject}, kinds(rep.Findings))
			require.Equal(t, tc.rowAction, rep.Findings[0].Action)
			require.Contains(t, rep.Findings[0].Detail, "so reads succeed")
			require.Equal(t, lost, rep.Findings[1].Key)
			require.True(t, rep.Findings[1].Current)
			require.Equal(t, actionLostCurrent, rep.Findings[1].Action)

			f.store.remove(g1.rec.ManifestKey)
			rep = f.pass(t, testOptions(false))
			require.NoError(t, rep.Err)
			require.Zero(t, rep.ConcurrentlyExpired, "the record still names the generation: nothing collected it")
			require.Equal(t, []FindingKind{tc.rowKind, MissingGeneration}, kinds(rep.Findings))
			require.Equal(t, tc.rowAction, rep.Findings[0].Action, "the row's own recovery")
			require.Contains(t, rep.Findings[0].Detail, "could not verify the manifest the record names")
			manifest := rep.Findings[1]
			require.Equal(t, g1.rec.ManifestKey, manifest.Key)
			require.True(t, manifest.Current)
			require.Equal(t, "the manifest object is missing", manifest.Detail)
			require.Equal(t, actionMissingGeneration, manifest.Action, "the manifest's loss has the re-init as its recovery")
			require.Empty(t, rep.Plan.Objects, "a refused diagnosis plans nothing")
		})
	}
}
