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
// collection runs. With the manifest intact, the row's finding says the
// manifest verifies, and only that (R11-F3): an object the current manifest
// lists that S3 lost is reported against the current generation, with the
// re-init that restores reads, and nothing in the report says reads
// succeed. With the manifest deleted too, its loss is reported beside the
// row's finding, each with its own recovery, and neither is left out as
// concurrent collection: the record still names the generation, so nothing
// could have collected it.
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
			require.Contains(t, rep.Findings[0].Detail, readsManifestVerified, "the manifest verifies")
			require.Contains(t, rep.Findings[0].Detail, "the manifest only, not that the objects it lists are available")
			require.Equal(t, lost, rep.Findings[1].Key)
			require.True(t, rep.Findings[1].Current)
			require.Equal(t, actionLostCurrent, rep.Findings[1].Action, "but the data it lists is not")
			require.Contains(t, rep.Findings[1].Action, "reads fail closed")
			var out bytes.Buffer
			Report{Schemas: []SchemaReport{rep}}.Render(&out)
			require.NotContains(t, out.String(), "reads succeed", "no part of the report says the reads succeed")
			require.NotContains(t, out.String(), "fail closed only if", "nor that only the manifest decides it")

			f.store.remove(g1.rec.ManifestKey)
			rep = f.pass(t, testOptions(false))
			require.NoError(t, rep.Err)
			require.Zero(t, rep.ConcurrentlyExpired, "the record still names the generation: nothing collected it")
			require.Equal(t, []FindingKind{tc.rowKind, MissingGeneration}, kinds(rep.Findings))
			require.Equal(t, tc.rowAction, rep.Findings[0].Action, "the row's own recovery")
			require.Contains(t, rep.Findings[0].Detail, readsUnverified)
			manifest := rep.Findings[1]
			require.Equal(t, g1.rec.ManifestKey, manifest.Key)
			require.True(t, manifest.Current)
			require.Equal(t, "the manifest object is missing", manifest.Detail)
			require.Equal(t, actionMissingGeneration, manifest.Action, "the manifest's loss has the re-init as its recovery")
			require.Empty(t, rep.Plan.Objects, "a refused diagnosis plans nothing")
		})
	}
}

// TestDiagnosisReportsALossAReplacementCannotHaveCollected is R11-F2: the
// diagnosis's snapshot sees generation N current, with its history row and
// its manifest both missing. While the manifest is read, N+1 publishes, and
// no collection runs. The record no longer names N, and N has no retained
// row, yet nothing collected N: its row is above the history's expiry
// floor, which did not move, and the gap it leaves stops every collection.
// So the manifest's loss is reported, not left out, and the row's finding
// points to a finding the report holds.
func TestDiagnosisReportsALossAReplacementCannotHaveCollected(t *testing.T) {
	f := newFixture(t)
	g1 := f.publishInit(t, f.initialized(t), 1)
	f.exec(t, "DELETE FROM %s WHERE generation = 1", f.tables.Generation)
	f.store.remove(g1.rec.ManifestKey)
	fired := false
	f.store.beforeGet = func(string) {
		if !fired {
			fired = true
			f.publishCompact(t, g1)
		}
	}

	rep := f.pass(t, testOptions(false))
	require.True(t, fired)
	require.NoError(t, rep.Err)
	require.Equal(t, g1.rec.Generation, rep.Generation, "the snapshot saw N current")
	require.Zero(t, rep.ConcurrentlyExpired, "a replacement alone is no evidence that N expired")
	require.Equal(t, []FindingKind{MissingGeneration, MissingGeneration}, kinds(rep.Findings))
	require.Equal(t, actionMissingHistory, rep.Findings[0].Action)
	require.Contains(t, rep.Findings[0].Detail, readsUnverified)
	require.Equal(t, g1.rec.ManifestKey, rep.Findings[1].Key, "the finding the row's detail points to")
	require.Equal(t, actionMissingGeneration, rep.Findings[1].Action)
	require.False(t, rep.Inconclusive())
}

// TestDiagnosisLeavesOutARecordManifestCollectedBelowTheFloor is R11-F2's
// other side: the same snapshot, N current with its row missing, but while
// the diagnosis reads two publications replace N and N+1, a collection
// retires N+1 and, a grace later, another finds it expired. N+1 is then the
// history's floor, N lies below it, and that collection deleted N's
// manifest, which no row names, as an orphan. The floor rose past N while
// the diagnosis ran, so the manifest's loss is left out, and the row's
// finding says so rather than pointing to the finding left out.
func TestDiagnosisLeavesOutARecordManifestCollectedBelowTheFloor(t *testing.T) {
	f := newFixture(t)
	g1 := f.publishInit(t, f.initialized(t), 1)
	f.exec(t, "DELETE FROM %s WHERE generation = 1", f.tables.Generation)
	fired := false
	var collected SchemaReport
	f.store.beforeGet = func(string) {
		if fired {
			return
		}
		fired = true
		g2 := f.publishCompact(t, g1)
		f.publishCompact(t, g2)
		require.Equal(t, []int64{g2.rec.Generation}, f.pass(t, testOptions(true)).Collected.Retired)
		f.ageStamps(t, time.Hour)
		collected = f.pass(t, testOptions(true))
	}

	rep := f.pass(t, testOptions(false))
	require.True(t, fired)
	require.NoError(t, collected.Err)
	require.Empty(t, collected.Refused)
	require.Contains(t, collected.Collected.Objects, g1.rec.ManifestKey, "N's manifest was collected as an orphan below the floor")
	require.NoError(t, rep.Err)
	require.Equal(t, 1, rep.ConcurrentlyExpired)
	require.Equal(t, []FindingKind{MissingGeneration}, kinds(rep.Findings), "the row's finding is kept, the manifest's left out")
	require.Equal(t, actionMissingHistory, rep.Findings[0].Action)
	require.Contains(t, rep.Findings[0].Detail, readsCollected)
	require.NotContains(t, rep.Findings[0].Detail, readsUnverified, "no pointer to the finding left out")
	require.True(t, rep.Inconclusive())
}
