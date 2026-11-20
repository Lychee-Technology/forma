package reconcile

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/publication"
)

// TestAChangedRecordKeepsTheInventoryAReaderPinned is R10-F1, the side of
// R9-F3 where the record, not the history row, is what changed. A reader
// pins generation N through the record and verifies its manifest; then only
// the record is changed, to name a valid manifest a losing publication
// staged for N. The diagnosis reports history_mismatch. A normal
// publication, or a drop, replaces N and writes N's row back from the record
// it replaced, which names the losing manifest; the manifest the reader
// verified is then in no history row. The replacement kept it as a displaced
// inventory, so the collections that follow inside the reader's deadline
// delete nothing for the schema, although every key of N is old enough to be
// an orphan, and the reader still fetches and verifies what it pinned. Only
// once N's row expired, a full grace after a pass retired it, does a pass
// collect both of N's inventories.
func TestAChangedRecordKeepsTheInventoryAReaderPinned(t *testing.T) {
	for name, replace := range map[string]func(*testing.T, *fixture, publication.Record) []string{
		"publication": func(t *testing.T, f *fixture, cur publication.Record) []string {
			return f.keysOf(f.publishInit(t, cur, 1))
		},
		"drop": func(t *testing.T, f *fixture, cur publication.Record) []string {
			require.NoError(t, f.inTx(t, func(tx publication.Tx) error {
				_, err := publication.Reset(context.Background(), tx, f.tables, cur)
				return err
			}))
			f.ageLifecycle(t, time.Hour) // the drop's own lifecycle fence is not what keeps N
			return nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			opts := agedOptions(true)
			g1 := f.publishInit(t, f.initialized(t), 1)
			won, lost := f.stageFlush(t, g1, 1), f.stageFlush(t, g1, 1)
			g2 := f.publish(t, won)
			reader, pinned := f.pin(t)
			require.Equal(t, g2.rec.ManifestKey, pinned.ManifestKey, "the reader chose N's committed manifest")
			_, err := f.db.Exec(fmt.Sprintf("UPDATE %s SET manifest_key = $1, manifest_sha256 = $2, cas_token = $3 WHERE schema_id = $4",
				f.tables.Record), lost.next.ManifestKey, lost.next.ManifestSHA256, lost.next.Token, testSchema)
			require.NoError(t, err)

			rep := f.pass(t, agedOptions(false))
			require.NoError(t, rep.Err)
			require.Equal(t, []FindingKind{HistoryMismatch}, kinds(rep.Findings))
			require.Contains(t, rep.Findings[0].Detail, "so reads succeed", "the changed record names a valid manifest of N")

			remaining := replace(t, f, f.record(t))
			f.ageStamps(t, time.Hour) // only the displacement keeps N's objects, not their age
			for pass := range 2 {
				rep := f.pass(t, opts)
				require.NoError(t, rep.Err)
				require.Empty(t, rep.Findings, "pass %d", pass)
				require.Contains(t, rep.Refused, "disagreed with the record when the generation was replaced", "pass %d", pass)
				require.Contains(t, rep.Refused, g2.rec.ManifestKey, "pass %d names the displaced inventory", pass)
				require.Empty(t, rep.Collected.Objects, "pass %d", pass)
				require.Empty(t, rep.Collected.Manifests, "pass %d", pass)
				require.Empty(t, rep.Collected.Forgotten, "pass %d", pass)
				f.requireOrphansEligible(t, rep, opts, f.keysOf(g2))
				if pass == 0 {
					require.Contains(t, rep.Collected.Retired, g2.rec.Generation, "N is retired after its replacement")
				}
			}
			f.requireReadable(t, pinned)
			require.Subset(t, f.store.keys(), append(f.keysOf(g2), f.stagedKeys(lost, g1)...))
			require.NoError(t, reader.Commit())

			f.ageStamps(t, time.Hour) // N's row expired: no reader of either inventory is left
			rep = f.pass(t, opts)
			require.NoError(t, rep.Err)
			require.Empty(t, rep.Refused)
			require.Contains(t, rep.Collected.Manifests, g2.rec.ManifestKey, "the displaced manifest is collected as an expired one")
			require.Contains(t, rep.Collected.Manifests, lost.next.ManifestKey, "the row's manifest expired with N")
			slices.Sort(remaining)
			require.Equal(t, remaining, f.store.keys())
			displaced, err := publication.Displaced(context.Background(), publication.DB(f.db), f.tables, testSchema)
			require.NoError(t, err)
			require.Empty(t, displaced, "a displacement no longer live is forgotten")
		})
	}
}

// TestADroppedSchemaFinishesCollectingItsDisplacedInventory is R11-F1: the
// retention grace is shorter than the orphan age, as with the CLI's
// defaults (15 minutes and 2 hours). A drop replaces generation N, whose
// record had been changed to name another manifest, and keeps the row's
// inventory as displaced. Once N's row expired, nothing else of the schema
// is left to verify in the bucket, and the displaced manifest and the object
// only it lists are still younger than the orphan cutoff. The pass collects
// them as an expired generation's inventory, whatever their age, and
// forgets the displacement only after they are gone: a pass whose delete of
// the displaced manifest fails leaves the displacement, and the next pass,
// with no verified history manifest left, finishes the collection.
func TestADroppedSchemaFinishesCollectingItsDisplacedInventory(t *testing.T) {
	f := newFixture(t)
	opts := Options{GC: true, RetentionGrace: time.Minute, ReaderDeadline: 50 * time.Second,
		ClockMargin: 50 * time.Millisecond, OrphanAge: 2 * time.Hour}
	g1 := f.publishInit(t, f.initialized(t), 1)
	won, lost := f.stageFlush(t, g1, 1), f.stageFlush(t, g1, 1)
	g2 := f.publish(t, won)
	_, err := f.db.Exec(fmt.Sprintf("UPDATE %s SET manifest_key = $1, manifest_sha256 = $2, cas_token = $3 WHERE schema_id = $4",
		f.tables.Record), lost.next.ManifestKey, lost.next.ManifestSHA256, lost.next.Token, testSchema)
	require.NoError(t, err)
	require.NoError(t, f.inTx(t, func(tx publication.Tx) error {
		_, err := publication.Reset(context.Background(), tx, f.tables, f.record(t))
		return err
	}))
	f.ageLifecycle(t, time.Hour)

	rep := f.pass(t, opts)
	require.NoError(t, rep.Err)
	require.Contains(t, rep.Refused, g2.rec.ManifestKey, "the displaced inventory is live inside the grace")
	require.ElementsMatch(t, []int64{g1.rec.Generation, g2.rec.Generation}, rep.Collected.Retired)

	f.ageRetirements(t, time.Hour) // past the grace, while every object is younger than the orphan age
	displacedOnly := f.stagedKeys(won, g1)
	f.store.failDelete = func(key string) error {
		if key == g2.rec.ManifestKey {
			return errors.New("injected delete failure")
		}
		return nil
	}
	rep = f.pass(t, opts)
	require.ErrorContains(t, rep.Err, "delete the expired manifests")
	cutoff := publication.OrphanCutoff(rep.Horizon, opts.ClockMargin)
	listed, err := f.store.List(context.Background(), "")
	require.NoError(t, err)
	for _, o := range listed {
		require.GreaterOrEqual(t, o.LastModified, cutoff, "%s is not old enough to be collected as an orphan", o.Key)
	}
	require.Subset(t, rep.Collected.Objects, displacedOnly[1:], "the object only the displaced inventory lists, whatever its age")
	require.Equal(t, []string{g2.rec.ManifestKey}, f.store.keys(), "only the displaced manifest whose delete failed is left")
	displaced, err := publication.Displaced(context.Background(), publication.DB(f.db), f.tables, testSchema)
	require.NoError(t, err)
	require.Len(t, displaced, 1, "a displacement is not forgotten before its manifest is deleted")

	f.store.failDelete = nil
	rep = f.pass(t, opts)
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Refused, "the displacement's verified manifest is the schema's evidence in the bucket")
	require.Equal(t, []string{g2.rec.ManifestKey}, rep.Collected.Manifests)
	require.Empty(t, f.store.keys(), "the drop's collection finished")
	displaced, err = publication.Displaced(context.Background(), publication.DB(f.db), f.tables, testSchema)
	require.NoError(t, err)
	require.Empty(t, displaced, "forgotten once its manifest is gone")

	rep = f.pass(t, opts)
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Findings)
	require.Empty(t, rep.Refused, "nothing is left to delete, and nothing is refused")
	require.Empty(t, rep.Plan.Objects)
	require.Empty(t, rep.Plan.Manifests)
}

// ageRetirements moves every retirement stamp of the schema's history back
// by d, leaving the objects' LastModified as they are.
func (f *fixture) ageRetirements(t *testing.T, d time.Duration) {
	t.Helper()
	_, err := f.db.Exec(fmt.Sprintf("UPDATE %s SET retired_at = retired_at - $1 WHERE schema_id = $2 AND retired_at <> 0",
		f.tables.Generation), d.Milliseconds(), testSchema)
	require.NoError(t, err)
}
