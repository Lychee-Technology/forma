package reconcile

import (
	"context"
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
			require.Contains(t, rep.Collected.Objects, g2.rec.ManifestKey, "the displaced manifest is an orphan now")
			require.Contains(t, rep.Collected.Manifests, lost.next.ManifestKey, "the row's manifest expired with N")
			slices.Sort(remaining)
			require.Equal(t, remaining, f.store.keys())
			displaced, err := publication.Displaced(context.Background(), publication.DB(f.db), f.tables, testSchema)
			require.NoError(t, err)
			require.Empty(t, displaced, "a displacement no longer live is forgotten")
		})
	}
}
