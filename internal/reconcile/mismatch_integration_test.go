package reconcile

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/publication"
)

type historyDamageCase struct {
	name     string
	damage   func(t *testing.T, f *fixture, g2 generation, lost staged)
	wantKind FindingKind
}

// TestHistoryDisagreementFailsClosedAndRecoversAtFreshEpoch verifies that when
// the current generation's history row is missing, has a mismatched field, or
// carries an early superseded_at / retired_at stamp:
//   - Diagnosis verifies the record's manifest as readers do, reports
//     HistoryMismatch (or MissingGeneration for missing row), and refuses GC.
//   - Publish and Reset fail closed with publication.ErrHistoryInconsistent.
//   - Fresh-epoch recovery (delete record -> InitializeAfterLoss -> cdc-init)
//     protects any pinned reader through the recovery fence + grace G and then
//     lets GC clean up everything cleanly.
func TestHistoryDisagreementFailsClosedAndRecoversAtFreshEpoch(t *testing.T) {
	cases := []historyDamageCase{
		{
			name: "digest disagrees",
			damage: func(t *testing.T, f *fixture, _ generation, _ staged) {
				f.exec(t, "UPDATE %s SET manifest_sha256 = '"+strings.Repeat("0", 64)+"' WHERE generation = 2", f.tables.Generation)
			},
			wantKind: HistoryMismatch,
		},
		{
			name: "row names another manifest",
			damage: func(t *testing.T, f *fixture, g2 generation, lost staged) {
				_, err := f.db.Exec(fmt.Sprintf("UPDATE %s SET manifest_key = $1, manifest_sha256 = $2, cas_token = $3 WHERE generation = $4",
					f.tables.Generation), lost.next.ManifestKey, lost.next.ManifestSHA256, lost.next.Token, g2.rec.Generation)
				require.NoError(t, err)
			},
			wantKind: HistoryMismatch,
		},
		{
			name: "row stamped superseded while current",
			damage: func(t *testing.T, f *fixture, _ generation, _ staged) {
				f.exec(t, "UPDATE %s SET superseded_at = 1 WHERE generation = 2", f.tables.Generation)
			},
			wantKind: HistoryMismatch,
		},
		{
			name: "row stamped retired early while current",
			damage: func(t *testing.T, f *fixture, g2 generation, _ staged) {
				_, err := f.db.Exec(fmt.Sprintf("UPDATE %s SET retired_at = $1 WHERE generation = $2", f.tables.Generation),
					f.serverMillis(t)-2*time.Hour.Milliseconds(), g2.rec.Generation)
				require.NoError(t, err)
			},
			wantKind: HistoryMismatch,
		},
		{
			name: "current row deleted",
			damage: func(t *testing.T, f *fixture, _ generation, _ staged) {
				f.exec(t, "DELETE FROM %s WHERE generation = 2", f.tables.Generation)
			},
			wantKind: MissingGeneration,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			verifyHistoryDisagreementCase(t, tc)
		})
	}
}

func verifyHistoryDisagreementCase(t *testing.T, tc historyDamageCase) {
	t.Helper()
	f := newFixture(t)
	ctx := context.Background()
	opts := agedOptions(true)
	g1 := f.publishInit(t, f.initialized(t), 1)
	won, lost := f.stageFlush(t, g1, 1), f.stageFlush(t, g1, 1)
	g2 := f.publish(t, won)
	tc.damage(t, f, g2, lost)

	// Diagnosis checks the record's manifest as readers do.
	diag := f.pass(t, agedOptions(false))
	require.NoError(t, diag.Err)
	require.Equal(t, []FindingKind{tc.wantKind}, kinds(diag.Findings))
	require.Equal(t, actionHistoryMismatch, diag.Findings[0].Action)
	require.Contains(t, diag.Findings[0].Detail, readsManifestVerified)
	require.NotEmpty(t, diag.Refused)

	// Corrupting the record's manifest is caught on the record's key.
	intact, err := f.store.Get(ctx, g2.rec.ManifestKey)
	require.NoError(t, err)
	f.corrupt(t, g2.rec.ManifestKey)
	badDiag := f.pass(t, agedOptions(false))
	require.NoError(t, badDiag.Err)
	require.Equal(t, []FindingKind{tc.wantKind, InvalidManifest}, kinds(badDiag.Findings))
	require.Equal(t, g2.rec.ManifestKey, badDiag.Findings[1].Key)
	require.Contains(t, badDiag.Findings[0].Detail, readsUnverified)
	f.store.set(g2.rec.ManifestKey, intact, f.serverMillis(t))

	// Publish and Reset fail closed with ErrHistoryInconsistent.
	_, err = f.commit(t, f.stageFlush(t, g2, 1))
	require.ErrorIs(t, err, publication.ErrHistoryInconsistent)
	require.False(t, publication.Conflict(err))
	err = f.inTx(t, func(tx publication.Tx) error {
		_, err := publication.Reset(ctx, tx, f.tables, g2.rec)
		return err
	})
	require.ErrorIs(t, err, publication.ErrHistoryInconsistent)

	// GC deletes nothing and never retires the current generation.
	before := f.store.keys()
	gcRep := f.pass(t, opts)
	require.NoError(t, gcRep.Err)
	require.NotEmpty(t, gcRep.Refused)
	require.NotContains(t, gcRep.Collected.Retired, g2.rec.Generation)
	require.Equal(t, before, f.store.keys())

	// Fresh-epoch recovery protects a pinned reader through grace G, then cleans up.
	reader, err := f.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = reader.Rollback() }()
	pinned, err := publication.Read(ctx, publication.SQLTx(reader), f.tables, testSchema)
	require.NoError(t, err)

	f.exec(t, "DELETE FROM %s", f.tables.Record)
	recoverLostRecord(t, f)
	reinit := f.publishInit(t, f.record(t), 1)
	require.Greater(t, reinit.rec.Epoch, g2.rec.Epoch)

	f.ageStamps(t, time.Hour)
	rep := f.pass(t, opts)
	require.NoError(t, rep.Err)
	require.True(t, rep.Collected.RecoveryRetired)
	if tc.name == "digest disagrees" {
		require.Contains(t, rep.Refused, string(InvalidManifest))
	} else {
		require.Contains(t, rep.Refused, "recovery fence")
	}
	require.Empty(t, rep.Collected.Objects)
	requirePinnedIntact(t, f, pinned.Record)
	require.NoError(t, reader.Commit())

	f.ageStamps(t, time.Hour)
	f.ageRecovery(t, time.Hour)
	rep = f.pass(t, opts)
	require.NoError(t, rep.Err)
	if tc.name == "digest disagrees" {
		require.Equal(t, []FindingKind{InvalidManifest}, kinds(rep.Findings))
		require.Equal(t, g2.rec.Generation, rep.Findings[0].Generation)
		return
	}
	require.Empty(t, rep.Refused)
	require.Equal(t, f.keysOf(reinit), f.store.keys())
}
