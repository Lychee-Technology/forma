package internal

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// What a claim holds, and what it answers while others hold rows (#653), as
// docs/claims.md states it. Like the concurrency proofs, these rely on
// PostgreSQL's advisory locks and non-blocking skip, so each is
// _PostgresOnly.

// versionLockHeld probes, from another transaction, whether rowID's per-row
// advisory lock is taken.
func (e *outboxPG) versionLockHeld(t *testing.T, rowID uuid.UUID) bool {
	t.Helper()
	probe, err := e.pool.Begin(e.ctx)
	require.NoError(t, err)
	defer func() { _ = probe.Rollback(e.ctx) }()
	var free bool
	require.NoError(t, probe.QueryRow(e.ctx, "SELECT pg_try_advisory_xact_lock($1)", rowVersionLockKey(outboxSchemaID, rowID)).Scan(&free))
	return !free
}

// lockRowOutsideForma takes rowID's entity_main row lock in a raw
// transaction, without the advisory lock every Forma writer takes, and
// answers that transaction; the test rolls it back.
func (e *outboxPG) lockRowOutsideForma(t *testing.T, rowID uuid.UUID) pgx.Tx {
	t.Helper()
	raw, err := e.pool.Begin(e.ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Rollback(e.ctx) })
	_, err = raw.Exec(e.ctx, fmt.Sprintf("SELECT 1 FROM %s WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2 FOR UPDATE",
		sanitizeIdentifier(e.tables.EntityMain)), outboxSchemaID, rowID)
	require.NoError(t, err)
	return raw
}

// holdingClaim runs req in a Tx that stays open until release, and fails
// the Tx unless the claim took want.
func (e *outboxPG) holdingClaim(req *forma.ClaimRequest, want []uuid.UUID) (held <-chan struct{}, release func(), finished <-chan error) {
	return e.holdingTx(func(ctx context.Context, tx forma.Tx) error {
		result, err := tx.Claim(ctx, req)
		if err == nil && !slices.Equal(rowIDsOf(result.Records), want) {
			err = fmt.Errorf("the claim took %v, not %v", rowIDsOf(result.Records), want)
		}
		return err
	})
}

// A candidate whose advisory lock the claim got, but which the re-check
// turned away, keeps that lock until the claim's Tx ends; a candidate the
// claim never tried is not locked. With Limit 2 over four candidates, r1 is
// turned away, r2 and r3 are claimed, and r4 is never tried.
func TestClaimKeepsTheLockOfACandidateItTurnedAway_PostgresOnly(t *testing.T) {
	turnAway := map[string]func(t *testing.T, env *outboxPG, r1 uuid.UUID){
		"dropped, having stopped matching": func(t *testing.T, env *outboxPG, r1 uuid.UUID) {
			env.txr.testHooks = &txTestHooks{afterCandidates: func([]uuid.UUID) {
				_, err := env.em.Update(env.ctx, complete(r1, 0))
				assert.NoError(t, err)
			}}
			t.Cleanup(func() { env.txr.testHooks = nil })
		},
		"skipped for a row lock held outside Forma": func(t *testing.T, env *outboxPG, r1 uuid.UUID) {
			env.lockRowOutsideForma(t, r1)
		},
	}
	for name, setup := range turnAway {
		t.Run(name, func(t *testing.T) {
			env := newOutboxPG(t)
			ids := env.createMessages(t, "t", 4)
			setup(t, env, ids[0])
			held, release, holder := env.holdingClaim(claimOf("t", "B", 2), ids[1:3])
			defer release()
			await(t, held, holder, "the claim")

			assert.True(t, env.versionLockHeld(t, ids[0]), "r1 keeps the lock the claim got")
			assert.True(t, env.versionLockHeld(t, ids[1]), "a claimed row is held")
			assert.False(t, env.versionLockHeld(t, ids[3]), "a candidate never tried is not locked")
			release()
			require.NoError(t, <-holder)
			assert.False(t, env.versionLockHeld(t, ids[0]), "the lock ends with the claim's Tx")
		})
	}
}

// Open holders can starve a claim: while A and B hold the first 2 × Limit
// eligible rows, C answers none without waiting, though 20 eligible rows lie
// beyond them. Once A and B commit, C claims the rows after theirs.
func TestClaimAnswersNoneWhileOthersHoldItsCandidates_PostgresOnly(t *testing.T) {
	env := newOutboxPG(t)
	ids := env.createMessages(t, "t", 30)
	var holders []<-chan error
	var releases []func()
	for i, worker := range []string{"A", "B"} {
		held, release, holder := env.holdingClaim(claimLeasing("t", worker, 5, heldLease), ids[5*i:5*(i+1)])
		defer release()
		await(t, held, holder, worker+"'s claim")
		holders, releases = append(holders, holder), append(releases, release)
	}

	records, waited := env.claimWatched(t, claimOf("t", "C", 5))
	assert.Empty(t, rowIDsOf(records), "each of C's ten candidates is held")
	assert.False(t, waited, "C's backend never had an ungranted lock")

	for i, release := range releases {
		release()
		require.NoError(t, <-holders[i])
	}
	records, err := env.claim(claimOf("t", "C", 5))
	require.NoError(t, err)
	assert.Equal(t, ids[10:15], rowIDsOf(records), "once the holders commit, C claims past their rows")
}
