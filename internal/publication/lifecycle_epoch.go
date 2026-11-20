package publication

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/lychee-technology/forma/internal/sqlgen"
)

// ErrEpochReused reports a transition that would write a record at an epoch
// a record of the same schema id carried before: the id's lifecycle epoch
// (the epoch table, epochDDL) is at it or above it already. A process may
// still pin that epoch together with metadata of the registration that held
// it, and would take the new record for its own, so the transition's
// transaction must not commit. Only a record that went back, restored from an
// older state or written outside this package, meets it.
var ErrEpochReused = errors.New("publication epoch already used by the schema id")

// ErrLifecycleLost reports a schema id whose record is missing while its
// generation history survives, and whose lifecycle epoch is missing too. Its
// last epoch cannot be told: drops and re-registrations move it without a
// generation row, so the history's highest epoch is only a floor, and a
// record recovered above that floor could carry an epoch a running process
// still pins. Recovery is the operator's (docs/publication-protocol-v2.md, "A
// lost record").
var ErrLifecycleLost = errors.New("publication record and lifecycle epoch both lost")

// raiseEpochSQL raises schema $1's lifecycle epoch to $2 without touching
// forgotten_generation, recovery_epoch or recovery_retired_at, so normal
// transitions (Initialize, Reset, Activate) never weaken a recovery fence.
const raiseEpochSQL = `INSERT INTO %s AS e (schema_id, epoch, updated_at)
	VALUES ($1, $2, ` + sqlgen.ServerTimeMillisSQL + `)
	ON CONFLICT (schema_id) DO UPDATE SET epoch = EXCLUDED.epoch, updated_at = EXCLUDED.updated_at
	WHERE e.epoch < EXCLUDED.epoch`

// raiseEpochAfterLossSQL raises schema $1's lifecycle epoch to $2 and arms the
// schema-wide recovery fence at recovery_epoch = $2 with recovery_retired_at = 0.
// Like Retire for generations, the collector stamps recovery_retired_at with
// now() only after observing the committed recovery in a snapshot (RetireRecovery),
// so the recovery grace never counts from a transaction's start time and needs
// no clock_timestamp(). forgotten_generation is preserved untouched.
const raiseEpochAfterLossSQL = `INSERT INTO %s AS e (schema_id, epoch, updated_at, recovery_epoch, recovery_retired_at)
	VALUES ($1, $2, ` + sqlgen.ServerTimeMillisSQL + `, $2, 0)
	ON CONFLICT (schema_id) DO UPDATE SET
		epoch = EXCLUDED.epoch,
		updated_at = EXCLUDED.updated_at,
		recovery_epoch = EXCLUDED.recovery_epoch,
		recovery_retired_at = 0
	WHERE e.epoch < EXCLUDED.epoch`

// raiseEpoch records, on r, that schemaID's record now carries epoch, in the
// transaction that moved the record there. It refuses (ErrEpochReused) when
// a record of the id carried epoch or a later one before, and the caller's
// transaction then must not commit.
func raiseEpoch(ctx context.Context, r runner, t Tables, schemaID int16, epoch int64) error {
	return execRaiseEpoch(ctx, r, raiseEpochSQL, t, schemaID, epoch)
}

// raiseEpochAfterLoss raises schemaID's lifecycle epoch to epoch and arms the
// post-commit recovery fence for InitializeAfterLoss.
func raiseEpochAfterLoss(ctx context.Context, r runner, t Tables, schemaID int16, epoch int64) error {
	return execRaiseEpoch(ctx, r, raiseEpochAfterLossSQL, t, schemaID, epoch)
}

func execRaiseEpoch(ctx context.Context, r runner, query string, t Tables, schemaID int16, epoch int64) error {
	n, err := r.exec(ctx, fmt.Sprintf(query, t.Epoch), schemaID, epoch)
	if err != nil {
		return fmt.Errorf("raise the lifecycle epoch of schema %d to %d in %s: %w", schemaID, epoch, t.Epoch, err)
	}
	if n != 1 {
		return fmt.Errorf("raise the lifecycle epoch of schema %d to %d in %s: the id's lifecycle epoch is %d or later already, "+
			"so a record of it carried the epoch before and a running process may still pin it: %w", schemaID, epoch, t.Epoch, epoch, ErrEpochReused)
	}
	return nil
}

// Lifecycle is what the collector reads from the lifecycle epoch table in its
// snapshot: the highest epoch the schema id carried, the highest generation
// number legitimately forgotten by GC, and any active lost-record recovery
// fence.
type Lifecycle struct {
	Present             bool
	Epoch               int64
	ForgottenGeneration int64
	RecoveryEpoch       int64
	RecoveryRetiredAt   int64
	// RecoveryActive is true while a lost-record recovery has not yet been
	// retired and waited out for the full retention grace.
	RecoveryActive bool
}

const (
	readLifecycleSQL = `SELECT epoch,
		COALESCE(forgotten_generation, 0),
		COALESCE(recovery_epoch, 0),
		COALESCE(recovery_retired_at, 0),
		COALESCE(recovery_epoch, 0) > 0 AND (
			COALESCE(recovery_retired_at, 0) = 0
			OR COALESCE(recovery_retired_at, 0) > ` + sqlgen.ServerTimeMillisSQL + ` - $2
		)
	FROM %s WHERE schema_id = $1`
	retireRecoverySQL = `UPDATE %s SET recovery_retired_at = ` + sqlgen.ServerTimeMillisSQL + `
	WHERE schema_id = $1 AND recovery_epoch = $2 AND COALESCE(recovery_retired_at, 0) = 0`
)

// ReadLifecycle answers the lifecycle epoch, forgotten floor and recovery
// fence of schemaID on q under grace (compared in full via GraceMillis). An id
// without a lifecycle row answers Present == false.
func ReadLifecycle(ctx context.Context, q Querier, t Tables, schemaID int16, grace time.Duration) (Lifecycle, error) {
	var l Lifecycle
	err := q.runner().queryRow(ctx, fmt.Sprintf(readLifecycleSQL, t.Epoch), schemaID, GraceMillis(grace)).
		Scan(&l.Epoch, &l.ForgottenGeneration, &l.RecoveryEpoch, &l.RecoveryRetiredAt, &l.RecoveryActive)
	if errors.Is(err, sql.ErrNoRows) {
		return Lifecycle{}, nil
	}
	if err != nil {
		return Lifecycle{}, fmt.Errorf("read lifecycle of schema %d in %s: %w", schemaID, t.Epoch, err)
	}
	l.Present = true
	return l, nil
}

// RetireRecovery stamps recovery_retired_at with the database time once for
// recoveryEpoch. Run it on a pool after the snapshot that saw recoveryEpoch
// committed has ended: like Retire, that ensures the recovery grace counts
// strictly from after the recovery committed. A later recovery raises
// recovery_epoch and resets recovery_retired_at to 0, so a stale call for an
// older recoveryEpoch matches nothing.
func RetireRecovery(ctx context.Context, q Querier, t Tables, schemaID int16, recoveryEpoch int64) (bool, error) {
	if recoveryEpoch <= 0 {
		return false, nil
	}
	n, err := q.runner().exec(ctx, fmt.Sprintf(retireRecoverySQL, t.Epoch), schemaID, recoveryEpoch)
	if err != nil {
		return false, fmt.Errorf("retire recovery epoch %d of schema %d in %s: %w", recoveryEpoch, schemaID, t.Epoch, err)
	}
	return n == 1, nil
}
