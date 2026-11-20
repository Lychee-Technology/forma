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

// raiseEpochSQL raises schema $1's lifecycle epoch to $2. It writes nothing
// when the id's lifecycle epoch is $2 or above already.
const raiseEpochSQL = `INSERT INTO %s AS e (schema_id, epoch, updated_at)
	VALUES ($1, $2, ` + sqlgen.ServerTimeMillisSQL + `)
	ON CONFLICT (schema_id) DO UPDATE SET epoch = EXCLUDED.epoch, updated_at = EXCLUDED.updated_at
	WHERE e.epoch < EXCLUDED.epoch`

// raiseEpoch records, on r, that schemaID's record now carries epoch, in the
// transaction that moved the record there. It refuses (ErrEpochReused) when
// a record of the id carried epoch or a later one before, and the caller's
// transaction then must not commit.
func raiseEpoch(ctx context.Context, r runner, t Tables, schemaID int16, epoch int64) error {
	n, err := r.exec(ctx, fmt.Sprintf(raiseEpochSQL, t.Epoch), schemaID, epoch)
	if err != nil {
		return fmt.Errorf("raise the lifecycle epoch of schema %d to %d in %s: %w", schemaID, epoch, t.Epoch, err)
	}
	if n != 1 {
		return fmt.Errorf("raise the lifecycle epoch of schema %d to %d in %s: the id's lifecycle epoch is %d or later already, "+
			"so a record of it carried the epoch before and a running process may still pin it: %w", schemaID, epoch, t.Epoch, epoch, ErrEpochReused)
	}
	return nil
}

// LifecycleFence is when a schema id's lifecycle epoch last moved: its epoch
// row's updated_at, which every transition that moves a record's epoch
// stamps, and a lost-record recovery stamps after the loss (stampRecovery).
type LifecycleFence struct {
	// Present says the id has a lifecycle epoch row.
	Present bool
	// MovedAt is the row's updated_at, epoch milliseconds on the database
	// clock.
	MovedAt int64
	// Inside says MovedAt is less than the grace before the database time.
	Inside bool
}

// lifecycleFenceSQL reads the epoch row's updated_at and whether it is
// inside the grace $2.
const lifecycleFenceSQL = `SELECT updated_at, updated_at > ` + sqlgen.ServerTimeMillisSQL + ` - $2
	FROM %s WHERE schema_id = $1`

// ReadLifecycleFence answers, on q, when schemaID's lifecycle epoch last
// moved, and whether that was less than grace ago. A garbage collector reads
// it in the snapshot it reads the record and the history in, and deletes
// nothing while it is inside the grace: a lost record's recovery cannot tell
// whether the lost head's history row was lost too, and then no row names the
// objects a reader pinned to that head still scans (docs/publication-
// protocol-v2.md, "A lost record"). An id without the row answers Present
// false.
func ReadLifecycleFence(ctx context.Context, q Querier, t Tables, schemaID int16, grace time.Duration) (LifecycleFence, error) {
	var f LifecycleFence
	err := q.runner().queryRow(ctx, fmt.Sprintf(lifecycleFenceSQL, t.Epoch), schemaID, grace.Milliseconds()).Scan(&f.MovedAt, &f.Inside)
	if errors.Is(err, sql.ErrNoRows) {
		return LifecycleFence{}, nil
	}
	if err != nil {
		return LifecycleFence{}, fmt.Errorf("read the lifecycle fence of schema %d in %s: %w", schemaID, t.Epoch, err)
	}
	f.Present = true
	return f, nil
}
