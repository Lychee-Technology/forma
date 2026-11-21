package publication

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// History is what survives of a schema id's lifecycle apart from its record:
// the id's lifecycle epoch and forgotten generation floor (the epoch table,
// epochDDL), and how many rows the generation table holds of its committed
// generations, with the highest epoch and generation number they carry. An id
// that has a lifecycle epoch, forgotten floor, or history rows but no record
// had a V2 record once and lost it: a record written for it at epoch 1 and
// generation 0 would carry an epoch a running process may still pin, and reuse
// generation numbers it committed.
type History struct {
	// Epoch is the id's lifecycle epoch: the highest epoch any record of it
	// carried, 0 when it has none.
	Epoch int64
	// ForgottenGeneration is the highest generation number legitimately
	// forgotten by garbage collection (stored on the epoch table), 0 when
	// none have been forgotten.
	ForgottenGeneration int64
	Generations         int64
	MaxEpoch            int64
	MaxGeneration       int64
}

// Lost answers whether h says the id had a record: its lifecycle epoch,
// forgotten generation floor, or generation history survives.
func (h History) Lost() bool {
	return h.Epoch > 0 || h.ForgottenGeneration > 0 || h.Generations > 0
}

// historySQL reads the schema's surviving history, lifecycle epoch, and
// forgotten generation floor in one snapshot.
const historySQL = `SELECT COALESCE((SELECT epoch FROM %[2]s WHERE schema_id = $1), 0),
	COALESCE((SELECT forgotten_generation FROM %[2]s WHERE schema_id = $1), 0),
	COUNT(*), COALESCE(MAX(epoch), 0), COALESCE(MAX(generation), 0)
	FROM %[1]s WHERE schema_id = $1`

// ReadHistory answers what the generation and epoch tables hold of
// schemaID's lifecycle, on q.
func ReadHistory(ctx context.Context, q Querier, t Tables, schemaID int16) (History, error) {
	var h History
	err := q.runner().queryRow(ctx, fmt.Sprintf(historySQL, t.Generation, t.Epoch), schemaID).
		Scan(&h.Epoch, &h.ForgottenGeneration, &h.Generations, &h.MaxEpoch, &h.MaxGeneration)
	if err != nil {
		return History{}, fmt.Errorf("read the publication history of schema %d in %s and %s: %w", schemaID, t.Generation, t.Epoch, err)
	}
	return h, nil
}

// recoverSQL writes a lost record at the epoch and allocation counter $2 and
// $3, in the state $4, without a current generation. A record that exists is
// left alone.
const recoverSQL = `INSERT INTO %s (schema_id, epoch, state, generation, parent_generation,
		manifest_key, manifest_sha256, lineage, cas_token, updated_at, gc_horizon)
	VALUES ($1, $2, $4, $3, 0, '', '', NULL, $5, ` + sqlgen.ServerTimeMillisSQL + `, 0)
	ON CONFLICT (schema_id) DO NOTHING`

// InitializeAfterLoss writes, on tx, the record of a schema whose record is
// missing while its lifecycle epoch survives (h.Epoch > 0): at the epoch
// after the id's lifecycle epoch, and after every epoch its generation
// history names, with max(h.MaxGeneration, h.ForgottenGeneration) as its
// allocation counter. Every epoch a record of the id carried is at or below
// its lifecycle epoch, since every transition that moved a record's epoch
// raised it in the same transaction, so no process holds a pin on the
// recovered epoch: a reader or writer pinned to an epoch of the lost record,
// or of any registration before it, fails closed on the recovered record,
// and on every later one, since epochs only grow. No generation number the
// surviving or forgotten history committed is used again either.
//
// state is AwaitingInit for a registered schema with generation history,
// whose parquet tiers the next cdc-init replaces; Unpublished for a
// registered schema without any, whose rows the caller found to satisfy the
// UNPUBLISHED invariant; or Dropped for an id whose old registration is gone,
// which Activate then re-registers after its residue check. It answers the
// record written and whether it wrote one; a record that exists is left as it
// is.
//
// It refuses, with ErrLifecycleLost, an id whose lifecycle epoch is lost as
// well (h.Epoch == 0): the history's highest epoch is only a floor, since a
// drop and a re-registration move the epoch without a generation row, and a
// record recovered above that floor could carry an epoch a running process
// still pins. The operator establishes the id's lifecycle epoch first.
func InitializeAfterLoss(ctx context.Context, tx Tx, t Tables, schemaID int16, h History, state State) (Record, bool, error) {
	if h.Epoch <= 0 {
		return Record{}, false, fmt.Errorf("recover the record of schema %d: its lifecycle epoch is missing from %s too, "+
			"and its %d surviving generation(s) through epoch %d do not bound the epochs a drop or re-registration took since: %w",
			schemaID, t.Epoch, h.Generations, h.MaxEpoch, ErrLifecycleLost)
	}
	if state != AwaitingInit && state != Unpublished && state != Dropped {
		return Record{}, false, fmt.Errorf("recover the record of schema %d as %s: a lost record is recovered %s, %s or %s only",
			schemaID, state, AwaitingInit, Unpublished, Dropped)
	}
	rec := Record{
		SchemaID:   schemaID,
		Epoch:      max(h.Epoch, h.MaxEpoch) + 1,
		State:      state,
		Generation: max(h.MaxGeneration, h.ForgottenGeneration),
		Token:      uuid.New(),
	}
	r := tx.runner()
	n, err := r.exec(ctx, fmt.Sprintf(recoverSQL, t.Record), schemaID, rec.Epoch, rec.Generation, string(state), rec.Token)
	if err != nil {
		return Record{}, false, fmt.Errorf("recover the %s record of schema %d at epoch %d in %s: %w", state, schemaID, rec.Epoch, t.Record, err)
	}
	if n != 1 {
		return Record{}, false, nil
	}
	if err := raiseEpochAfterLoss(ctx, r, t, schemaID, rec.Epoch); err != nil {
		return Record{}, false, fmt.Errorf("recover the %s record of schema %d: %w", state, schemaID, err)
	}
	if _, err := r.exec(ctx, fmt.Sprintf(supersedeLostSQL, t.Generation), schemaID); err != nil {
		return Record{}, false, fmt.Errorf("supersede the former head of schema %d after loss: %w", schemaID, err)
	}
	return rec, true, nil
}

// supersedeLostSQL stamps every generation of the schema that nothing
// superseded: the former head the lost record named, which the recovered
// record does not name. Without it no later transition would ever supersede
// that row and GC would retain it forever. Clearing retired_at discards any
// bogus stamp on an unsuperseded row so GC retires it only after seeing the
// recovery committed.
const supersedeLostSQL = `UPDATE %s SET superseded_at = ` + sqlgen.ServerTimeMillisSQL + `, retired_at = 0
	WHERE schema_id = $1 AND superseded_at = 0`

// holdDroppedSQL locks a dropped record while it is still DROPPED at the
// epoch. FOR UPDATE, as holdSQL, so an Activate waits for the holder.
const holdDroppedSQL = `SELECT generation FROM %s
	WHERE schema_id = $1 AND epoch = $2 AND state = '` + string(Dropped) + `'
	FOR UPDATE`

// HoldDropped locks schemaID's record on tx while it is DROPPED at epoch, so
// the caller can purge a chunk of the dropped schema's rows knowing that no
// re-registration committed: Activate waits for tx, and a record that moved
// on (an activated id) answers ErrRecordMoved, after which nothing of the id
// is the dropped schema's.
func HoldDropped(ctx context.Context, tx Tx, t Tables, schemaID int16, epoch int64) error {
	var generation int64
	err := tx.runner().queryRow(ctx, fmt.Sprintf(holdDroppedSQL, t.Record), schemaID, epoch).Scan(&generation)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("hold the dropped record of schema %d at epoch %d: %w", schemaID, epoch, ErrRecordMoved)
	}
	if err != nil {
		return fmt.Errorf("hold the dropped record of schema %d at epoch %d: %w", schemaID, epoch, err)
	}
	return nil
}
