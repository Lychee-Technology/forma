package publication

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// History is what the generation table still holds of a schema's committed
// generations: how many rows, and the highest epoch and generation number
// they carry. A record names a generation number only after its history row
// committed in the same transaction, so a schema whose record is missing
// while History counts rows had a V2 record once and lost it: a record
// written for it at epoch 1 and generation 0 would reuse an epoch and a
// generation number it already committed.
type History struct {
	Generations   int64
	MaxEpoch      int64
	MaxGeneration int64
}

// historySQL reads the schema's surviving history.
const historySQL = `SELECT COUNT(*), COALESCE(MAX(epoch), 0), COALESCE(MAX(generation), 0)
	FROM %s WHERE schema_id = $1`

// ReadHistory answers what the generation table holds of schemaID's
// committed generations, on q.
func ReadHistory(ctx context.Context, q Querier, t Tables, schemaID int16) (History, error) {
	var h History
	err := q.runner().queryRow(ctx, fmt.Sprintf(historySQL, t.Generation), schemaID).
		Scan(&h.Generations, &h.MaxEpoch, &h.MaxGeneration)
	if err != nil {
		return History{}, fmt.Errorf("read the publication history of schema %d in %s: %w", schemaID, t.Generation, err)
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
// missing while its history h survives (h.Generations > 0): at the epoch
// after every epoch h names and with h's highest generation number as its
// allocation counter, so no epoch the surviving history names and no
// generation number it committed is used again. state is AwaitingInit for a
// registered schema, whose parquet tiers the next cdc-init replaces, or
// Dropped for an id whose old registration is gone, which Activate then
// re-registers after its residue check. It answers the record written and
// whether it wrote one; a record that exists is left as it is.
//
// The epoch is a floor, not the lost record's own: a drop and re-activation
// after the last publication moved the lost record's epoch without leaving
// history. Every writer that staged under that epoch fails its
// compare-and-set against the recovered record's other token in any case.
func InitializeAfterLoss(ctx context.Context, tx Tx, t Tables, schemaID int16, h History, state State) (Record, bool, error) {
	if h.Generations <= 0 {
		return Record{}, false, fmt.Errorf("recover the record of schema %d: no surviving history; a schema without history is initialized at epoch 1", schemaID)
	}
	if state != AwaitingInit && state != Dropped {
		return Record{}, false, fmt.Errorf("recover the record of schema %d as %s: a lost record is recovered %s or %s only", schemaID, state, AwaitingInit, Dropped)
	}
	rec := Record{SchemaID: schemaID, Epoch: h.MaxEpoch + 1, State: state, Generation: h.MaxGeneration, Token: uuid.New()}
	n, err := tx.runner().exec(ctx, fmt.Sprintf(recoverSQL, t.Record), schemaID, rec.Epoch, rec.Generation, string(state), rec.Token)
	if err != nil {
		return Record{}, false, fmt.Errorf("recover the %s record of schema %d at epoch %d in %s: %w", state, schemaID, rec.Epoch, t.Record, err)
	}
	return rec, n == 1, nil
}

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
