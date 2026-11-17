package publication

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// resetSQL starts the schema's next epoch: unpublished, no manifest, no
// lineage. generation keeps its value, so a later publication never reuses a
// number an earlier epoch committed.
const resetSQL = `UPDATE %s SET epoch = epoch + 1, state = '` + string(Unpublished) + `',
		parent_generation = generation, manifest_key = '', manifest_sha256 = '',
		lineage = NULL, cas_token = $4, updated_at = ` + sqlgen.ServerTimeMillisSQL + `
	WHERE schema_id = $1 AND epoch = $2 AND cas_token = $3
	RETURNING epoch, updated_at`

// Reset moves cur's schema to its next epoch, unpublished, on tx. It is the
// drop transition: the caller deletes the schema's registry row on the same
// tx, and the schema's entity_main, eav_data and change_log rows are purged
// before the id is registered again (docs/publication-protocol-v2.md,
// "Drop and re-creation"), because an unpublished record promises that every
// live row has an unflushed entry. Every writer staged under cur's epoch then
// fails its compare-and-set, whatever generation it expected.
func Reset(ctx context.Context, tx Querier, t Tables, cur Record) (Record, error) {
	out := cur
	out.Token = uuid.New()
	err := tx.QueryRowContext(ctx, fmt.Sprintf(resetSQL, t.Record), cur.SchemaID, cur.Epoch, cur.Token, out.Token).
		Scan(&out.Epoch, &out.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Record{}, fmt.Errorf("reset schema %d from epoch %d token %s: %w", cur.SchemaID, cur.Epoch, cur.Token, ErrRecordMoved)
	}
	if err != nil {
		return Record{}, fmt.Errorf("reset schema %d from epoch %d: %w", cur.SchemaID, cur.Epoch, err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(supersedeSQL, t.Generation), cur.SchemaID, cur.Generation); err != nil {
		return Record{}, fmt.Errorf("supersede schema %d generation %d on reset: %w", cur.SchemaID, cur.Generation, err)
	}
	out.State, out.ParentGeneration = Unpublished, cur.Generation
	out.ManifestKey, out.ManifestSHA256, out.Lineage = "", "", uuid.Nil
	return out, nil
}

// Outcome is what Resolve learned about a publication whose commit answer was
// lost.
type Outcome int

const (
	// NotCommitted: no generation with that number committed. The writer may
	// send the same publication again, with the same expectation and token:
	// if the lost commit lands first, the retry loses the compare-and-set and
	// Resolve then answers Committed.
	NotCommitted Outcome = iota
	// Committed: the generation committed with the writer's token.
	Committed
	// Lost: another writer's generation took the number. The writer's own
	// transaction cannot commit any more, because both moved the record from
	// the same generation; the writer reads the record and rebases.
	Lost
)

const resolveSQL = `SELECT cas_token FROM %s WHERE schema_id = $1 AND generation = $2`

// Resolve answers what became of a publication of generation on schemaID
// with token, after its COMMIT failed without an answer. It reads in a new
// transaction. The generation's row is written in the transaction that
// commits the generation, and garbage collection removes a row only long
// after it was superseded, so Resolve is meant to run right after the failure.
func Resolve(ctx context.Context, q Querier, t Tables, schemaID int16, generation int64, token uuid.UUID) (Outcome, error) {
	var got uuid.UUID
	err := q.QueryRowContext(ctx, fmt.Sprintf(resolveSQL, t.Generation), schemaID, generation).Scan(&got)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return NotCommitted, nil
	case err != nil:
		return NotCommitted, fmt.Errorf("resolve schema %d generation %d: %w", schemaID, generation, err)
	case got == token:
		return Committed, nil
	}
	return Lost, nil
}

// ErrLineageReplaced reports that the lineage a cdc-init handoff marks for is
// no longer the record's: a later cdc-init, or a reset, replaced it. The
// handoff stops, and the rows it did not mark stay dirty for the next flush.
var ErrLineageReplaced = errors.New("publication lineage replaced")

// holdSQL locks the record while it still carries epoch and lineage. FOR
// UPDATE, not FOR SHARE: on Aurora DSQL only FOR UPDATE puts a read row into
// the transaction's conflict check, so a transition that commits meanwhile
// fails one of the two transactions.
const holdSQL = `SELECT generation FROM %s
	WHERE schema_id = $1 AND epoch = $2 AND lineage = $3 AND state = '` + string(Published) + `'
	FOR UPDATE`

// HoldLineage is the first statement of a cdc-init handoff transaction, which
// marks a chunk of the entries the export holds after the init's generation
// committed. It holds the record at epoch and lineage until tx ends, so no
// transition that would replace the lineage commits between the check and the
// marks.
func HoldLineage(ctx context.Context, tx Querier, t Tables, schemaID int16, epoch int64, lineage uuid.UUID) (int64, error) {
	var generation int64
	err := tx.QueryRowContext(ctx, fmt.Sprintf(holdSQL, t.Record), schemaID, epoch, lineage).Scan(&generation)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("hold schema %d lineage %s at epoch %d: %w", schemaID, lineage, epoch, ErrLineageReplaced)
	}
	if err != nil {
		return 0, fmt.Errorf("hold schema %d lineage %s at epoch %d: %w", schemaID, lineage, epoch, err)
	}
	return generation, nil
}
