package publication

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// resetSQL drops the schema: the next epoch, DROPPED, no manifest, no
// lineage. generation keeps its value, so a later publication never reuses a
// number an earlier epoch committed.
const resetSQL = `UPDATE %s SET epoch = epoch + 1, state = '` + string(Dropped) + `',
		parent_generation = 0, manifest_key = '', manifest_sha256 = '',
		lineage = NULL, cas_token = $4, updated_at = ` + sqlgen.ServerTimeMillisSQL + `
	WHERE schema_id = $1 AND epoch = $2 AND cas_token = $3 AND state <> '` + string(Dropped) + `'
	RETURNING epoch, updated_at`

// Reset moves cur's schema to its next epoch, DROPPED, on tx. It is the drop
// transition: the caller deletes the schema's registry row on the same tx.
// While the record is DROPPED readers fail closed and nothing publishes or
// marks, so the schema's entity_main, eav_data and change_log rows can be
// purged without a reader or a writer seeing them; Activate then refuses to
// re-register the id while any remain (docs/publication-protocol-v2.md,
// "Lifecycle of the row"). Every writer staged under cur's epoch fails its
// compare-and-set, whatever generation it expected.
func Reset(ctx context.Context, tx Tx, t Tables, cur Record) (Record, error) {
	if cur.State == Dropped {
		return Record{}, fmt.Errorf("reset schema %d at epoch %d: %w", cur.SchemaID, cur.Epoch, ErrDropped)
	}
	r := tx.runner()
	out := cur
	out.Token = uuid.New()
	err := r.queryRow(ctx, fmt.Sprintf(resetSQL, t.Record), cur.SchemaID, cur.Epoch, cur.Token, out.Token).
		Scan(&out.Epoch, &out.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Record{}, fmt.Errorf("reset schema %d from epoch %d token %s: %w", cur.SchemaID, cur.Epoch, cur.Token, ErrRecordMoved)
	}
	if err != nil {
		return Record{}, fmt.Errorf("reset schema %d from epoch %d: %w", cur.SchemaID, cur.Epoch, err)
	}
	if cur.State == Published {
		if _, err := r.exec(ctx, fmt.Sprintf(supersedeSQL, t.Generation), cur.SchemaID, cur.Generation); err != nil {
			return Record{}, fmt.Errorf("supersede schema %d generation %d on reset: %w", cur.SchemaID, cur.Generation, err)
		}
	}
	out.State, out.ParentGeneration = Dropped, 0
	out.ManifestKey, out.ManifestSHA256, out.Lineage = "", "", uuid.Nil
	return out, nil
}

// ErrResidue reports rows of a dropped schema that are still there when its
// id is registered again: an UNPUBLISHED record would promise that each of
// them has an unflushed entry, and a writer of the new schema would export
// them.
var ErrResidue = errors.New("dropped schema's rows not purged")

const residueSQL = `SELECT 1 FROM %s WHERE schema_id = $1 LIMIT 1`

// activateSQL starts the re-registered schema's epoch, UNPUBLISHED, from a
// DROPPED record.
const activateSQL = `UPDATE %s SET epoch = epoch + 1, state = '` + string(Unpublished) + `',
		cas_token = $4, updated_at = ` + sqlgen.ServerTimeMillisSQL + `
	WHERE schema_id = $1 AND epoch = $2 AND cas_token = $3 AND state = '` + string(Dropped) + `'
	RETURNING epoch, updated_at`

// Activate re-registers a dropped schema id on tx, the registration
// transaction that inserts its registry row again: it checks in tx that none
// of the schema's rows remain in residue (its entity_main, eav_data and
// change_log tables), then moves the record from DROPPED to the next epoch,
// UNPUBLISHED, under a new token. Nothing that read the record before it
// carries the new epoch, so no reader, writer or handoff of the old schema
// acts on the new one without reading the record again.
func Activate(ctx context.Context, tx Tx, t Tables, cur Record, residue []string) (Record, error) {
	if cur.State != Dropped {
		return Record{}, fmt.Errorf("activate schema %d: its record is %s, and only a %s one is activated", cur.SchemaID, cur.State, Dropped)
	}
	r := tx.runner()
	for _, table := range residue {
		var one int
		err := r.queryRow(ctx, fmt.Sprintf(residueSQL, sqlutil.SanitizeIdentifier(table)), cur.SchemaID).Scan(&one)
		if err == nil {
			return Record{}, fmt.Errorf("activate schema %d: rows of epoch %d remain in %s: %w", cur.SchemaID, cur.Epoch, table, ErrResidue)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return Record{}, fmt.Errorf("activate schema %d: check %s for its rows: %w", cur.SchemaID, table, err)
		}
	}
	out := cur
	out.Token = uuid.New()
	err := r.queryRow(ctx, fmt.Sprintf(activateSQL, t.Record), cur.SchemaID, cur.Epoch, cur.Token, out.Token).
		Scan(&out.Epoch, &out.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Record{}, fmt.Errorf("activate schema %d from epoch %d token %s: %w", cur.SchemaID, cur.Epoch, cur.Token, ErrRecordMoved)
	}
	if err != nil {
		return Record{}, fmt.Errorf("activate schema %d from epoch %d: %w", cur.SchemaID, cur.Epoch, err)
	}
	out.State = Unpublished
	return out, nil
}

// Outcome is what Resolve learned about a publication whose commit answer was
// lost.
type Outcome int

const (
	// NotCommitted: the record still carries the attempt's expectation, so no
	// generation committed from it. The writer may send the same publication
	// again, with the same expectation and token: if the lost commit lands
	// first, the retry loses the compare-and-set and Resolve then answers
	// Committed.
	NotCommitted Outcome = iota
	// Committed: the generation committed with the writer's token, or the
	// record carries the fresh lineage only this attempt introduced.
	Committed
	// Lost: another writer's generation took the number. The writer's own
	// transaction cannot commit any more, because both moved the record from
	// the same generation; the writer reads the record and rebases.
	Lost
	// Unknown: the record moved past the attempt's expectation, and the
	// history no longer holds the number. The attempt can never commit now,
	// and whether it did is not recoverable. The writer abandons it and starts
	// over from the head, which is safe for every kind: a flush selects the
	// still unflushed entries, a compaction reads the head's files, and a
	// cdc-init's lineage is no longer the record's, so its handoff would stop.
	Unknown
)

// resolveSQL reads the record and the history row of the attempt's number in
// one statement, so both come from one snapshot.
const resolveSQL = `SELECT r.epoch, r.generation, r.cas_token, r.lineage, g.cas_token
	FROM %s r LEFT JOIN %s g ON g.schema_id = r.schema_id AND g.generation = $2
	WHERE r.schema_id = $1`

// Resolve answers what became of next, published on cur, after its COMMIT
// failed without an answer. It reads in a new transaction, at any time after
// the failure: a forgotten history row is told apart from an attempt that
// never committed by the record read in the same snapshot.
func Resolve(ctx context.Context, q Querier, t Tables, cur Record, next Next) (Outcome, error) {
	var epoch, generation int64
	var token uuid.UUID
	var lineage, got uuid.NullUUID
	err := q.runner().queryRow(ctx, fmt.Sprintf(resolveSQL, t.Record, t.Generation), cur.SchemaID, cur.Generation+1).
		Scan(&epoch, &generation, &token, &lineage, &got)
	if errors.Is(err, sql.ErrNoRows) {
		return NotCommitted, fmt.Errorf("resolve schema %d generation %d: %w", cur.SchemaID, cur.Generation+1, ErrNoRecord)
	}
	if err != nil {
		return NotCommitted, fmt.Errorf("resolve schema %d generation %d: %w", cur.SchemaID, cur.Generation+1, err)
	}
	switch {
	case got.Valid && got.UUID == next.Token:
		return Committed, nil
	case got.Valid:
		return Lost, nil
	case epoch == cur.Epoch && generation == cur.Generation && token == cur.Token:
		return NotCommitted, nil
	case epoch == cur.Epoch && next.Lineage != cur.Lineage && lineage.Valid && lineage.UUID == next.Lineage:
		return Committed, nil
	}
	return Unknown, nil
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
// marks; the caller marks on tx and commits or rolls it back.
func HoldLineage(ctx context.Context, tx Tx, t Tables, schemaID int16, epoch int64, lineage uuid.UUID) (int64, error) {
	var generation int64
	err := tx.runner().queryRow(ctx, fmt.Sprintf(holdSQL, t.Record), schemaID, epoch, lineage).Scan(&generation)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("hold schema %d lineage %s at epoch %d: %w", schemaID, lineage, epoch, ErrLineageReplaced)
	}
	if err != nil {
		return 0, fmt.Errorf("hold schema %d lineage %s at epoch %d: %w", schemaID, lineage, epoch, err)
	}
	return generation, nil
}

// fenceSQL writes the record row, changing nothing, while it still carries
// the expectation of a publication whose COMMIT went without an answer.
const fenceSQL = `UPDATE %s SET cas_token = cas_token
	WHERE schema_id = $1 AND epoch = $2 AND generation = $3 AND cas_token = $4`

// Fence settles a publication from cur whose COMMIT went without an answer
// and which Resolve found NotCommitted, when that COMMIT may still be on its
// way. It writes the record on tx while the record still carries cur, and
// answers whether it did; the caller commits tx. A COMMIT the fence's commit
// precedes can never land: the publication's compare-and-set wrote the same
// row before its COMMIT was sent, so on PostgreSQL the fence's write waits
// on the row lock that transaction holds until it ends (and finds the record
// moved if it committed), and on Aurora DSQL whichever of the two commits
// second fails with 40001. So once a fence that answered true commits,
// NotCommitted is final. False says the record moved: Resolve then answers
// Committed, Lost or Unknown, and none of them changes again.
func Fence(ctx context.Context, tx Tx, t Tables, cur Record) (bool, error) {
	n, err := tx.runner().exec(ctx, fmt.Sprintf(fenceSQL, t.Record), cur.SchemaID, cur.Epoch, cur.Generation, cur.Token)
	if err != nil {
		return false, fmt.Errorf("fence schema %d at epoch %d generation %d: %w", cur.SchemaID, cur.Epoch, cur.Generation, err)
	}
	return n == 1, nil
}
