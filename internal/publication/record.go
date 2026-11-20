package publication

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// State is a publication record's state.
type State string

const (
	// Unpublished: the schema has no parquet in this epoch. Every change_log
	// entry of the schema is unflushed and every live row has one, so the hot
	// tier alone answers a read.
	Unpublished State = "UNPUBLISHED"
	// Published: the record names a committed generation, which may list no
	// file at all. An empty generation is a statement that the parquet tiers
	// hold nothing, never a reason to look anywhere else.
	Published State = "PUBLISHED"
	// AwaitingInit: the V2 cutover found parquet history for the schema (a
	// flushed entry, or a live row without an entry) and no V2 generation
	// exists yet. The hot tier alone would drop rows, so readers fail closed,
	// and only a cdc-init may publish: its export replaces whatever V1 wrote.
	AwaitingInit State = "AWAITING_INIT"
	// Dropped: the schema was dropped and its rows may not be purged yet.
	// Readers fail closed, nothing publishes or marks, and only Activate, at
	// re-registration once the purge is verified, leaves the state.
	Dropped State = "DROPPED"
)

// Kind is the writer that committed a generation. The rebase rules key on it
// (Rebase).
type Kind string

const (
	KindFlush   Kind = "flush"
	KindCompact Kind = "compact"
	KindInit    Kind = "init"
)

// ErrNoRecord reports a schema without a publication record. The record is
// written when a schema is registered, or by the V2 cutover, so its absence
// is a deployment fault, never "unpublished": a reader that took it for
// unpublished would answer from the hot tier alone and drop every row the
// parquet tiers hold.
var ErrNoRecord = errors.New("publication record missing")

// ErrAwaitingInit reports a schema the V2 cutover left for a cdc-init to
// re-onboard. A reader fails closed on it.
var ErrAwaitingInit = errors.New("schema awaits its first V2 cdc-init")

// ErrDropped reports a dropped schema whose id is not registered again yet.
// Readers fail closed on it, and no transition but Activate leaves it.
var ErrDropped = errors.New("schema dropped")

// Record is one schema's publication record as a transaction read it.
type Record struct {
	SchemaID int16
	Epoch    int64
	State    State
	// Generation is the allocation counter, the current generation only when
	// State is Published.
	Generation int64
	// ParentGeneration is the current generation's predecessor in its epoch,
	// 0 for an epoch's first generation and for a record without one.
	ParentGeneration int64
	ManifestKey      string
	ManifestSHA256   string
	// Lineage is uuid.Nil exactly when State is not Published.
	Lineage   uuid.UUID
	Token     uuid.UUID
	UpdatedAt int64
	// Horizon is the collector's fence: no publication whose staging began at
	// or before it commits (RaiseHorizon).
	Horizon int64
}

// Readable answers whether a reader may answer from r: nil for Unpublished
// (the hot tier alone) and Published (the generation), ErrAwaitingInit or
// ErrDropped for a state a reader must fail closed on.
func (r Record) Readable() error {
	switch r.State {
	case Unpublished, Published:
		return nil
	case AwaitingInit:
		return fmt.Errorf("schema %d at epoch %d: %w", r.SchemaID, r.Epoch, ErrAwaitingInit)
	}
	return fmt.Errorf("schema %d at epoch %d: %w", r.SchemaID, r.Epoch, ErrDropped)
}

// insertSQL writes a record that does not exist yet, at epoch 1 and
// generation 0, in the state $3, for an id that never had a lifecycle epoch
// (the epoch table %[2]s). A record that exists is left alone, so running
// registration again never resets a schema, and so is an id whose record was
// lost: epoch 1 is one of its old epochs.
const insertSQL = `INSERT INTO %[1]s (schema_id, epoch, state, generation, parent_generation,
		manifest_key, manifest_sha256, lineage, cas_token, updated_at, gc_horizon)
	SELECT $1, 1, $3, 0, 0, '', '', NULL, $2::uuid, ` + sqlgen.ServerTimeMillisSQL + `, 0
	WHERE NOT EXISTS (SELECT 1 FROM %[2]s WHERE schema_id = $1)
	ON CONFLICT (schema_id) DO NOTHING`

// insertedSQL tells, after insertSQL wrote nothing, whether the record exists
// and the id's lifecycle epoch, 0 when it has none.
const insertedSQL = `SELECT EXISTS (SELECT 1 FROM %[1]s WHERE schema_id = $1),
	COALESCE((SELECT epoch FROM %[2]s WHERE schema_id = $1), 0)`

// Initialize writes a newly registered schema's record, unpublished, on tx,
// the registration transaction that inserts the schema's registry row, and
// answers whether it wrote one. A record that exists is left as it is: a
// DROPPED one stays dropped until Activate. Registration and the V2 cutover
// are the only places a record is created: an existing schema whose record
// is missing stays missing (ErrNoRecord). An id whose record is missing while
// its lifecycle epoch survives lost its record: Initialize refuses it with
// ErrRecordLost, and init-db recovers it (InitializeAfterLoss).
func Initialize(ctx context.Context, tx Tx, t Tables, schemaID int16) (bool, error) {
	return insertRecord(ctx, tx, t, schemaID, Unpublished)
}

// InitializeAwaitingInit writes the record of a schema the V2 cutover found
// with parquet history, AWAITING_INIT, on tx, the transaction that read the
// history under the schema lock. Readers fail closed on it until a cdc-init
// publishes the schema's first V2 generation.
func InitializeAwaitingInit(ctx context.Context, tx Tx, t Tables, schemaID int16) (bool, error) {
	return insertRecord(ctx, tx, t, schemaID, AwaitingInit)
}

// ErrRecordLost reports a schema id without a record whose lifecycle epoch
// survives: it had a record, and a new one at epoch 1 would carry an epoch a
// running process may still pin. init-db recovers it above that epoch
// (InitializeAfterLoss).
var ErrRecordLost = errors.New("publication record lost")

func insertRecord(ctx context.Context, tx Tx, t Tables, schemaID int16, state State) (bool, error) {
	r := tx.runner()
	n, err := r.exec(ctx, fmt.Sprintf(insertSQL, t.Record, t.Epoch), schemaID, uuid.New(), string(state))
	if err != nil {
		return false, fmt.Errorf("initialize %s publication record of schema %d in %s: %w", state, schemaID, t.Record, err)
	}
	if n == 1 {
		if err := raiseEpoch(ctx, r, t, schemaID, 1); err != nil {
			return false, fmt.Errorf("initialize %s publication record of schema %d: %w", state, schemaID, err)
		}
		return true, nil
	}
	var exists bool
	var epoch int64
	if err := r.queryRow(ctx, fmt.Sprintf(insertedSQL, t.Record, t.Epoch), schemaID).Scan(&exists, &epoch); err != nil {
		return false, fmt.Errorf("initialize %s publication record of schema %d: read what holds its place: %w", state, schemaID, err)
	}
	if !exists && epoch > 0 {
		return false, fmt.Errorf("initialize %s publication record of schema %d: its record is missing while its lifecycle epoch %d "+
			"in %s survives: %w", state, schemaID, epoch, t.Epoch, ErrRecordLost)
	}
	return false, nil
}

const recordColumns = `schema_id, epoch, state, generation, parent_generation,
		manifest_key, manifest_sha256, lineage, cas_token, updated_at, gc_horizon`

// readSQL reads one record and the transaction's database time. Read in a
// reader's REPEATABLE READ snapshot, the time is the snapshot's start, the
// instant the reader's scan deadline is measured from (Snapshot).
const readSQL = `SELECT ` + recordColumns + `, ` + sqlgen.ServerTimeMillisSQL + `
	FROM %s WHERE schema_id = $1`

// Snapshot is a record together with the database time of the transaction
// that read it.
type Snapshot struct {
	Record
	ReadAt int64
}

// Read reads schemaID's record on q. A reader calls it in the snapshot it
// reads the dirty set in, before any other statement of that snapshot, and
// then asks Readable.
func Read(ctx context.Context, q Querier, t Tables, schemaID int16) (Snapshot, error) {
	var s Snapshot
	var state string
	var lineage uuid.NullUUID
	err := q.runner().queryRow(ctx, fmt.Sprintf(readSQL, t.Record), schemaID).Scan(
		&s.SchemaID, &s.Epoch, &state, &s.Generation, &s.ParentGeneration,
		&s.ManifestKey, &s.ManifestSHA256, &lineage, &s.Token, &s.UpdatedAt, &s.Horizon, &s.ReadAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Snapshot{}, fmt.Errorf("read publication record of schema %d in %s: %w", schemaID, t.Record, ErrNoRecord)
	}
	if err != nil {
		return Snapshot{}, fmt.Errorf("read publication record of schema %d in %s: %w", schemaID, t.Record, err)
	}
	s.State = State(state)
	s.Lineage = lineage.UUID
	if err := s.Record.validate(); err != nil {
		return Snapshot{}, fmt.Errorf("read publication record of schema %d in %s: %w", schemaID, t.Record, &InvalidRecordError{Record: s.Record, Err: err})
	}
	return s, nil
}

// ErrRecordInvalid marks a record row that was read and that no transition
// of this package writes. The read succeeded: what it found is the fault, so
// a reader fails closed on it as it does on ErrNoRecord, and never takes it
// for a failed read.
var ErrRecordInvalid = errors.New("publication record invalid")

// InvalidRecordError is the record row Read refused, as read, and why. It
// matches ErrRecordInvalid.
type InvalidRecordError struct {
	Record Record
	Err    error
}

func (e *InvalidRecordError) Error() string {
	return fmt.Sprintf("%v: %v", ErrRecordInvalid, e.Err)
}

func (e *InvalidRecordError) Unwrap() []error { return []error{ErrRecordInvalid, e.Err} }

// validate refuses a record no transition of this package writes: a reader
// must not guess what such a row means. Its identity first: every record is
// created at epoch 1 (or, recovered, above its id's lifecycle epoch) with a
// fresh token, and every transition moves the epoch up, the generation
// counter up or not at all, and writes a fresh token, so
// a non-positive epoch, a negative counter or horizon, or a nil token is a
// row nobody wrote through the protocol, whatever its state.
func (r Record) validate() error {
	if err := r.validateIdentity(); err != nil {
		return err
	}
	return r.validateShape()
}

// validateIdentity refuses identity values no transition emits.
func (r Record) validateIdentity() error {
	switch {
	case r.Epoch < 1:
		return fmt.Errorf("record of schema %d is at epoch %d; every record starts at epoch 1 and epochs only grow", r.SchemaID, r.Epoch)
	case r.Generation < 0 || r.ParentGeneration < 0:
		return fmt.Errorf("record of schema %d names generation %d with parent %d; generation numbers start at 1 and the counter at 0", r.SchemaID, r.Generation, r.ParentGeneration)
	case r.Token == uuid.Nil:
		return fmt.Errorf("record of schema %d at epoch %d has the nil token; every transition writes a fresh one", r.SchemaID, r.Epoch)
	case r.Horizon < 0:
		return fmt.Errorf("record of schema %d at epoch %d has the negative horizon %d", r.SchemaID, r.Epoch, r.Horizon)
	}
	return nil
}

// validateShape refuses the fields a record's state does not allow.
func (r Record) validateShape() error {
	switch r.State {
	case Unpublished, AwaitingInit, Dropped:
		if r.ManifestKey != "" || r.ManifestSHA256 != "" || r.Lineage != uuid.Nil || r.ParentGeneration != 0 {
			return fmt.Errorf("record of schema %d is %s but names manifest %q, lineage %s, parent %d; only a published record names them", r.SchemaID, r.State, r.ManifestKey, r.Lineage, r.ParentGeneration)
		}
	case Published:
		if r.Generation <= 0 || r.ManifestKey == "" || r.ManifestSHA256 == "" || r.Lineage == uuid.Nil {
			return fmt.Errorf("record of schema %d is %s at generation %d with manifest %q, lineage %s; a published record names a positive generation, its manifest, digest and lineage", r.SchemaID, r.State, r.Generation, r.ManifestKey, r.Lineage)
		}
		if r.ParentGeneration >= r.Generation {
			return fmt.Errorf("record of schema %d is %s at generation %d with parent %d; a generation's parent precedes it", r.SchemaID, r.State, r.Generation, r.ParentGeneration)
		}
	default:
		return fmt.Errorf("record of schema %d has state %q, want %s, %s, %s or %s", r.SchemaID, r.State, Unpublished, Published, AwaitingInit, Dropped)
	}
	return nil
}
