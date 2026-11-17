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
)

// Kind is the writer that committed a generation. The rebase rules key on it
// (RebaseAllowed).
type Kind string

const (
	KindFlush   Kind = "flush"
	KindCompact Kind = "compact"
	KindInit    Kind = "init"
)

// ErrNoRecord reports a schema without a publication record. The record is
// written when a schema is registered, so its absence is a deployment fault,
// never "unpublished": a reader that took it for unpublished would answer
// from the hot tier alone and drop every row the parquet tiers hold.
var ErrNoRecord = errors.New("publication record missing")

// Record is one schema's publication record as a transaction read it.
type Record struct {
	SchemaID         int16
	Epoch            int64
	State            State
	Generation       int64
	ParentGeneration int64
	ManifestKey      string
	ManifestSHA256   string
	// Lineage is uuid.Nil exactly when State is Unpublished.
	Lineage   uuid.UUID
	Token     uuid.UUID
	UpdatedAt int64
}

// Querier is what the package runs statements on: *sql.DB or *sql.Tx.
type Querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// initializeSQL writes a newly registered schema's record: epoch 1,
// unpublished, generation 0. A record that exists is left alone, so running
// registration again never resets a published schema.
const initializeSQL = `INSERT INTO %s (schema_id, epoch, state, generation, parent_generation,
		manifest_key, manifest_sha256, lineage, cas_token, updated_at)
	VALUES ($1, 1, '` + string(Unpublished) + `', 0, 0, '', '', NULL, $2, ` + sqlgen.ServerTimeMillisSQL + `)
	ON CONFLICT (schema_id) DO NOTHING`

// Initialize writes schemaID's record in the registration transaction that
// inserts the schema's registry row, and answers whether it wrote one.
// Registration is the only place a record is created: an existing schema
// whose record is missing stays missing (ErrNoRecord) until an operator
// re-onboards it.
func Initialize(ctx context.Context, q Querier, t Tables, schemaID int16) (bool, error) {
	res, err := q.ExecContext(ctx, fmt.Sprintf(initializeSQL, t.Record), schemaID, uuid.New())
	if err != nil {
		return false, fmt.Errorf("initialize publication record of schema %d in %s: %w", schemaID, t.Record, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("count initialized publication records of schema %d: %w", schemaID, err)
	}
	return n == 1, nil
}

const recordColumns = `schema_id, epoch, state, generation, parent_generation,
		manifest_key, manifest_sha256, lineage, cas_token, updated_at`

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
// reads the dirty set in, before any other statement of that snapshot.
func Read(ctx context.Context, q Querier, t Tables, schemaID int16) (Snapshot, error) {
	var s Snapshot
	var state string
	var lineage uuid.NullUUID
	err := q.QueryRowContext(ctx, fmt.Sprintf(readSQL, t.Record), schemaID).Scan(
		&s.SchemaID, &s.Epoch, &state, &s.Generation, &s.ParentGeneration,
		&s.ManifestKey, &s.ManifestSHA256, &lineage, &s.Token, &s.UpdatedAt, &s.ReadAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Snapshot{}, fmt.Errorf("read publication record of schema %d in %s: %w", schemaID, t.Record, ErrNoRecord)
	}
	if err != nil {
		return Snapshot{}, fmt.Errorf("read publication record of schema %d in %s: %w", schemaID, t.Record, err)
	}
	s.State = State(state)
	s.Lineage = lineage.UUID
	if err := s.Record.validate(); err != nil {
		return Snapshot{}, fmt.Errorf("read publication record of schema %d in %s: %w", schemaID, t.Record, err)
	}
	return s, nil
}

// validate refuses a record no transition of this package writes: a reader
// must not guess what such a row means.
func (r Record) validate() error {
	switch r.State {
	case Unpublished:
		if r.ManifestKey != "" || r.ManifestSHA256 != "" || r.Lineage != uuid.Nil {
			return fmt.Errorf("record of schema %d is %s but names manifest %q, lineage %s; an unpublished record names neither", r.SchemaID, r.State, r.ManifestKey, r.Lineage)
		}
	case Published:
		if r.Generation <= 0 || r.ManifestKey == "" || r.ManifestSHA256 == "" || r.Lineage == uuid.Nil {
			return fmt.Errorf("record of schema %d is %s at generation %d with manifest %q, lineage %s; a published record names a positive generation, its manifest, digest and lineage", r.SchemaID, r.State, r.Generation, r.ManifestKey, r.Lineage)
		}
	default:
		return fmt.Errorf("record of schema %d has state %q, want %s or %s", r.SchemaID, r.State, Unpublished, Published)
	}
	return nil
}
