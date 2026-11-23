// Package publication is the PostgreSQL side of Protocol V2's parquet
// publication contract (#888, docs/publication-protocol-v2.md): one durable
// record per schema that names the schema's immutable manifest generation,
// and the history of the generations it has named.
//
// The record is the only pointer a reader or a writer resolves. A writer
// stages its parquet objects and its manifest object in S3 first, then moves
// the record in one PostgreSQL transaction with compare-and-set on the
// record's token, and in that same transaction marks the change_log entries
// the generation holds. A reader reads the record in the snapshot it reads
// the dirty set in, so the generation it scans and the rows it treats as
// flushed come from one snapshot.
//
// init-db creates the three tables from Statements (#895) and, in the
// registration transaction, each schema's record (Initialize, Activate, and
// for a lost record InitializeAfterLoss). cdc-flush, cdc-init and the
// compactor publish through internal/cdcv2, the federated reader reads the
// record in its pass snapshot through internal/federatedv2, and garbage
// collection retires and forgets generations through internal/reconcile.
// Only the owners
// TestOnlyTheProtocolOwnersMoveAPublicationRecord names may move a record.
//
// The tables keep plain rows: no JSONB, arrays, sequences, enums, CHECK
// constraints or foreign keys, none of which this package relies on Aurora
// DSQL to support. Times are epoch milliseconds on the database clock.
package publication

import (
	"fmt"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlutil"
	"github.com/lychee-technology/forma/internal/textcollation"
)

// Table suffixes name the tables after the entity_main table, like the
// schema-lease table: cdc-flush, cdc-init, the compactor and the reader are
// all given entity_main, so naming the tables after it lets every holder
// find the same rows.
const (
	RecordTableSuffix     = "_publication"
	GenerationTableSuffix = "_publication_generation"
	EpochTableSuffix      = "_publication_epoch"
	StorageTableSuffix    = "_publication_storage"
)

// Tables names the record, generation, epoch and storage-binding tables of one
// deployment, each sanitized for a statement.
type Tables struct {
	Record, Generation, Epoch, Storage string
}

// TablesFor names the publication tables of entityMain. A schema-qualified
// name keeps its qualifier, and each name is bounded to PostgreSQL's
// identifier limit.
func TablesFor(entityMain string) Tables {
	return Tables{
		Record:     sqlutil.SanitizeIdentifier(sqlutil.SuffixedIdentifier(entityMain, RecordTableSuffix)),
		Generation: sqlutil.SanitizeIdentifier(sqlutil.SuffixedIdentifier(entityMain, GenerationTableSuffix)),
		Epoch:      sqlutil.SanitizeIdentifier(sqlutil.SuffixedIdentifier(entityMain, EpochTableSuffix)),
		Storage:    sqlutil.SanitizeIdentifier(sqlutil.SuffixedIdentifier(entityMain, StorageTableSuffix)),
	}
}

// recordDDL is the per-schema publication record: exactly one row per
// schema_id that has ever been registered. It is never deleted, so a schema
// id is never reused with an epoch, generation or token it had before.
//
//   - epoch is the schema's lifecycle epoch. Drop (Reset) and re-activation
//     (Activate) each increase it, and every compare-and-set names it.
//   - state is UNPUBLISHED, PUBLISHED, AWAITING_INIT or DROPPED.
//   - generation is the allocation counter: the last generation number
//     committed, across epochs too, so a number is never reused. Only a
//     PUBLISHED record names it as its current generation.
//   - parent_generation is the current generation's CAS predecessor in the
//     same epoch, and 0 when the generation is its epoch's first or when the
//     record has no current generation.
//   - manifest_key and manifest_sha256 name the generation's immutable
//     manifest object; both are empty exactly when state is not PUBLISHED.
//   - lineage identifies the content line a cdc-init export started; flush
//     and compaction keep it. NULL exactly when state is not PUBLISHED.
//   - cas_token changes on every transition. It is the token of the
//     transaction that wrote the row, and the generation's own identity.
//   - gc_horizon is the garbage collector's fence (RaiseHorizon): no
//     publication whose staging began at or before it commits.
const recordDDL = `CREATE TABLE IF NOT EXISTS %s (
		schema_id         SMALLINT NOT NULL,
		epoch             BIGINT   NOT NULL,
		state             ` + textcollation.TextType + ` NOT NULL,
		generation        BIGINT   NOT NULL,
		parent_generation BIGINT   NOT NULL,
		manifest_key      ` + textcollation.TextType + ` NOT NULL,
		manifest_sha256   ` + textcollation.TextType + ` NOT NULL,
		lineage           UUID,
		cas_token         UUID     NOT NULL,
		updated_at        BIGINT   NOT NULL,
		gc_horizon        BIGINT   NOT NULL,
		PRIMARY KEY (schema_id)
	)`

// generationDDL is one row per committed generation, written in the
// transaction that commits it. superseded_at is 0 while the generation is
// the record's current one, and the start time of the transaction that
// replaced it afterwards. retired_at is 0 until a collector, in a transaction
// that began after it saw the replacement committed, stamps its own time:
// retention counts from retired_at, never from superseded_at, because the
// replacing transaction may commit long after it started (retention.go).
const generationDDL = `CREATE TABLE IF NOT EXISTS %s (
		schema_id         SMALLINT NOT NULL,
		generation        BIGINT   NOT NULL,
		epoch             BIGINT   NOT NULL,
		kind              ` + textcollation.TextType + ` NOT NULL,
		parent_generation BIGINT   NOT NULL,
		manifest_key      ` + textcollation.TextType + ` NOT NULL,
		manifest_sha256   ` + textcollation.TextType + ` NOT NULL,
		lineage           UUID     NOT NULL,
		cas_token         UUID     NOT NULL,
		committed_at      BIGINT   NOT NULL,
		superseded_at     BIGINT   NOT NULL DEFAULT 0,
		retired_at        BIGINT   NOT NULL DEFAULT 0,
		PRIMARY KEY (schema_id, generation)
	)`

// epochDDL is the durable lifecycle identity of each schema id, kept apart
// from the record so that it outlives a lost record row: epoch is the highest
// lifecycle epoch any record of the id has carried. Every transition that
// moves a record's epoch (Initialize, Reset, Activate,
// InitializeAfterLoss) raises it to the new epoch in its own
// transaction, and refuses to commit unless the new epoch is above it, so it
// only grows and no record is ever written at an epoch a record of the id
// carried before. A lost record is recovered above it (recovery.go): no
// process can hold a pin on that epoch. The row is never deleted.
const epochDDL = `CREATE TABLE IF NOT EXISTS %s (
		schema_id            SMALLINT NOT NULL,
		epoch                BIGINT   NOT NULL,
		updated_at           BIGINT   NOT NULL,
		forgotten_generation BIGINT,
		recovery_epoch       BIGINT,
		recovery_retired_at  BIGINT,
		PRIMARY KEY (schema_id)
	)`

// Upgrade statements for existing _publication_epoch tables created before
// forgotten_generation, recovery_epoch and recovery_retired_at existed.
// Nullable BIGINT columns keep each ALTER metadata-only and DSQL-safe.
const (
	epochForgottenDDL       = `ALTER TABLE %s ADD COLUMN IF NOT EXISTS forgotten_generation BIGINT`
	epochRecoveryEpochDDL   = `ALTER TABLE %s ADD COLUMN IF NOT EXISTS recovery_epoch BIGINT`
	epochRecoveryRetiredDDL = `ALTER TABLE %s ADD COLUMN IF NOT EXISTS recovery_retired_at BIGINT`
)

// Statements answers the publication tables' DDL for entityMain on engine,
// one statement each with no secondary index, so each is one DDL transaction
// on Aurora DSQL too. Text columns are COLLATE "C" on PostgreSQL and plain
// TEXT on DSQL, whose collation is C and which refuses the clause (#816).
func Statements(engine forma.DatabaseEngine, entityMain string) []string {
	t := TablesFor(entityMain)
	return []string{
		textcollation.ForEngine(engine, fmt.Sprintf(recordDDL, t.Record)),
		textcollation.ForEngine(engine, fmt.Sprintf(generationDDL, t.Generation)),
		textcollation.ForEngine(engine, fmt.Sprintf(epochDDL, t.Epoch)),
		fmt.Sprintf(epochForgottenDDL, t.Epoch),
		fmt.Sprintf(epochRecoveryEpochDDL, t.Epoch),
		fmt.Sprintf(epochRecoveryRetiredDDL, t.Epoch),
	}
}
