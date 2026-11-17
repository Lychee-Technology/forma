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
// Nothing in Forma's writers or readers uses this package yet: it fixes the
// statements and their semantics, and the #887 children move cdc-flush,
// cdc-init, the compactor, the federated reader and manifest-reconcile onto
// it.
//
// Both tables keep plain rows: no JSONB, arrays, sequences, enums, CHECK
// constraints or foreign keys, none of which this package relies on Aurora
// DSQL to support. Times are epoch milliseconds on the database clock.
package publication

import (
	"fmt"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlutil"
	"github.com/lychee-technology/forma/internal/textcollation"
)

// Table suffixes name both tables after the entity_main table, like the
// schema-lease table: cdc-flush, cdc-init, the compactor and the reader are
// all given entity_main, so naming the tables after it lets every holder
// find the same rows.
const (
	RecordTableSuffix     = "_publication"
	GenerationTableSuffix = "_publication_generation"
)

// Tables names the record and generation tables of one deployment, each
// sanitized for a statement.
type Tables struct {
	Record, Generation string
}

// TablesFor names the publication tables of entityMain. A schema-qualified
// name keeps its qualifier, and each name is bounded to PostgreSQL's
// identifier limit.
func TablesFor(entityMain string) Tables {
	return Tables{
		Record:     sqlutil.SanitizeIdentifier(sqlutil.SuffixedIdentifier(entityMain, RecordTableSuffix)),
		Generation: sqlutil.SanitizeIdentifier(sqlutil.SuffixedIdentifier(entityMain, GenerationTableSuffix)),
	}
}

// recordDDL is the per-schema publication record: exactly one row per
// schema_id that has ever been registered. It is never deleted, so a schema
// id is never reused with an epoch, generation or token it had before.
//
//   - epoch is the schema's lifecycle epoch. Drop (Reset) increases it, and
//     every compare-and-set names it.
//   - state is UNPUBLISHED or PUBLISHED. generation is 0 only before the first
//     publication, and only ever increases, across epochs too.
//   - manifest_key and manifest_sha256 name the generation's immutable
//     manifest object; both are empty exactly when state is UNPUBLISHED.
//   - lineage identifies the content line a cdc-init export started; flush
//     and compaction keep it. NULL exactly when state is UNPUBLISHED.
//   - cas_token changes on every transition. It is the token of the
//     transaction that wrote the row, and the generation's own identity.
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
		PRIMARY KEY (schema_id)
	)`

// generationDDL is one row per committed generation, written in the
// transaction that commits it. superseded_at is 0 while the generation is
// the record's current one, and the database time of the transaction that
// replaced it afterwards; garbage collection reads it (retention.go).
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
		PRIMARY KEY (schema_id, generation)
	)`

// Statements answers the publication tables' DDL for entityMain on engine,
// one statement each with no secondary index, so each is one DDL transaction
// on Aurora DSQL too. Text columns are COLLATE "C" on PostgreSQL and plain
// TEXT on DSQL, whose collation is C and which refuses the clause (#816).
func Statements(engine forma.DatabaseEngine, entityMain string) []string {
	t := TablesFor(entityMain)
	return []string{
		textcollation.ForEngine(engine, fmt.Sprintf(recordDDL, t.Record)),
		textcollation.ForEngine(engine, fmt.Sprintf(generationDDL, t.Generation)),
	}
}
