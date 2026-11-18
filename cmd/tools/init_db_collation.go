package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/rowguard"
	"github.com/lychee-technology/forma/internal/schemalease"
	"github.com/lychee-technology/forma/internal/schemaledger"
	"github.com/lychee-technology/forma/internal/textcollation"
	"github.com/lychee-technology/forma/internal/uniquekey"
)

// ownedTable is one table init-db ensures, as SQL spells it, the role an
// operator knows it by, and the flag whose value names it or the table it is
// derived from.
type ownedTable struct {
	role, flag, sql string
}

// ownedTableRoles names every table init-db ensures: the core tables, the
// unique-key, row-guard and schema-lease tables, the generation ledger and
// the publication tables. Each is a table Forma owns, so each falls under the
// text collation contract (#816), including those that hold no text today.
func ownedTableRoles(opts initDBOptions) []ownedTable {
	ledger := schemaledger.TablesFor(opts.schemaTable)
	pub := publication.TablesFor(opts.entityMain)
	return []ownedTable{
		{"the schema registry table", "--schema-table", quoteIdentifier(opts.schemaTable)},
		{"the entity main table", "--entity-main-table", quoteIdentifier(opts.entityMain)},
		{"the EAV table", "--eav-table", quoteIdentifier(opts.eavTable)},
		{"the change log table", "--change-log-table", quoteIdentifier(opts.changeLog)},
		{"the unique-key table", "--entity-main-table", quoteIdentifier(uniquekey.KeyTable(opts.entityMain))},
		{"the unique-key state table", "--entity-main-table", quoteIdentifier(uniquekey.StateTable(opts.entityMain))},
		{"the row-guard table", "--entity-main-table", quoteIdentifier(rowguard.Table(opts.entityMain))},
		{"the schema-lease table", "--entity-main-table", quoteIdentifier(schemalease.Table(opts.entityMain))},
		{"the generation ledger table", "--schema-table", ledger.Generation},
		{"the attribute ledger table", "--schema-table", ledger.Attribute},
		{publicationRecordRole, "--entity-main-table", pub.Record},
		{publicationGenerationRole, "--entity-main-table", pub.Generation},
	}
}

// ownedTables is ownedTableRoles' tables alone, as SQL spells them.
func ownedTables(opts initDBOptions) []string {
	roles := ownedTableRoles(opts)
	tables := make([]string, 0, len(roles))
	for _, table := range roles {
		tables = append(tables, table.sql)
	}
	return tables
}

// requireTextCollation reads the catalog of every table init-db just ensured
// and refuses a collatable column that is not under the C collation (#816).
// CREATE TABLE IF NOT EXISTS skips a table created before the contract, so
// without this a rerun would report success over columns whose indexes
// cannot seek a text range. It runs inside the table transaction and changes
// nothing: the refusal rolls the transaction back, and the migration is the
// operator's (docs/text-collation.md). On Aurora DSQL, whose collation is
// fixed at C and whose DDL names none, there is nothing to check.
func requireTextCollation(ctx context.Context, tx pgx.Tx, opts initDBOptions) error {
	if opts.engine.Resolved() == forma.EngineAuroraDSQL {
		return nil
	}
	if err := textcollation.Require(ctx, tx, ownedTables(opts)); err != nil {
		return fmt.Errorf("check the text collation of Forma's tables: %w", err)
	}
	return nil
}
