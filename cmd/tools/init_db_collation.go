package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/rowguard"
	"github.com/lychee-technology/forma/internal/schemalease"
	"github.com/lychee-technology/forma/internal/schemaledger"
	"github.com/lychee-technology/forma/internal/textcollation"
	"github.com/lychee-technology/forma/internal/uniquekey"
)

// ownedTables names every table init-db ensures, as SQL spells it: the core
// tables, the unique-key, row-guard and schema-lease tables, and the
// generation ledger. Each is a table Forma owns, so each falls under the text
// collation contract (#816), including those that hold no text today.
func ownedTables(opts initDBOptions) []string {
	ledger := schemaledger.TablesFor(opts.schemaTable)
	return []string{
		quoteIdentifier(opts.schemaTable),
		quoteIdentifier(opts.entityMain),
		quoteIdentifier(opts.eavTable),
		quoteIdentifier(opts.changeLog),
		quoteIdentifier(uniquekey.KeyTable(opts.entityMain)),
		quoteIdentifier(uniquekey.StateTable(opts.entityMain)),
		quoteIdentifier(rowguard.Table(opts.entityMain)),
		quoteIdentifier(schemalease.Table(opts.entityMain)),
		ledger.Generation,
		ledger.Attribute,
	}
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
