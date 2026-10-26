package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"

	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma/internal/schemaledger"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// ledgerDDL is init-db's schema generation ledger (#733): the registry's
// active-generation column, which ADD COLUMN IF NOT EXISTS adds to a registry
// created before it, and the two ledger tables. The text comes from
// schemaledger.Statements, the one source the e2e harnesses use too. It runs
// after the core tables, since it alters the registry table.
func ledgerDDL(opts initDBOptions) []tableDDL {
	stmts := schemaledger.Statements(opts.schemaTable)
	generation := sqlutil.SuffixedIdentifier(opts.schemaTable, schemaledger.GenerationTableSuffix)
	attribute := sqlutil.SuffixedIdentifier(opts.schemaTable, schemaledger.AttributeTableSuffix)
	return []tableDDL{
		{
			statement: stmts[0],
			announce:  fmt.Sprintf("Ensured active generation column: %s.%s\n", opts.schemaTable, schemaledger.PointerColumn),
			failure:   "ensure active generation column",
		},
		{
			statement: stmts[1],
			announce:  fmt.Sprintf("Created schema generation table: %s\n", generation),
			failure:   "ensure schema generation table",
		},
		{
			statement: stmts[2],
			announce:  fmt.Sprintf("Created schema generation attribute table: %s\n", attribute),
			failure:   "ensure schema generation attribute table",
		},
	}
}

// recordFirstGeneration records generation 1 of a schema registerSchemas has
// just inserted, in the same transaction: a schema init-db registers starts
// with a baseline, since it has no stored data to audit. A schema without an
// attributes file is registered with no generation, as before the ledger;
// baseline-schema seeds it once the file exists.
func recordFirstGeneration(ctx context.Context, tx pgx.Tx, schemaTable, schemaDir, schemaName string, schemaID int16) error {
	decl, _, err := schemameta.ReadDeclaration(schemaDir, schemaName, schemaID)
	if errors.Is(err, fs.ErrNotExist) {
		fmt.Printf("Schema %s has no attributes file, so no generation is recorded; run baseline-schema once it has one\n", schemaName)
		return nil
	}
	if err != nil {
		return fmt.Errorf("record generation 1 of schema %s: %w", schemaName, err)
	}
	rec := schemaledger.Record{SchemaID: schemaID, Hash: decl.Hash, Entries: decl.Entries, RecordedBy: "init-db"}
	if err := schemaledger.Write(ctx, tx, schemaTable, rec); err != nil {
		return fmt.Errorf("record generation 1 of schema %s: %w", schemaName, err)
	}
	fmt.Printf("Recorded generation 1 of schema %s\n", schemaName)
	return nil
}
