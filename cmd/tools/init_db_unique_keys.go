package main

import (
	"context"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma/internal"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/uniquekey"
)

// uniqueKeyDDL is init-db's share of the unique-key tables (#651): the key
// table and the built-key state table, announced like the core tables, and
// the key table's row index, which runs with the other indexes. The text comes
// from uniquekey.Statements, the one source both e2e harnesses use too.
func uniqueKeyDDL(opts initDBOptions) (tables []tableDDL, index tableDDL) {
	keyTable := uniquekey.KeyTable(opts.entityMain)
	stateTable := uniquekey.StateTable(opts.entityMain)
	statements := uniquekey.Statements(
		quoteIdentifier(keyTable), quoteIdentifier(makeIndexName(keyTable, "row")), quoteIdentifier(stateTable))
	tables = []tableDDL{
		{
			statement: statements[0],
			announce:  fmt.Sprintf("Created unique key table: %s\n", keyTable),
			failure:   "ensure unique key table",
		},
		{
			statement: statements[2],
			announce:  fmt.Sprintf("Created unique key state table: %s\n", stateTable),
			failure:   "ensure unique key state table",
		},
	}
	return tables, tableDDL{statement: statements[1], failure: "create unique key row index"}
}

// ensureUniqueKeys is init-db's unique-key step (#651). A key over a schema
// with no live row is built at once, since there is nothing to scan; a key
// over a schema with rows needs the offline build, which init-db only names,
// because it must run with every writer stopped.
func ensureUniqueKeys(ctx context.Context, pool *pgxpool.Pool, opts initDBOptions, out io.Writer) error {
	if opts.schemaDir == "" {
		fmt.Fprintln(out, "Skipped unique keys: no --schema-dir to read x-forma-unique-keys from.")
		return nil
	}
	registry, err := loadDeclaringRegistry(ctx, pool, opts)
	if err != nil {
		return fmt.Errorf("unique keys on %s: %w", opts.entityMain, err)
	}
	catalog, err := internal.NewUniqueKeyCatalog(registry)
	if err != nil {
		return fmt.Errorf("unique keys on %s: %w", opts.entityMain, err)
	}
	tables := model.StorageTables{EntityMain: opts.entityMain, EAVData: opts.eavTable}
	built, pending, err := catalog.BuildUniqueKeysOfEmptySchemas(ctx, pool, tables)
	for _, k := range built {
		fmt.Fprintf(out, "Built unique key %s of schema %s (no rows to scan)\n", k.Key, k.Schema)
	}
	if err != nil {
		return fmt.Errorf("unique keys on %s: %w", opts.entityMain, err)
	}
	for _, line := range pending {
		fmt.Fprintf(out, "Unique key needs a build (the server refuses to start until it runs): %s\n", line)
	}
	return nil
}
