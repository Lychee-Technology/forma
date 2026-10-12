package main

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/declaredindex"
)

// ensureDeclaredIndexes is init-db's declared-index step (#647). It runs after
// the table transaction commits, because CREATE INDEX CONCURRENTLY cannot run
// inside one, and builds each missing x-forma-indexes index as its own
// autocommit statement on conn so concurrent writes are never blocked.
//
// The declarations are read through the file registry the server loads at
// startup, over the schemas the registry table now holds, so init-db plans
// exactly the set the server will validate. It prints what it built, found and
// left alone, and fails when a declared index is invalid or mismatched; it
// never drops an index.
func ensureDeclaredIndexes(ctx context.Context, pool *pgxpool.Pool, conn declaredindex.Conn, opts initDBOptions, out io.Writer) error {
	if opts.skipDeclaredIndexes {
		fmt.Fprintln(out, "Skipped declared indexes (--skip-declared-indexes).")
		return nil
	}
	if opts.schemaDir == "" {
		fmt.Fprintln(out, "Skipped declared indexes: no --schema-dir to read x-forma-indexes from.")
		return nil
	}

	registry, err := loadDeclaringRegistry(ctx, pool, opts)
	if err != nil {
		return fmt.Errorf("declared indexes on %s: %w", opts.entityMain, err)
	}
	report, err := declaredindex.EnsureFromRegistry(ctx, conn, opts.entityMain, registry)
	printDeclaredIndexReport(out, opts.entityMain, report)
	if err != nil {
		return fmt.Errorf("declared indexes on %s: %w", opts.entityMain, err)
	}
	fmt.Fprintf(out, "Declared indexes: %d created, %d already present, %d unreferenced\n",
		len(report.Created), len(report.Present), len(report.Unreferenced))
	return nil
}

// loadDeclaringRegistry loads every registered schema from opts.schemaDir. A
// directory without schema files yields a nil registry, which declares
// nothing; the step still runs, so an index whose declaration was removed is
// reported.
func loadDeclaringRegistry(ctx context.Context, pool *pgxpool.Pool, opts initDBOptions) (forma.SchemaRegistry, error) {
	names, err := listSchemaNames(opts.schemaDir)
	if err != nil {
		return nil, fmt.Errorf("find schemas that may declare indexes: %w", err)
	}
	if len(names) == 0 {
		return nil, nil
	}
	registry, err := buildToolSchemaRegistry(ctx, pool, opts.schemaTable, opts.schemaDir)
	if err != nil {
		return nil, fmt.Errorf("load schemas from %s for declared indexes: %w", opts.schemaDir, err)
	}
	return registry, nil
}

// printDeclaredIndexReport writes one line per declared index Ensure built,
// found in place, or found unreferenced. A failed run's report is partial, so
// the closing count is printed only on success.
func printDeclaredIndexReport(out io.Writer, table string, report declaredindex.Report) {
	for _, ix := range report.Created {
		fmt.Fprintf(out, "Created declared index: %s on %s (%s) for %s\n",
			ix.Name, table, strings.Join(ix.Columns, ", "), declaredindex.FormatSources(ix.Sources))
	}
	for _, ix := range report.Present {
		fmt.Fprintf(out, "Declared index already present: %s on %s (%s) for %s\n",
			ix.Name, table, strings.Join(ix.Columns, ", "), declaredindex.FormatSources(ix.Sources))
	}
	for _, ix := range report.Unreferenced {
		fmt.Fprintf(out, "Unreferenced declared index: %s on %s (%s): no schema declares this key; init-db never drops indexes, so once nothing reads through it, drop it yourself with %s\n",
			ix.Name, table, strings.Join(ix.Columns, ", "), declaredindex.DropSQL(report.Schema, ix.Name))
	}
}
