package main

import (
	"fmt"

	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// publicationDDL is init-db's share of Protocol V2's publication contract
// (#895, docs/publication-protocol-v2.md): the per-schema publication record
// and generation tables, named after entity_main. The text comes from
// publication.Statements, the one source the publication package's own
// suites create them from, rendered for opts.engine.
//
// init-db only creates the tables. No Forma code path inserts, reads or
// updates them yet: registering a schema writes no record, and cdc-flush,
// cdc-init, the compactor and the federated reader behave as before. The
// writers and the reader move onto the record together in #896, because a
// record written while V1 writers still run goes stale.
func publicationDDL(opts initDBOptions) []tableDDL {
	record := sqlutil.SuffixedIdentifier(opts.entityMain, publication.RecordTableSuffix)
	generation := sqlutil.SuffixedIdentifier(opts.entityMain, publication.GenerationTableSuffix)
	stmts := publication.Statements(opts.engine, opts.entityMain)
	return []tableDDL{
		{
			statement: stmts[0],
			announce:  fmt.Sprintf("Created publication record table: %s\n", record),
			failure:   "ensure publication record table",
		},
		{
			statement: stmts[1],
			announce:  fmt.Sprintf("Created publication generation table: %s\n", generation),
			failure:   "ensure publication generation table",
		},
	}
}
