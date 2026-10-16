package main

import (
	"fmt"

	"github.com/lychee-technology/forma/internal/schemalease"
)

// schemaLeaseDDL is init-db's schema-lease table (#633), the Aurora DSQL
// engine's per-schema CDC lock. Like the row-guard table it is created on
// every deployment and stays empty on PostgreSQL. The text comes from
// schemalease.Statement, the one source both e2e harnesses use too.
func schemaLeaseDDL(opts initDBOptions) tableDDL {
	table := schemalease.Table(opts.entityMain)
	return tableDDL{
		statement: schemalease.Statement(quoteIdentifier(table)),
		announce:  fmt.Sprintf("Created schema lease table: %s\n", table),
		failure:   "ensure schema lease table",
	}
}
