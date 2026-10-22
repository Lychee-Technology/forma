package main

import (
	"fmt"

	"github.com/lychee-technology/forma/internal/rowguard"
)

// rowGuardDDL is init-db's row-guard table (#633), the Aurora DSQL engine's
// per-row write lock. init-db creates it on every deployment, so switching a
// database to the aurora-dsql engine needs no second schema step; the
// PostgreSQL engine leaves it empty. The text comes from rowguard.Statement,
// the one source both e2e harnesses use too.
func rowGuardDDL(opts initDBOptions) tableDDL {
	table := rowguard.Table(opts.entityMain)
	return tableDDL{
		statement: rowguard.Statement(quoteIdentifier(table)),
		announce:  fmt.Sprintf("Created row guard table: %s\n", table),
		failure:   "ensure row guard table",
	}
}
