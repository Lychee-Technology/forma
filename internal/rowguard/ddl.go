// Package rowguard names and creates the row-guard table, the per-row write
// lock of the Aurora DSQL engine (#633).
//
// DSQL has no advisory locks and never makes a transaction wait. What it has
// is commit-time conflict detection: of two concurrent transactions that
// write the same row, the one that commits second fails with SQLSTATE 40001.
// So on DSQL every writer and asserter of a (schema_id, row_id) first writes
// that row's guard row, and two transactions that touch one entity row always
// share a row to conflict on, even when the entity row does not exist yet or
// no longer exists. On PostgreSQL the table exists but stays empty: the
// PostgreSQL engine locks rows with pg_advisory_xact_lock instead.
//
// The table is derived state, like the unique-key tables: it is never in
// change_log, never flushed to S3 and never read by DuckDB. A guard row
// outlives its entity row, because a recreate of the row ID must conflict
// with the delete it races (#274).
package rowguard

import (
	"fmt"

	"github.com/lychee-technology/forma/internal/sqlutil"
)

// TableSuffix names the row-guard table after the entity_main table it
// guards.
const TableSuffix = "_row_guard"

// Table names the row-guard table of entityMain. A schema-qualified name
// keeps its qualifier, and a quoted one its quotes.
func Table(entityMain string) string { return sqlutil.SuffixedIdentifier(entityMain, TableSuffix) }

// tableDDL is the row-guard table. generation counts the transactions that
// have written the row: the write is what DSQL adjudicates, and the counter
// makes it change the row rather than rewrite it with equal values.
const tableDDL = `CREATE TABLE IF NOT EXISTS %s (
		ltbase_schema_id SMALLINT NOT NULL,
		ltbase_row_id    UUID     NOT NULL,
		generation       BIGINT   NOT NULL,
		PRIMARY KEY (ltbase_schema_id, ltbase_row_id)
	)`

// Statement answers the row-guard table's DDL, the one source init-db and
// both e2e harnesses create it from. table is an identifier as the statement
// should spell it: the caller quotes it if it needs quoting. The statement
// has no secondary index, so it is one DDL statement on DSQL too.
func Statement(table string) string { return fmt.Sprintf(tableDDL, table) }

// StatementFor answers Statement for an entity_main name that needs no
// quoting, as both e2e harnesses name it.
func StatementFor(entityMain string) string { return Statement(Table(entityMain)) }
