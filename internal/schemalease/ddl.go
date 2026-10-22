// Package schemalease names and creates the schema-lease table, the
// per-schema CDC lock of the Aurora DSQL engine (#633).
//
// cdc-flush, cdc-init and manifest-reconcile keep apart per schema (#290).
// On PostgreSQL they take the session advisory lock
// pg_try_advisory_lock(schemaID, schemaID). DSQL has no advisory locks and
// no session state to tie a lock to, so there a holder writes the schema's
// lease row instead: a holder token and an expiry on the database clock,
// which the holder renews while it works. On PostgreSQL the table exists but
// stays empty.
//
// The table is named after entity_main, not change_log, because all three
// holders are given the entity_main table and only cdc-flush is given the
// change_log table: holders that named the lease table differently would
// never exclude each other.
package schemalease

import (
	"fmt"

	"github.com/lychee-technology/forma/internal/sqlutil"
)

// TableSuffix names the schema-lease table after the entity_main table of
// the deployment it locks.
const TableSuffix = "_schema_lease"

// Table names the schema-lease table of entityMain. A schema-qualified name
// keeps its qualifier, and a quoted one its quotes. The name is bounded to
// PostgreSQL's identifier limit like the row-guard table's: unbounded, the
// lease table of a 63-byte entity_main name would truncate to entity_main's
// own name.
func Table(entityMain string) string { return sqlutil.SuffixedIdentifier(entityMain, TableSuffix) }

// tableDDL is the schema-lease table: at most one lease row per schema.
// expires_at is epoch milliseconds on the database clock, so every holder
// judges expiry by one clock whatever its host's.
const tableDDL = `CREATE TABLE IF NOT EXISTS %s (
		schema_id  SMALLINT NOT NULL,
		holder     UUID     NOT NULL,
		expires_at BIGINT   NOT NULL,
		PRIMARY KEY (schema_id)
	)`

// Statement answers the schema-lease table's DDL, the one source init-db and
// both e2e harnesses create it from. table is an identifier as the statement
// should spell it: the caller quotes it if it needs quoting. The statement
// has no secondary index, so it is one DDL statement on DSQL too.
func Statement(table string) string { return fmt.Sprintf(tableDDL, table) }

// StatementFor answers Statement for an entity_main name that needs no
// quoting, as both e2e harnesses name it.
func StatementFor(entityMain string) string { return Statement(Table(entityMain)) }
