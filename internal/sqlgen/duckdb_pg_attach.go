package sqlgen

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/lychee-technology/forma/internal/sqlutil"
)

// The federated hot leg reads PostgreSQL through a catalog the DuckDB
// postgres extension attaches. DuckDB 2.0's extension dropped the
// postgres_scan(conn, schema, table) table function the template used
// before (#752), so each scan is now a catalog-qualified table reference,
// <alias>."schema"."table". The engine runs PostgresAttachSQL on a pinned
// pooled connection, under a process-wide guard, and then runs the query on
// that same connection (federated.DuckDBClientQueryExecutor's
// queryWithPostgres), so the query reads the database the attach reached.
// Only an executor without that pinned-connection seam (the engine-seam
// fakes, the e2e harness) gets PostgresAttachSQL at the head of the query's
// statement batch instead.
//
// The alias names the connection string without revealing it. An ATTACH is
// shared by every connection of a DuckDB database, and two engines can share
// one database (a file-backed DuckDB opens through duckdb-go's instance
// cache), so a fixed alias would let ATTACH IF NOT EXISTS keep the first
// engine's PostgreSQL and answer the second engine's query from it. A plain
// hash of the connection string would leak a guessable digest of the
// password into every rendered query and log line, so the alias is an HMAC
// under a key drawn once per process. Attachments do not outlive the
// process, so an alias never has to match across processes.
var pgAttachAliasKey = newPGAttachAliasKey()

func newPGAttachAliasKey() []byte {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		panic(fmt.Sprintf("sqlgen: draw the postgres attach alias key: %v", err))
	}
	return key
}

// PostgresCatalogAlias returns the DuckDB catalog name the hot leg attaches
// connStr under: stable for one connection string within the process,
// distinct for different ones.
func PostgresCatalogAlias(connStr string) string {
	mac := hmac.New(sha256.New, pgAttachAliasKey)
	mac.Write([]byte(connStr))
	return "forma_pg_" + hex.EncodeToString(mac.Sum(nil)[:8])
}

// PostgresAttachSQL returns the statements the engine runs ahead of every
// hot-leg query: attach connStr read-only under PostgresCatalogAlias(connStr),
// and keep filters out of what the scans send PostgreSQL. IF NOT EXISTS makes
// the attach a no-op on a database that already holds it, and running it per
// query re-attaches a database DuckDB invalidated and the recovering
// connector reopened (#702) without any state of the engine's own.
//
// Scans of an attached catalog push filters into their COPY by default,
// which postgres_scan never did. Pushing $PG_WHERE_CLAUSE would move
// predicates DuckDB evaluates today into PostgreSQL, which compares an
// unconstrained NUMERIC exactly where DuckDB compares the DOUBLE it widens it
// to, so a query could select different rows on the hot leg than on parquet
// (the #621 numeric transport question). Whether to push down is #631's open
// decision, so the hot leg keeps sending whole-table COPYs until it is made
// (design.md §6.1).
//
// The statements embed the connection string, password included, so they
// never belong in the rendered query, its execution plan or a log line;
// DuckDB's error prose on an attach failure quotes it too, which the engine
// scrubs (#306).
func PostgresAttachSQL(connStr string) string {
	return fmt.Sprintf("ATTACH IF NOT EXISTS '%s' AS %s (TYPE postgres, READ_ONLY);\nSET pg_enable_filter_pushdown = false;",
		sqlutil.EscapeLiteral(connStr), PostgresCatalogAlias(connStr))
}

// PostgresTableRef renders the catalog-qualified reference to schema.table
// in the catalog attached under alias. The schema and table come from
// deployment configuration, not from callers, and are quoted as
// identifiers regardless.
func PostgresTableRef(alias, schema, table string) string {
	return alias + "." + quoteDuckDBIdent(schema) + "." + quoteDuckDBIdent(table)
}

func quoteDuckDBIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
