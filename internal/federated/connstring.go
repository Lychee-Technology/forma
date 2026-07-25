package federated

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma/internal/pgdsn"
)

// DuckDBPostgresConnStringFromPool derives the libpq-style connection string
// DuckDB's postgres_scanner needs from an existing pgx pool. It returns ""
// for a nil pool or pool config.
//
// Every string value is quoted via pgdsn.Quote, the same rule internal/cdc's
// BuildPGDSN has used since #290. The unquoted form this replaced was two bugs:
//
//   - A password (or host, user, dbname) containing a space produced a DSN libpq
//     could not parse, so postgres_scan failed to attach at all.
//   - The credential scrubber cannot tell where an unquoted value ends. It
//     terminates on whitespace, so an attach failure — whose prose quotes this
//     whole string back — logged the tail of the password past the placeholder,
//     which is the exposure #301 exists to close.
//
// All four string fields are quoted, not just the password: libpq accepts quoted
// values for every keyword, a host may be a socket path, and user and database
// names may legally contain spaces. Quoting uniformly means no field can be the
// one that breaks parsing. sslmode is deliberately not emitted here — it is
// absent from this DSN today, and emitting it with an empty quoted value would be
// rejected by libpq.
//
// The result is embedded in a single-quoted DuckDB SQL literal
// (postgres_scan('{{.PG_CONN}}', …)), so the renderer escapes it; see
// escapeSQLLiteral in internal/sqlgen/duckdb_template_renderer.go.
func DuckDBPostgresConnStringFromPool(pool *pgxpool.Pool) string {
	if pool == nil {
		return ""
	}
	cfg := pool.Config()
	if cfg == nil {
		return ""
	}
	connCfg := cfg.ConnConfig
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s",
		pgdsn.Quote(connCfg.Host),
		connCfg.Port,
		pgdsn.Quote(connCfg.User),
		pgdsn.Quote(connCfg.Password),
		pgdsn.Quote(connCfg.Database),
	)
}
