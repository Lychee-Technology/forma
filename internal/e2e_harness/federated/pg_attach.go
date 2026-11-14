package federated

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/lychee-technology/forma/internal/sqlutil"
)

// The harness's own reference queries read the hot tier from inside DuckDB,
// through a PostgreSQL catalog they attach with DuckDB's postgres extension.
// The engine stopped doing so when its attach hot leg was retired (#858): it
// reads the hot tier over pgx. These helpers are the harness's copy of what
// the engine used, kept so the reference stays independent of the engine's
// own read path.

// pgCatalogAlias names the catalog the harness attaches connStr under. A plain
// digest is enough here: the connection strings are the test containers'.
func pgCatalogAlias(connStr string) string {
	sum := sha256.Sum256([]byte(connStr))
	return "harness_pg_" + hex.EncodeToString(sum[:8])
}

// pgAttachSQL attaches connStr read-only under pgCatalogAlias(connStr), and
// keeps filters out of what the scans send PostgreSQL, as the engine's attach
// hot leg did. IF NOT EXISTS makes it a no-op on a database that holds it.
func pgAttachSQL(connStr string) string {
	return fmt.Sprintf("ATTACH IF NOT EXISTS '%s' AS %s (TYPE postgres, READ_ONLY);\nSET pg_enable_filter_pushdown = false;",
		sqlutil.EscapeLiteral(connStr), pgCatalogAlias(connStr))
}

// pgTableRef renders the catalog-qualified reference to schema.table in the
// catalog attached for connStr.
func pgTableRef(connStr, schema, table string) string {
	return pgCatalogAlias(connStr) + "." + quoteDuckDBIdent(schema) + "." + quoteDuckDBIdent(table)
}

func quoteDuckDBIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
