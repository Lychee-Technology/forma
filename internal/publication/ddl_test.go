package publication

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/textcollation"
)

// TestTablesForFollowsEntityMain pins the table names: suffixed to the
// entity_main table every holder is given, qualifier kept.
func TestTablesForFollowsEntityMain(t *testing.T) {
	require.Equal(t, Tables{
		Record:     `"entity_main_publication"`,
		Generation: `"entity_main_publication_generation"`,
		Epoch:      `"entity_main_publication_epoch"`,
		Storage:    `"entity_main_publication_storage"`,
	}, TablesFor("entity_main"))
	require.Equal(t, Tables{
		Record:     `"app"."em_publication"`,
		Generation: `"app"."em_publication_generation"`,
		Epoch:      `"app"."em_publication_epoch"`,
		Storage:    `"app"."em_publication_storage"`,
	}, TablesFor("app.em"))
}

// TestStatementsUseOnlyPlainRows pins the DSQL-safe shape of the DDL: one
// statement each, no secondary index and none of the column features this
// package does not rely on DSQL for.
func TestStatementsUseOnlyPlainRows(t *testing.T) {
	for _, s := range Statements(forma.EnginePostgreSQL, "entity_main") {
		require.Nil(t, textcollation.DDLDrift(s), "every text column is COLLATE \"C\" on PostgreSQL: %s", s)
	}
	stmts := Statements(forma.EngineAuroraDSQL, "entity_main")
	require.Len(t, stmts, 7)
	require.Equal(t, StorageStatement(forma.EngineAuroraDSQL, "entity_main"), stmts[6], "init-db creates the storage binding table")
	for i, s := range stmts {
		require.NotContains(t, s, "COLLATE", "DSQL refuses a COLLATE clause")
		if i < 3 || i == 6 {
			require.True(t, strings.HasPrefix(s, "CREATE TABLE IF NOT EXISTS "), s)
		} else {
			require.True(t, strings.HasPrefix(s, "ALTER TABLE "), s)
		}
		upper := strings.ToUpper(s)
		for _, banned := range []string{"JSONB", "SERIAL", "REFERENCES", "CHECK", "[]", "CREATE INDEX", "SEQUENCE"} {
			require.NotContains(t, upper, banned, s)
		}
	}
}
