package rowguard

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/sqlutil"
	"github.com/lychee-technology/forma/internal/uniquekey"
	"github.com/stretchr/testify/assert"
)

func TestTableFollowsEntityMain(t *testing.T) {
	assert.Equal(t, "entity_main_row_guard", Table("entity_main"))
	assert.Equal(t, "tenant.entity_main_row_guard", Table("tenant.entity_main"))
	assert.Equal(t, `"tenant"."Entity_row_guard"`, Table(`"tenant"."Entity"`))
}

// PostgreSQL truncates an identifier to 63 bytes. Unbounded, the guard table
// of an entity_main name of 63 bytes or more would truncate to that name:
// init-db's CREATE TABLE IF NOT EXISTS would do nothing and the factory's
// check would find entity_main, so the engine would start and every write
// fail at its first guard upsert. The bounded name is never entity_main's,
// nor a unique-key table's.
func TestTableOfALongEntityMainIsItsOwnTable(t *testing.T) {
	for _, n := range []int{50, 62, 63, 64, 80} {
		entityMain := strings.Repeat("m", n)
		table := Table(entityMain)
		assert.LessOrEqual(t, len(table), sqlutil.MaxIdentifierLen, table)
		assert.True(t, strings.HasSuffix(table, TableSuffix), table)
		// What PostgreSQL names entity_main, truncated or not.
		assert.NotEqual(t, entityMain[:min(n, sqlutil.MaxIdentifierLen)], table, "%d bytes", n)
		assert.NotContains(t, []string{uniquekey.KeyTable(entityMain), uniquekey.StateTable(entityMain)}, table)
	}
}

// TestStatementIsOneDSQLStatement pins what makes the DDL valid on Aurora
// DSQL: one CREATE TABLE, keyed by the primary key alone.
func TestStatementIsOneDSQLStatement(t *testing.T) {
	ddl := StatementFor("entity_main")
	assert.True(t, strings.HasPrefix(ddl, "CREATE TABLE IF NOT EXISTS entity_main_row_guard ("), ddl)
	assert.Contains(t, ddl, "PRIMARY KEY (ltbase_schema_id, ltbase_row_id)")
	assert.NotContains(t, ddl, ";")
	assert.NotContains(t, strings.ToUpper(ddl), "INDEX")
}
