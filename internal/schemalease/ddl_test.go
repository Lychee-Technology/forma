package schemalease

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/rowguard"
	"github.com/lychee-technology/forma/internal/sqlutil"
	"github.com/stretchr/testify/assert"
)

func TestTableFollowsEntityMain(t *testing.T) {
	assert.Equal(t, "entity_main_schema_lease", Table("entity_main"))
	assert.Equal(t, "tenant.entity_main_schema_lease", Table("tenant.entity_main"))
	assert.Equal(t, `"tenant"."Entity_schema_lease"`, Table(`"tenant"."Entity"`))
}

// PostgreSQL truncates an identifier to 63 bytes. Unbounded, the lease table
// of an entity_main name of 63 bytes or more would truncate to that name, and
// init-db's CREATE TABLE IF NOT EXISTS would do nothing. The bounded name is
// never entity_main's, nor the row-guard table's.
func TestTableOfALongEntityMainIsItsOwnTable(t *testing.T) {
	for _, n := range []int{50, 62, 63, 64, 80} {
		entityMain := strings.Repeat("m", n)
		table := Table(entityMain)
		assert.LessOrEqual(t, len(table), sqlutil.MaxIdentifierLen, table)
		assert.True(t, strings.HasSuffix(table, TableSuffix), table)
		assert.NotEqual(t, entityMain[:min(n, sqlutil.MaxIdentifierLen)], table, "%d bytes", n)
		assert.NotEqual(t, rowguard.Table(entityMain), table, "%d bytes", n)
	}
}

// TestStatementIsOneDSQLStatement pins what makes the DDL valid on Aurora
// DSQL: one CREATE TABLE, keyed by the primary key alone.
func TestStatementIsOneDSQLStatement(t *testing.T) {
	ddl := StatementFor("entity_main")
	assert.True(t, strings.HasPrefix(ddl, "CREATE TABLE IF NOT EXISTS entity_main_schema_lease ("), ddl)
	assert.Contains(t, ddl, "PRIMARY KEY (schema_id)")
	assert.NotContains(t, ddl, ";")
	assert.NotContains(t, strings.ToUpper(ddl), "INDEX")
}
