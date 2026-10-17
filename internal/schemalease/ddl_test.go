package schemalease

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableFollowsEntityMain(t *testing.T) {
	assert.Equal(t, "entity_main_schema_lease", Table("entity_main"))
	assert.Equal(t, "tenant.entity_main_schema_lease", Table("tenant.entity_main"))
	assert.Equal(t, `"tenant"."Entity_schema_lease"`, Table(`"tenant"."Entity"`))
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
