package model

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// runtimeSetDDL renders a CREATE TABLE statement that declares exactly the
// runtime column set, in descriptor order, so the tests below can perturb it.
func runtimeSetDDL(t *testing.T) string {
	t.Helper()
	lines := make([]string, 0, len(EntityMainColumnDescriptors)+2)
	lines = append(lines, `CREATE TABLE IF NOT EXISTS "entity_main" (`)
	for _, desc := range EntityMainColumnDescriptors {
		lines = append(lines, "\t\t"+desc.Name+" "+columnKindSQLType[desc.Kind]+",")
	}
	lines = append(lines, "\t\tPRIMARY KEY (ltbase_schema_id, ltbase_row_id)", ")")
	return strings.Join(lines, "\n")
}

func TestEntityMainDDLDrift_ExactRuntimeSetIsClean(t *testing.T) {
	assert.Nil(t, EntityMainDDLDrift(runtimeSetDDL(t)))
	// NOT NULL and alignment padding are not part of the contract.
	padded := strings.ReplaceAll(runtimeSetDDL(t), "ltbase_row_id UUID,", "ltbase_row_id      UUID NOT NULL,")
	assert.Nil(t, EntityMainDDLDrift(padded))
}

func TestEntityMainDDLDrift_ReportsEachKindOfDrift(t *testing.T) {
	ddl := runtimeSetDDL(t)
	// A column the DDL creates but the runtime never touches (#585 shape).
	extra := strings.Replace(ddl, "\t\tbigint_03 BIGINT,", "\t\tbigint_03 BIGINT,\n\t\tbigint_04 BIGINT,", 1)
	require.Equal(t, []string{"DDL column bigint_04 is not a runtime column: never written, projected or flushed"}, EntityMainDDLDrift(extra))

	missing := strings.Replace(ddl, "\t\tdouble_03 DOUBLE PRECISION,\n", "", 1)
	require.Equal(t, []string{"runtime column double_03 is not declared by the DDL"}, EntityMainDDLDrift(missing))

	retyped := strings.Replace(ddl, "\t\tsmallint_03 SMALLINT,", "\t\tsmallint_03 INTEGER,", 1)
	require.Equal(t, []string{"column smallint_03 is declared INTEGER, runtime kind expects SMALLINT"}, EntityMainDDLDrift(retyped))
}
