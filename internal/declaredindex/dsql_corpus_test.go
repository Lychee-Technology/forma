package declaredindex

import (
	"testing"

	"github.com/lychee-technology/forma/internal/dsqllint/corpus"
)

// TestDSQLCorpus renders the declared-index SQL init-db sends for the DSQL
// lint gate (#740): the catalog reads Ensure plans from and one index build.
// DropSQL is printed for an operator, never run, and dsql-lint 0.2.22 cannot
// parse DROP INDEX CONCURRENTLY, so the inventory lists it as uncovered.
func TestDSQLCorpus(t *testing.T) {
	cols := []string{leadingColumn, "text_01", "bigint_01", trailingColumn}
	ix := Index{Table: "entity_main", Name: Name("entity_main", cols), Columns: cols}
	corpus.Export(t, corpus.Unit{Name: "internal-declaredindex", Statements: []corpus.Statement{
		{Origin: "internal/declaredindex/ensure.go:tableSchemaQuery", SQL: tableSchemaQuery},
		{Origin: "internal/declaredindex/ensure.go:catalogQuery", SQL: catalogQuery},
		{Origin: "internal/declaredindex/plan.go:Index.CreateSQL", SQL: ix.CreateSQL()},
	}})
}
