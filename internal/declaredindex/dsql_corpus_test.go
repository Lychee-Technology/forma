package declaredindex

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/dsqllint/corpus"
)

// TestDSQLCorpus exports the declared-index SQL init-db sends for the DSQL
// lint gate (#740): the catalog reads Ensure plans from and one index build.
// DropSQL is not here: it is printed for an operator, never run, and dsql-lint
// 0.2.22 cannot parse DROP INDEX CONCURRENTLY.
func TestDSQLCorpus(t *testing.T) {
	cols := []string{leadingColumn, "text_01", "bigint_01", trailingColumn}
	ix := Index{Table: "entity_main", Name: Name("entity_main", cols), Columns: cols}
	corpus.Export(t, corpus.Unit{Name: "internal-declaredindex", Engine: forma.EngineAuroraDSQL, Statements: []corpus.Statement{
		corpus.Rendered("internal/declaredindex/ensure.go:tableSchemaQuery", tableSchemaQuery),
		corpus.Rendered("internal/declaredindex/ensure.go:catalogQuery", catalogQuery),
		corpus.Rendered("internal/declaredindex/plan.go:Index.CreateSQL", ix.CreateSQL()),
	}})
}
