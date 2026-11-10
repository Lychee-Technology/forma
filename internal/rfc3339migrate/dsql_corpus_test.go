package rfc3339migrate

import (
	"testing"

	"github.com/google/uuid"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/dsqllint/corpus"
)

// TestDSQLCorpus exports the migration's statements for the DSQL lint gate
// (#740): the count apply-schema and migrate-rfc3339 share, a page of the
// rewrite's candidates, one rewrite batch, and the listing of the images the rewrite leaves.
func TestDSQLCorpus(t *testing.T) {
	target := Target{SchemaName: "events", SchemaID: 300, AttrID: 1, AttrName: "due", Column: forma.MainColumnText02}
	builders := []struct {
		label string
		build func() (string, []any, error)
	}{
		{"internal/rfc3339migrate/migrate.go:BuildCountQuery", func() (string, []any, error) { return BuildCountQuery("entity_main", target) }},
		{"internal/rfc3339migrate/migrate.go:BuildCandidatesQuery", func() (string, []any, error) {
			return BuildCandidatesQuery("entity_main", target, BatchSize, nil)
		}},
		{"internal/rfc3339migrate/migrate.go:BuildCandidatesQuery", func() (string, []any, error) {
			return BuildCandidatesQuery("entity_main", target, BatchSize, &uuid.Nil)
		}},
		{"internal/rfc3339migrate/migrate.go:BuildRewriteStatement", func() (string, []any, error) {
			return BuildRewriteStatement("entity_main", target, []uuid.UUID{uuid.Nil}, []string{"2024-01-02T03:04:05Z"})
		}},
		{"internal/rfc3339migrate/migrate.go:BuildOthersQuery", func() (string, []any, error) { return BuildOthersQuery("entity_main", target) }},
	}
	var statements []corpus.Statement
	for _, b := range builders {
		query, _, err := b.build()
		if err != nil {
			t.Fatalf("%s: %v", b.label, err)
		}
		statements = append(statements, corpus.Rendered(b.label, query))
	}
	corpus.Export(t, corpus.Unit{Name: "internal-rfc3339migrate", Engine: forma.EngineAuroraDSQL, Statements: statements})
}
