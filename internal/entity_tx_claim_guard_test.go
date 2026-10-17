package internal

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// claimListTests names the test of each #593 claim-list item (#653).
var claimListTests = map[string]string{
	"TestClaimNeverHandsOutARowTwice_PostgresOnly":  "1. never the same record",
	"TestClaimAtMostOneActiveClaimant_PostgresOnly": "2. at most one active claimant",
	"TestClaimReturnsTheRowsAsStored":               "3. claimed rows updated before return",
	"TestClaimTakesTheFirstRowsInSortOrder":         "4. deterministic ordering and limit",
	"TestClaimReclaimsAnExpiredLease":               "5. expired lease reclaimable",
	"TestClaimLeavesAnActiveLease":                  "6. active lease not reclaimable",
	"TestClaimStaleGenerationCannotComplete":        "7. stale generation fails",
	"TestClaimRollbackReleasesTheRow_PostgresOnly":  "8. rollback releases",
}

// #593 claim list 9: every claim-list test runs on real PostgreSQL. Each one
// builds its env with newOutboxPG, whose newSchemaPG connects through
// testdb.Connect (which fails rather than skips in CI, #385), and no claim
// test file mocks the storage layer.
func TestClaimListRunsOnRealPostgres(t *testing.T) {
	fset := token.NewFileSet()
	files := map[string]*ast.File{}
	for _, name := range []string{
		"entity_tx_claim_integration_test.go",
		"entity_tx_claim_concurrency_integration_test.go",
		"entity_tx_claim_fixture_test.go",
		"entity_conditional_write_integration_test.go",
	} {
		file, err := parser.ParseFile(fset, name, nil, 0)
		require.NoError(t, err)
		files[name] = file
	}
	bodies := map[string]*ast.BlockStmt{}
	for name, file := range files {
		for _, imp := range file.Imports {
			if strings.HasPrefix(name, "entity_tx_claim") {
				assert.NotContains(t, imp.Path.Value, "pgxmock", "%s mocks the storage layer", name)
			}
		}
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil {
				bodies[fn.Name.Name] = fn.Body
			}
		}
	}
	require.True(t, callsFunc(bodies["newSchemaPG"], "testdb", "Connect"), "newSchemaPG connects through testdb.Connect")
	require.True(t, callsFunc(bodies["newOutboxPG"], "", "newSchemaPG"), "newOutboxPG builds on newSchemaPG")
	for test, item := range claimListTests {
		body, ok := bodies[test]
		if assert.True(t, ok, "claim list %s: %s is missing", item, test) {
			assert.True(t, callsFunc(body, "", "newOutboxPG"), "claim list %s: %s does not run on newOutboxPG", item, test)
		}
	}
}

// callsFunc reports whether body calls pkg.name, or name when pkg is empty.
func callsFunc(body *ast.BlockStmt, pkg, name string) bool {
	if body == nil {
		return false
	}
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return !found
		}
		switch fun := call.Fun.(type) {
		case *ast.Ident:
			found = found || (pkg == "" && fun.Name == name)
		case *ast.SelectorExpr:
			x, ok := fun.X.(*ast.Ident)
			found = found || (ok && x.Name == pkg && fun.Sel.Name == name)
		}
		return !found
	})
	return found
}
