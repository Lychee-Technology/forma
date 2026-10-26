package schemameta

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// gateBypasses are the ways production code loads a declaration without the
// generation gate (#733), each with the files allowed to use it: the
// control-plane tools, and directory mode, which has no ledger to gate on.
var gateBypasses = map[string][]string{
	"NewControlPlaneSchemaRegistryContext": {"cmd/tools/init_db_indexes.go"},
	"ControlPlane":                         {"cmd/tools/validate_schema_consistency.go"},
	"NewFileSchemaRegistryFromDirectory":   {"cmd/sample/main.go", "internal/e2e_harness/federated/benchmark/schema.go"},
}

const ledgertestImport = "github.com/lychee-technology/forma/internal/schemaledger/ledgertest"

// TestGateBypassesStayInTheControlPlane: a data-plane process that used a
// bypass, or ledgertest's unaudited recording, would run a declaration the
// ledger never admitted. Test harnesses under internal/e2e_harness may seed
// with ledgertest; nothing else outside tests may.
func TestGateBypassesStayInTheControlPlane(t *testing.T) {
	root := filepath.Join("..", "..")
	files := productionGoFiles(t, root)
	require.Greater(t, len(files), 200, "the module's non-test Go files")
	violations, err := gateBypassViolations(root, files)
	require.NoError(t, err)
	require.Empty(t, violations)

	t.Run("the guard catches a planted bypass and import", func(t *testing.T) {
		dir := t.TempDir()
		planted := filepath.Join(dir, "internal", "cdc", "planted.go")
		require.NoError(t, os.MkdirAll(filepath.Dir(planted), 0o755))
		src := "package cdc\n\nimport (\n\t_ \"" + ledgertestImport + "\"\n\t\"github.com/lychee-technology/forma/internal/schemameta\"\n)\n\n" +
			"var _ = schemameta.NewControlPlaneSchemaRegistryContext\n"
		require.NoError(t, os.WriteFile(planted, []byte(src), 0o600))
		violations, err := gateBypassViolations(dir, []string{planted})
		require.NoError(t, err)
		require.Equal(t, []string{
			"internal/cdc/planted.go imports " + ledgertestImport,
			"internal/cdc/planted.go uses NewControlPlaneSchemaRegistryContext",
		}, violations)
	})
}

// productionGoFiles answers the non-test Go files under root, skipping the
// schemameta package that defines the bypasses and hidden directories.
func productionGoFiles(t *testing.T, root string) []string {
	t.Helper()
	var files []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != root && (strings.HasPrefix(d.Name(), ".") || d.Name() == "build") {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") {
			files = append(files, path)
		}
		return nil
	})
	require.NoError(t, err)
	return files
}

func gateBypassViolations(root string, files []string) ([]string, error) {
	fset := token.NewFileSet()
	var violations []string
	for _, f := range files {
		rel, err := filepath.Rel(root, f)
		if err != nil {
			return nil, err
		}
		rel = filepath.ToSlash(rel)
		if strings.HasPrefix(rel, "internal/schemameta/") || strings.HasPrefix(rel, "internal/schemaledger/ledgertest/") {
			continue
		}
		parsed, err := parser.ParseFile(fset, f, nil, parser.SkipObjectResolution)
		if err != nil {
			return nil, err
		}
		for _, spec := range parsed.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return nil, err
			}
			if path == ledgertestImport && !strings.HasPrefix(rel, "internal/e2e_harness/") {
				violations = append(violations, rel+" imports "+path)
			}
		}
		violations = append(violations, bypassUses(rel, parsed)...)
	}
	sort.Strings(violations)
	return violations, nil
}

func bypassUses(rel string, file *ast.File) []string {
	seen := map[string]bool{}
	ast.Inspect(file, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		allowed, isBypass := gateBypasses[sel.Sel.Name]
		if isBypass && !slices.Contains(allowed, rel) {
			seen[sel.Sel.Name] = true
		}
		return true
	})
	var uses []string
	for name := range seen {
		uses = append(uses, rel+" uses "+name)
	}
	return uses
}
