package publication

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	publicationImport = "github.com/lychee-technology/forma/internal/publication"
	publicationDir    = "internal/publication"
	// federatedHarnessPublication is the e2e harness file that registers
	// and publishes the federated harness's schemas.
	federatedHarnessPublication = "internal/e2e_harness/federated/publication.go"
)

// transitions are the identifiers of this package that move a record or
// write its history, and owners the production packages (by directory) or
// files allowed to use each of them: registration in init-db
// (cmd/tools/init_db_records.go), and the cdc-flush, cdc-init and
// compaction publication paths (internal/cdcv2, which the compactor in
// internal/compaction publishes through). The federated e2e harness's
// publication file stands in for both, registration and publication, over
// the synthetic parquet the benchmark loads (cmd/benchmark runs it outside
// any test, so it is not a _test.go file). The drop (Reset, and HoldDropped
// over its purge) is drop-schema's (cmd/tools/drop_schema.go), and the
// recovery of a lost record (InitializeAfterLoss) init-db's. Garbage
// collection (RaiseHorizon, Retire, RetireRecovery, Forget) is
// internal/reconcile's, and binding a deployment's storage target
// (BindStorage) is manifest-reconcile's CLI.
var transitions = map[string]map[string]bool{
	"Initialize":          {"cmd/tools/init_db_records.go": true, federatedHarnessPublication: true},
	"InitializeAfterLoss": {"cmd/tools/init_db_records.go": true},
	"Activate":            {"cmd/tools/init_db_records.go": true},
	"Publish":             {"internal/cdcv2": true, federatedHarnessPublication: true},
	"HoldLineage":         {"internal/cdcv2": true},
	"HoldDropped":         {"cmd/tools/drop_schema.go": true},
	"Fence":               {"internal/cdcv2": true},
	"Reset":               {"cmd/tools/drop_schema.go": true},
	"RaiseHorizon":        {"internal/reconcile": true},
	"Retire":              {"internal/reconcile": true},
	"RetireRecovery":      {"internal/reconcile": true},
	"Forget":              {"internal/reconcile": true},
	"BindStorage":         {"cmd/tools/manifest_reconcile.go": true},
}

// TestOnlyTheProtocolOwnersMoveAPublicationRecord is a static check over
// every production file of the module that keeps the publication record's
// transitions where docs/publication-protocol-v2.md assigns them
// ("Ownership and migration"). Outside this package it refuses:
//
//   - a use of a transition (transitions) outside its owners;
//   - a dot import of this package, which would hide which identifiers a
//     file uses;
//   - a string literal that spells a publication table name: every statement
//     over the tables is this package's, so the protocol's invariants are
//     argued over its SQL alone.
//
// Exemptions name a package directory or a file, never a subtree: a
// subpackage of an owner is held to every rule. Reading a record (Read,
// Readable, Resolve, KindsSince, Retained, Expired) and the DDL
// (Statements, TablesFor) are free.
func TestOnlyTheProtocolOwnersMoveAPublicationRecord(t *testing.T) {
	root := filepath.Join("..", "..")
	files, err := productionFiles(root)
	require.NoError(t, err)
	require.Greater(t, len(files), 200, "the module's non-test Go files")
	violations, err := publicationViolations(root, files)
	require.NoError(t, err)
	require.Empty(t, violations)

	t.Run("the guard catches a stray transition, a dot import and a spelled table name", func(t *testing.T) {
		dir := t.TempDir()
		for rel, src := range map[string]string{
			// A reader may read the record, but not publish.
			"internal/federated/planted.go": "package federated\n\nimport \"" + publicationImport + "\"\n\nvar _ = publication.Read\nvar _ = publication.Publish\n",
			// init-db's DDL file is not its registration file.
			"cmd/tools/init_db_publication.go": "package main\n\nimport pub \"" + publicationImport + "\"\n\nvar _ = pub.Statements\nvar _ = pub.Initialize\n",
			// The registration file may register, and nothing else.
			"cmd/tools/init_db_records.go":   "package main\n\nimport \"" + publicationImport + "\"\n\nvar _ = publication.Activate\nvar _ = publication.Reset\n",
			"cmd/tools/init_db_collation.go": "package main\n\nimport . \"" + publicationImport + "\"\n\nvar _ = Statements\n",
			// The writer publishes, but never spells a table.
			"internal/cdcv2/planted.go": "package cdcv2\n\nimport \"" + publicationImport + "\"\n\nvar _ = publication.Publish\n\nconst table = \"z_publication\"\n",
			// A subpackage of an owner is another package.
			"internal/cdcv2/sub/planted.go": "package sub\n\nimport \"" + publicationImport + "\"\n\nvar _ = publication.HoldLineage\n",
			// This package may spell its tables and use itself.
			"internal/publication/planted.go": "package publication\n\nconst table = \"y_publication\"\n\nvar _ = Publish\n",
			".hidden/planted.go":              "package hidden\n\nconst table = \"x_publication\"\n",
		} {
			file := filepath.Join(dir, filepath.FromSlash(rel))
			require.NoError(t, os.MkdirAll(filepath.Dir(file), 0o755))
			require.NoError(t, os.WriteFile(file, []byte(src), 0o600))
		}
		files, err := productionFiles(dir)
		require.NoError(t, err)
		require.Len(t, files, 7, "the walk scans every production file but tests and hidden directories")
		violations, err := publicationViolations(dir, files)
		require.NoError(t, err)
		require.Equal(t, []string{
			"cmd/tools/init_db_collation.go dot-imports " + publicationImport,
			"cmd/tools/init_db_publication.go uses publication.Initialize",
			"cmd/tools/init_db_records.go uses publication.Reset",
			`internal/cdcv2/planted.go spells a publication table name: "z_publication"`,
			"internal/cdcv2/sub/planted.go uses publication.HoldLineage",
			"internal/federated/planted.go uses publication.Publish",
		}, violations)
	})

	t.Run("the scanner names the file it cannot parse", func(t *testing.T) {
		dir := t.TempDir()
		broken := filepath.Join(dir, "broken.go")
		require.NoError(t, os.WriteFile(broken, []byte("package"), 0o600))
		_, err := publicationViolations(dir, []string{broken})
		require.ErrorContains(t, err, "parse "+broken)
	})
}

// productionFiles answers the non-test Go files under root, skipping hidden
// and build directories. It skips no package: what each rule exempts is
// fileViolations' to decide.
func productionFiles(root string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("walk %s: %w", p, err)
		}
		if d.IsDir() {
			if p != root && (strings.HasPrefix(d.Name(), ".") || d.Name() == "build") {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(p, ".go") && !strings.HasSuffix(p, "_test.go") {
			files = append(files, p)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list the production files under %s: %w", root, err)
	}
	return files, nil
}

// publicationViolations answers, sorted, each violation of the rules of
// TestOnlyTheProtocolOwnersMoveAPublicationRecord among files.
func publicationViolations(root string, files []string) ([]string, error) {
	var violations []string
	fset := token.NewFileSet()
	for _, name := range files {
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if err != nil {
			return nil, fmt.Errorf("parse %s for uses of the publication package: %w", name, err)
		}
		rel, err := filepath.Rel(root, name)
		if err != nil {
			return nil, fmt.Errorf("name %s relative to the module root %s: %w", name, root, err)
		}
		rel = filepath.ToSlash(rel)
		if path.Dir(rel) == publicationDir {
			continue // this package may spell and use itself
		}
		violations = append(violations, fileViolations(rel, file)...)
	}
	sort.Strings(violations)
	return violations, nil
}

// fileViolations applies the rules to one file outside this package.
func fileViolations(rel string, file *ast.File) []string {
	var violations []string
	name := ""
	for _, imp := range file.Imports {
		if p, _ := strconv.Unquote(imp.Path.Value); p != publicationImport {
			continue
		}
		name = "publication"
		if imp.Name != nil {
			name = imp.Name.Name
		}
		if name == "." {
			violations = append(violations, rel+" dot-imports "+publicationImport)
		}
	}
	ast.Inspect(file, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.SelectorExpr:
			x, ok := n.X.(*ast.Ident)
			if !ok || name == "" || x.Name != name {
				return true
			}
			if owners, transition := transitions[n.Sel.Name]; transition && !owners[rel] && !owners[path.Dir(rel)] {
				violations = append(violations, rel+" uses publication."+n.Sel.Name)
			}
		case *ast.BasicLit:
			if n.Kind == token.STRING && strings.Contains(n.Value, RecordTableSuffix) {
				violations = append(violations, rel+" spells a publication table name: "+n.Value)
			}
		}
		return true
	})
	return violations
}
