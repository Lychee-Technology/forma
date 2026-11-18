package production

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const manifestImport = "github.com/lychee-technology/forma/internal/manifest"

// changeLogClear matches a statement that deletes or truncates change_log
// entries, in any case and across line breaks.
var changeLogClear = regexp.MustCompile(`(?is)\b(?:delete\s+from|truncate(?:\s+table)?)\s+(?:only\s+)?"?change_log\b`)

// changeLogHelpers is the one file of the package that may clear change_log:
// the helpers that name why a test does it (onboarded or flushed).
var changeLogHelpers = map[string]bool{"handoff.go": true}

// manifestWrapperTests are the unit tests of the harness's own manifest
// wrappers, which run without the e2e tag and drive them with fake stores:
// that loadManifestsFrom and loadKeptDeltaEntries only treat
// manifest.ErrObjectNotFound as absent, and that an injected S3 fault never
// classifies as manifest.IsNotFound (#464). They test the wrappers, so they
// name the package the wrappers wrap.
var manifestWrapperTests = map[string]bool{
	"faults_test.go":             true,
	"manifest_load_gate_test.go": true,
}

// TestE2ETestsReachV1StandInsOnlyThroughHelpers keeps the production e2e
// tests off the two Protocol V1 stand-ins #896 replaces (#901):
//
//   - No file but changeLogHelpers clears change_log. V2 never clears it, and
//     gives each intent the helpers name a different replacement.
//   - No test file but manifestWrapperTests imports internal/manifest. Tests
//     read and write the published inventory through manifest_access.go and
//     the other harness sources, whose bodies #896 rewrites.
//
// An exemption that no longer imports the package fails too, so the list
// cannot outlive its reason. The scan parses every Go file of the package,
// whatever its build tags, and reads string literals one at a time: a
// statement assembled from fragments, or one that names the table through
// Tables.ChangeLog, is not caught.
func TestE2ETestsReachV1StandInsOnlyThroughHelpers(t *testing.T) {
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	require.Greater(t, len(files), 150, "the package's Go files, e2e-tagged tests included")
	violations, err := standInViolations(files)
	require.NoError(t, err)
	require.Empty(t, violations)

	t.Run("the guard catches planted stand-ins and a stale exemption", func(t *testing.T) {
		// The statements are split across literals so that this file does
		// not plant a violation of its own.
		stmt := "DELETE FROM " + "change_log"
		trunc := "TRUNCATE TABLE " + "change_log"
		dir := t.TempDir()
		planted := map[string]string{
			"handoff.go":         "package p\n\nconst q = \"" + stmt + "\"\n",
			"cdc.go":             "package p\n\nimport _ \"" + manifestImport + "\"\n\nconst q = `" + stmt + " WHERE flushed_at <> 0`\n",
			"lower_e2e_test.go":  "//go:build e2e\n\npackage p\n\nconst q = \"delete\\n\\tfrom " + "change_log\"\n",
			"trunc_e2e_test.go":  "package p\n\nconst q = `" + trunc + "`\n",
			"import_e2e_test.go": "package p\n\nimport m \"" + manifestImport + "\"\n\nvar _ = m.FilterByTier\n",
			"faults_test.go":     "package p\n\nimport _ \"" + manifestImport + "\"\n",
			// A sibling table is not change_log.
			"manifest_load_gate_test.go": "package p\n\nconst q = \"" + stmt + "_archive\"\n",
		}
		var names []string
		for name, src := range planted {
			file := filepath.Join(dir, name)
			require.NoError(t, os.WriteFile(file, []byte(src), 0o600))
			names = append(names, file)
		}
		violations, err := standInViolations(names)
		require.NoError(t, err)
		require.Equal(t, []string{
			`cdc.go clears change_log: "` + stmt + ` WHERE flushed_at <> 0"`,
			"import_e2e_test.go imports " + manifestImport,
			`lower_e2e_test.go clears change_log: "delete\n\tfrom change_log"`,
			"manifest_load_gate_test.go is exempt from the manifest import rule but no longer imports " + manifestImport,
			`trunc_e2e_test.go clears change_log: "` + trunc + `"`,
		}, violations)
	})

	t.Run("the scanner names the file it cannot parse", func(t *testing.T) {
		broken := filepath.Join(t.TempDir(), "broken_test.go")
		require.NoError(t, os.WriteFile(broken, []byte("package"), 0o600))
		_, err := standInViolations([]string{broken})
		require.ErrorContains(t, err, "parse "+broken)
	})
}

// standInViolations answers, sorted, each violation of the two rules of
// TestE2ETestsReachV1StandInsOnlyThroughHelpers among files, which are named
// by their base name.
func standInViolations(files []string) ([]string, error) {
	var violations []string
	fset := token.NewFileSet()
	for _, name := range files {
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if err != nil {
			return nil, fmt.Errorf("parse %s for V1 stand-ins: %w", name, err)
		}
		base := filepath.Base(name)
		violations = append(violations, manifestImportViolations(base, file)...)
		if !changeLogHelpers[base] {
			violations = append(violations, changeLogClearViolations(base, file)...)
		}
	}
	sort.Strings(violations)
	return violations, nil
}

// manifestImportViolations applies the import rule to one file.
func manifestImportViolations(base string, file *ast.File) []string {
	if !strings.HasSuffix(base, "_test.go") {
		return nil
	}
	imports := false
	for _, spec := range file.Imports {
		if p, err := strconv.Unquote(spec.Path.Value); err == nil && p == manifestImport {
			imports = true
		}
	}
	switch {
	case imports && !manifestWrapperTests[base]:
		return []string{base + " imports " + manifestImport}
	case !imports && manifestWrapperTests[base]:
		return []string{base + " is exempt from the manifest import rule but no longer imports " + manifestImport}
	}
	return nil
}

// changeLogClearViolations applies the change_log rule to one file.
func changeLogClearViolations(base string, file *ast.File) []string {
	var violations []string
	ast.Inspect(file, func(n ast.Node) bool {
		lit, ok := n.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}
		value, err := strconv.Unquote(lit.Value)
		if err == nil && changeLogClear.MatchString(value) {
			violations = append(violations, fmt.Sprintf("%s clears change_log: %q", base, value))
		}
		return true
	})
	return violations
}
