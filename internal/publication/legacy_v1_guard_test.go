package publication

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
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
	v1ManifestPkgImport = "github.com/lychee-technology/forma/internal/manifest"
	v1ManifestPkgDir    = "internal/manifest"
	// federatedHarnessOracleQuery is the test/benchmark harness's independent
	// DuckDB oracle, which reads its own V1-shaped reference copy; it is not a
	// production read path.
	federatedHarnessOracleQuery = "internal/e2e_harness/federated/query.go"
)

// forbiddenV1Identifiers are retired V1 symbols that must not return to any
// production Go file outside internal/manifest.
var forbiddenV1Identifiers = map[string]bool{
	"QuerySource":               true,
	"FlushVisibilityGraceMs":    true,
	"HasParquetHistory":         true,
	"confirmNoParquetPublished": true,
	"S3ParquetPathTemplate":     true,
	"AllowCallerParquetPaths":   true,
	"GlobOnly":                  true,
	"ManifestConfig":            true,
	"ReplaceDelta":              true,
}

// forbiddenV1Literals are obsolete V1 flags, environment variables and level-3
// glob patterns that must not appear in production Go code outside
// internal/manifest.
var forbiddenV1Literals = []string{
	"glob-only",
	"manifest-template",
	"manifest-prefix",
	"replace-delta",
	"delta-prefix",
	"verify-stamps",
	"allow-empty-manifest-schema",
	"etag-retries",
	"gc-grace",
	"base-backoff-ms",
	"max-backoff-ms",
	"DUCKDB_MANIFEST_TEMPLATE",
	"MANIFEST_TEMPLATE",
	"DUCKDB_MANIFEST_PREFIX",
	"MANIFEST_PREFIX",
	"DUCKDB_S3_PREFIX",
	"FLUSH_VISIBILITY_GRACE_MS",
	"DUCKDB_ALLOW_CALLER_PARQUET_PATHS",
	"/*.parquet",
}

// publicProtocolDocs are the public and operator docs guarded against
// reintroducing obsolete V1 flags, env vars or level-3 glob examples in
// runnable snippets.
var publicProtocolDocs = []string{
	"README.md",
	"docs/index.md",
	"docs/publication-protocol-v2.md",
	"docs/federated-query/design.md",
	"docs/error-handling.md",
	"docs/manifest-reconcile.md",
	"docs/aurora-dsql.md",
	"docs/dsql-ministack.md",
	"cmd/tools/README.md",
}

// forbiddenDocCodeTokens are retired V1 flags, env vars, APIs and glob
// patterns that must never appear inside fenced code snippets of public docs.
// Single-dash forms match both Go `-flag` and POSIX `--flag` spellings.
var forbiddenDocCodeTokens = []string{
	"QuerySource.Fallback",
	"-glob-only",
	"-manifest-template",
	"-manifest-prefix",
	"-replace-delta",
	"-delta-prefix",
	"-repair",
	"-verify-stamps",
	"-allow-empty-manifest-schema",
	"-etag-retries",
	"-gc-grace",
	"DUCKDB_MANIFEST_TEMPLATE",
	"DUCKDB_MANIFEST_PREFIX",
	"DUCKDB_S3_PREFIX",
	"FLUSH_VISIBILITY_GRACE_MS",
	"/*.parquet",
}

// TestNoLegacyV1PathsOrObsoleteFlagsRemain proves that no production Go file
// outside internal/manifest imports the retired V1 manifest package, calls
// ValidateManifestPathTemplate, reintroduces QuerySource.Fallback / level-3
// glob / glob-only / obsolete V1 flags or env vars, and that no public doc
// code block teaches those retired V1 modes (#892).
func TestNoLegacyV1PathsOrObsoleteFlagsRemain(t *testing.T) {
	root := filepath.Join("..", "..")
	files, err := productionFiles(root)
	require.NoError(t, err)
	require.Greater(t, len(files), 200)

	codeViolations, err := legacyV1CodeViolations(root, files)
	require.NoError(t, err)
	require.Empty(t, codeViolations)

	docViolations, err := legacyV1DocViolations(root, publicProtocolDocs)
	require.NoError(t, err)
	require.Empty(t, docViolations)

	t.Run("catches planted V1 code and doc regressions", func(t *testing.T) {
		dir := t.TempDir()
		for rel, src := range map[string]string{
			"internal/federated/regressed.go": "package federated\n\nimport _ \"" + v1ManifestPkgImport + "\"\n\ntype QuerySource struct { Fallback bool }\nconst glob = \"s3://b/data/1/*.parquet\"\n",
			"cmd/tools/regressed.go":          "package main\n\nimport forma \"github.com/lychee-technology/forma\"\n\nvar _ = forma.ValidateManifestPathTemplate\nconst flagName = \"glob-only\"\n",
			"internal/manifest/allowed.go":    "package manifest\n\nconst glob = \"/*.parquet\"\n",
			"config_duckdb.go":                "package forma\n\nfunc ValidateManifestPathTemplate(field, tmpl string) error { return nil }\n",
		} {
			p := filepath.Join(dir, filepath.FromSlash(rel))
			require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
			require.NoError(t, os.WriteFile(p, []byte(src), 0o600))
		}
		plantedFiles, err := productionFiles(dir)
		require.NoError(t, err)
		gotCode, err := legacyV1CodeViolations(dir, plantedFiles)
		require.NoError(t, err)
		require.Equal(t, []string{
			`cmd/tools/regressed.go calls retired ValidateManifestPathTemplate`,
			`cmd/tools/regressed.go contains retired V1 literal "glob-only"`,
			`internal/federated/regressed.go contains retired V1 literal "/*.parquet"`,
			`internal/federated/regressed.go imports ` + v1ManifestPkgImport,
			`internal/federated/regressed.go references retired V1 identifier QuerySource`,
		}, gotCode)

		docPath := filepath.Join(dir, "README.md")
		docSrc := "Historical note: `--repair` was removed.\n\n```bash\nforma-tools cdc-flush -glob-only\nforma-tools manifest-reconcile --repair\n```\n"
		require.NoError(t, os.WriteFile(docPath, []byte(docSrc), 0o600))
		gotDocs, err := legacyV1DocViolations(dir, []string{"README.md"})
		require.NoError(t, err)
		require.Equal(t, []string{
			`README.md:4 code block uses retired V1 token "-glob-only"`,
			`README.md:5 code block uses retired V1 token "-repair"`,
		}, gotDocs)
	})
}

func legacyV1CodeViolations(root string, files []string) ([]string, error) {
	var violations []string
	fset := token.NewFileSet()
	for _, name := range files {
		rel, err := filepath.Rel(root, name)
		if err != nil {
			return nil, fmt.Errorf("relativize %s against %s: %w", name, root, err)
		}
		rel = filepath.ToSlash(rel)
		if path.Dir(rel) == v1ManifestPkgDir {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if err != nil {
			return nil, fmt.Errorf("parse %s for retired V1 symbols: %w", name, err)
		}
		violations = append(violations, legacyV1FileViolations(rel, file)...)
	}
	sort.Strings(violations)
	return violations, nil
}

func legacyV1FileViolations(rel string, file *ast.File) []string {
	var violations []string
	seen := map[string]bool{}
	add := func(msg string) {
		if !seen[msg] {
			seen[msg] = true
			violations = append(violations, msg)
		}
	}
	for _, imp := range file.Imports {
		if p, err := strconv.Unquote(imp.Path.Value); err == nil && p == v1ManifestPkgImport {
			add(rel + " imports " + v1ManifestPkgImport)
		}
	}
	ast.Inspect(file, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.Ident:
			if forbiddenV1Identifiers[node.Name] {
				add(rel + " references retired V1 identifier " + node.Name)
			}
			if node.Name == "ValidateManifestPathTemplate" && rel != "config_duckdb.go" {
				add(rel + " calls retired ValidateManifestPathTemplate")
			}
		case *ast.BasicLit:
			if node.Kind != token.STRING {
				return true
			}
			val, err := strconv.Unquote(node.Value)
			if err != nil {
				return true
			}
			for _, lit := range forbiddenV1Literals {
				if lit == "/*.parquet" && rel == federatedHarnessOracleQuery {
					continue
				}
				if strings.Contains(val, lit) {
					add(fmt.Sprintf("%s contains retired V1 literal %q", rel, lit))
				}
			}
		}
		return true
	})
	return violations
}

func legacyV1DocViolations(root string, relDocs []string) ([]string, error) {
	var violations []string
	for _, rel := range relDocs {
		raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			return nil, fmt.Errorf("read public doc %s: %w", rel, err)
		}
		inFence := false
		for idx, line := range strings.Split(string(raw), "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "```") {
				inFence = !inFence
				continue
			}
			if !inFence {
				continue
			}
			for _, tok := range forbiddenDocCodeTokens {
				if strings.Contains(line, tok) {
					violations = append(violations, fmt.Sprintf("%s:%d code block uses retired V1 token %q", rel, idx+1, tok))
				}
			}
		}
	}
	sort.Strings(violations)
	return violations, nil
}
