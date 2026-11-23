package publication

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The retired Protocol V1 surface, by exact name. Forma supports Protocol V2
// only, so none of these may come back: not in production Go, and not in a
// public doc, where any mention would describe an interface that does not
// exist. The lists are the guard's whole knowledge; nothing here interprets
// prose.
const v1ManifestPkgImport = "github.com/lychee-technology/forma/internal/manifest"

// federatedHarnessOracleQuery is the federated e2e harness's independent
// DuckDB oracle. It scans its own reference copy of the tiers with globs; no
// product code reads that copy, so its glob literals are not a V1 read path.
const federatedHarnessOracleQuery = "internal/e2e_harness/federated/query.go"

// forbiddenV1Identifiers are retired V1 symbols.
var forbiddenV1Identifiers = map[string]bool{
	"QuerySource":                  true,
	"FlushVisibilityGraceMs":       true,
	"HasParquetHistory":            true,
	"confirmNoParquetPublished":    true,
	"S3ParquetPathTemplate":        true,
	"AllowCallerParquetPaths":      true,
	"GlobOnly":                     true,
	"ManifestConfig":               true,
	"ReplaceDelta":                 true,
	"ValidateManifestPathTemplate": true,
	"ErrManifestSchemaMismatch":    true,
	"ManifestSchemaMismatchError":  true,
	"ErrManifestUnstamped":         true,
	"ManifestUnstampedError":       true,
	"InitializeAwaitingInit":       true,
	"CopyTmpToFinal":               true,
}

// forbiddenV1Literals are retired V1 flag names, environment variables and
// the level-3 glob, matched as substrings of a production string literal.
var forbiddenV1Literals = []string{
	"glob-only", "manifest-template", "manifest-prefix", "replace-delta", "delta-prefix",
	"verify-stamps", "allow-empty-manifest-schema", "etag-retries", "gc-grace",
	"base-backoff-ms", "max-backoff-ms",
	"DUCKDB_MANIFEST_TEMPLATE", "MANIFEST_TEMPLATE", "DUCKDB_MANIFEST_PREFIX", "MANIFEST_PREFIX",
	"DUCKDB_S3_PREFIX", "FLUSH_VISIBILITY_GRACE_MS", "DUCKDB_ALLOW_CALLER_PARQUET_PATHS",
	"/*.parquet",
}

// forbiddenV1FlagNames are retired flags whose bare name is too common to
// match as a substring; a production literal equal to one is a violation.
var forbiddenV1FlagNames = map[string]bool{"repair": true}

// forbiddenDocTokens are what a public doc may not name. A leading-dash name
// is a flag, matched in its -flag and --flag spellings and only as a whole
// flag, so "auto-repair" is not --repair. internal/manifest matches only the
// retired package, not internal/manifestv2.
var forbiddenDocTokens = docTokens(
	"-glob-only", "-manifest-template", "-manifest-prefix", "-replace-delta", "-delta-prefix",
	"-repair", "-verify-stamps", "-allow-empty-manifest-schema", "-etag-retries", "-gc-grace",
	"-base-backoff-ms", "-max-backoff-ms",
	"QuerySource.Fallback", "DUCKDB_MANIFEST_TEMPLATE", "DUCKDB_MANIFEST_PREFIX", "DUCKDB_S3_PREFIX",
	"FLUSH_VISIBILITY_GRACE_MS", "DUCKDB_ALLOW_CALLER_PARQUET_PATHS",
	"ErrManifestSchemaMismatch", "ErrManifestUnstamped", "ValidateManifestPathTemplate",
	"InitializeAwaitingInit", "internal/manifest", "/*.parquet",
)

// requiredPublicDocs must be found by publicDocs, so a walker that silently
// skips a tree fails instead of passing vacuously.
var requiredPublicDocs = []string{
	"README.md", "docs/index.md", "docs/publication-protocol-v2.md", "docs/publication-protocol-v2-evidence.md",
	"docs/federated-query/design.md", "docs/error-handling.md", "docs/manifest-reconcile.md",
	"docs/aurora-dsql.md", "docs/dsql-ministack.md", "docs/schema-evolution.md",
	"cmd/tools/README.md", "tests/e2e/README.md",
}

// nonPublicDocTrees hold markdown that is not product or operator
// documentation: agent research scaffolding and recorded benchmark output.
var nonPublicDocTrees = map[string]bool{"tools/autoresearch": true}

type docToken struct {
	name string
	re   *regexp.Regexp
}

func docTokens(names ...string) []docToken {
	tokens := make([]docToken, 0, len(names))
	for _, name := range names {
		pattern := regexp.QuoteMeta(name)
		switch {
		case strings.HasPrefix(name, "-"):
			pattern = `(?:^|[^A-Za-z0-9_-])--?` + regexp.QuoteMeta(name[1:]) + `(?:[^A-Za-z0-9_-]|$)`
		case name == "internal/manifest":
			pattern += `(?:[^A-Za-z0-9_]|$)`
		}
		tokens = append(tokens, docToken{name: name, re: regexp.MustCompile(pattern)})
	}
	return tokens
}

// TestNoLegacyV1PathsOrObsoleteFlagsRemain proves that no production Go file
// imports the retired V1 manifest package or names a retired V1 symbol, flag,
// environment variable or level-3 glob, and that no public doc names one
// (#892).
func TestNoLegacyV1PathsOrObsoleteFlagsRemain(t *testing.T) {
	root := filepath.Join("..", "..")
	_, err := os.Stat(filepath.Join(root, "internal", "manifest"))
	require.ErrorIs(t, err, fs.ErrNotExist, "the retired V1 manifest package must stay deleted")

	files, err := productionFiles(root)
	require.NoError(t, err)
	require.Greater(t, len(files), 200)
	codeViolations, err := legacyV1CodeViolations(root, files)
	require.NoError(t, err)
	require.Empty(t, codeViolations)

	docs, err := publicDocs(root)
	require.NoError(t, err)
	require.Subset(t, docs, requiredPublicDocs)
	docViolations, err := legacyV1DocViolations(root, docs)
	require.NoError(t, err)
	require.Empty(t, docViolations)
}

func legacyV1CodeViolations(root string, files []string) ([]string, error) {
	var violations []string
	fset := token.NewFileSet()
	for _, name := range files {
		rel, err := filepath.Rel(root, name)
		if err != nil {
			return nil, fmt.Errorf("relativize %s against %s: %w", name, root, err)
		}
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if err != nil {
			return nil, fmt.Errorf("parse %s for retired V1 symbols: %w", name, err)
		}
		violations = append(violations, legacyV1FileViolations(filepath.ToSlash(rel), file)...)
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
		case *ast.BasicLit:
			val, err := strconv.Unquote(node.Value)
			if node.Kind != token.STRING || err != nil {
				return true
			}
			if forbiddenV1FlagNames[val] {
				add(fmt.Sprintf("%s contains retired V1 flag name %q", rel, val))
			}
			for _, lit := range forbiddenV1Literals {
				if strings.Contains(val, lit) && (lit != "/*.parquet" || rel != federatedHarnessOracleQuery) {
					add(fmt.Sprintf("%s contains retired V1 literal %q", rel, lit))
				}
			}
		}
		return true
	})
	return violations
}

// publicDocs lists, sorted and slash-separated, every markdown file under
// root outside hidden, build, dependency and nonPublicDocTrees directories.
func publicDocs(root string) ([]string, error) {
	var docs []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("walk %s: %w", p, err)
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return fmt.Errorf("relativize %s against %s: %w", p, root, err)
		}
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			name := d.Name()
			skip := strings.HasPrefix(name, ".") || name == "build" || name == "node_modules" || name == "vendor" || nonPublicDocTrees[rel]
			if p != root && skip {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(rel, ".md") {
			docs = append(docs, rel)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list the public docs under %s: %w", root, err)
	}
	sort.Strings(docs)
	return docs, nil
}

// legacyV1DocViolations answers, sorted, every line of relDocs that names a
// forbidden token, in prose or in a code block alike.
func legacyV1DocViolations(root string, relDocs []string) ([]string, error) {
	var violations []string
	for _, rel := range relDocs {
		raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			return nil, fmt.Errorf("read public doc %s: %w", rel, err)
		}
		for i, line := range strings.Split(string(raw), "\n") {
			for _, tok := range forbiddenDocTokens {
				if tok.re.MatchString(line) {
					violations = append(violations, fmt.Sprintf("%s:%d names retired V1 token %q", rel, i+1, tok.name))
				}
			}
		}
	}
	sort.Strings(violations)
	return violations, nil
}

// TestLegacyV1GuardCatchesPlantedRegressions proves the guard effective:
// each planted regression fails it, including a recommendation of a retired
// flag next to a removal word, and nothing that merely resembles a retired
// name does.
func TestLegacyV1GuardCatchesPlantedRegressions(t *testing.T) {
	dir := t.TempDir()
	plant := func(files map[string]string) {
		for rel, src := range files {
			p := filepath.Join(dir, filepath.FromSlash(rel))
			require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
			require.NoError(t, os.WriteFile(p, []byte(src), 0o600))
		}
	}
	plant(map[string]string{
		"internal/federated/regressed.go": "package federated\n\nimport _ \"" + v1ManifestPkgImport + "\"\n\n" +
			"type QuerySource struct { Fallback bool }\nconst glob = \"s3://b/data/1/*.parquet\"\n",
		"cmd/tools/regressed.go": "package main\n\nimport forma \"github.com/lychee-technology/forma\"\n\n" +
			"var _ = forma.ErrManifestSchemaMismatch\nconst flagName = \"glob-only\"\nvar repair = fs.Bool(\"repair\", false, \"\")\n",
		"internal/federated/benign.go":            "package federated\n\nconst note = \"read-time auto-repair is not a flag\"\n",
		"internal/e2e_harness/federated/query.go": "package federated\n\nconst oracle = \"s3://b/1/base/*.parquet\"\n",
	})
	files, err := productionFiles(dir)
	require.NoError(t, err)
	gotCode, err := legacyV1CodeViolations(dir, files)
	require.NoError(t, err)
	require.Equal(t, []string{
		`cmd/tools/regressed.go contains retired V1 flag name "repair"`,
		`cmd/tools/regressed.go contains retired V1 literal "glob-only"`,
		`cmd/tools/regressed.go references retired V1 identifier ErrManifestSchemaMismatch`,
		`internal/federated/regressed.go contains retired V1 literal "/*.parquet"`,
		`internal/federated/regressed.go imports ` + v1ManifestPkgImport,
		`internal/federated/regressed.go references retired V1 identifier QuerySource`,
	}, gotCode)

	plant(map[string]string{
		"README.md":           "Run cdc-flush --glob-only, because the old cache was removed.\n",
		"cmd/tools/README.md": "```bash\nforma-tools manifest-reconcile --repair\n```\n",
		"docs/ops.md": "Historical note: `DUCKDB_S3_PREFIX` was removed.\n" +
			"There is no read-time auto-repair, and `--gc` collects garbage.\n" +
			"See internal/manifestv2 and `-s3-prefix`.\n",
		"tools/autoresearch/run/report.md":   "read_parquet('s3://b/1/base/*.parquet')\n",
		".autoforge/notes.md":                "Run `cdc-flush --glob-only`.\n",
		"tests/e2e/node_modules/x/README.md": "Run `cdc-flush --glob-only`.\n",
	})
	docs, err := publicDocs(dir)
	require.NoError(t, err)
	require.Equal(t, []string{"README.md", "cmd/tools/README.md", "docs/ops.md"}, docs)
	gotDocs, err := legacyV1DocViolations(dir, docs)
	require.NoError(t, err)
	require.Equal(t, []string{
		`README.md:1 names retired V1 token "-glob-only"`,
		`cmd/tools/README.md:2 names retired V1 token "-repair"`,
		`docs/ops.md:1 names retired V1 token "DUCKDB_S3_PREFIX"`,
	}, gotDocs)
}
