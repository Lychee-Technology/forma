package dsqllint

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
)

// sqlLiteral matches a string literal that starts a SQL statement. It is a
// cheap heuristic over upper-case keywords, which is how this repository
// spells SQL: a statement assembled entirely from fragments that do not
// start with a keyword evades it, so review still matters.
var sqlLiteral = regexp.MustCompile(`^\s*(?:` +
	`CREATE\s+(?:UNIQUE\s+)?(?:TABLE|INDEX|SEQUENCE|VIEW|SCHEMA|EXTENSION|TYPE|FUNCTION|TRIGGER)\b|` +
	`ALTER\s+(?:TABLE|INDEX|SEQUENCE)\b|` +
	`DROP\s+(?:TABLE|INDEX|SEQUENCE|VIEW|SCHEMA)\b|` +
	`INSERT\s+INTO\b|` +
	`UPDATE\s+\S+\s+(?:(?:AS\s+)?\w+\s+)?SET\b|` +
	`DELETE\s+FROM\b|` +
	`SELECT\s|` +
	`WITH\s+(?:RECURSIVE\s+)?\w+\s+AS\b|` +
	`BEGIN\b|START\s+TRANSACTION\b|SET\s+(?:TRANSACTION|SESSION|LOCAL)\b|` +
	`TRUNCATE\b|LOCK\s+TABLE\b|COPY\s|GRANT\s|REVOKE\s)`)

// Snapshot is what Scan found in the repository.
type Snapshot struct {
	// Sites maps each location holding a SQL-shaped literal to the line of
	// its first one.
	Sites map[string]int
	// Symbols holds every top-level symbol location of the scanned Go files.
	Symbols map[string]bool
	// SQLFiles holds every checked-in .sql file outside testdata.
	SQLFiles map[string]bool
}

func (s Snapshot) sortedSites() []string {
	sites := make([]string, 0, len(s.Sites))
	for site := range s.Sites {
		sites = append(sites, site)
	}
	sort.Strings(sites)
	return sites
}

func (s Snapshot) hasSiteIn(file string) bool {
	for site := range s.Sites {
		if fileOf(site) == file {
			return true
		}
	}
	return false
}

// Scan reads every non-test Go file and every .sql file of the repository
// at root, outside testdata directories.
func Scan(root string) (Snapshot, error) {
	files, err := listFiles(root)
	if err != nil {
		return Snapshot{}, err
	}
	snap := Snapshot{Sites: map[string]int{}, Symbols: map[string]bool{}, SQLFiles: map[string]bool{}}
	for _, rel := range files {
		if strings.HasSuffix(rel, ".sql") {
			snap.SQLFiles[rel] = true
			continue
		}
		src, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			return Snapshot{}, fmt.Errorf("read %s: %w", rel, err)
		}
		if err := scanGoFile(rel, src, snap); err != nil {
			return Snapshot{}, err
		}
	}
	if len(snap.Symbols) == 0 {
		return Snapshot{}, fmt.Errorf("scan of %s found no Go symbols: wrong repository root?", root)
	}
	return snap, nil
}

// listFiles answers the repository's Go and SQL files: from git when root
// is a checkout (tracked files plus untracked ones git would not ignore, as
// make fmt-check lists them), otherwise from a walk of the tree.
func listFiles(root string) ([]string, error) {
	out, err := exec.Command("git", "-C", root, "ls-files", "--cached", "--others", "--exclude-standard", "--", "*.go", "*.sql").Output()
	var files []string
	if err == nil {
		files = strings.Split(strings.TrimSpace(string(out)), "\n")
	} else if files, err = walkFiles(root); err != nil {
		return nil, err
	}
	kept := files[:0]
	for _, f := range files {
		if wanted(f) {
			if _, statErr := os.Stat(filepath.Join(root, f)); statErr == nil {
				kept = append(kept, f)
			}
		}
	}
	sort.Strings(kept)
	return kept, nil
}

func wanted(rel string) bool {
	if rel == "" || strings.HasSuffix(rel, "_test.go") {
		return false
	}
	if !strings.HasSuffix(rel, ".go") && !strings.HasSuffix(rel, ".sql") {
		return false
	}
	return !slices.Contains(strings.Split(filepath.ToSlash(rel), "/"), "testdata")
}

// skippedDirs are the generated or ignored trees a walk outside git skips.
var skippedDirs = map[string]bool{"build": true, "node_modules": true, "vendor": true, "local-only": true}

func walkFiles(root string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != root && (strings.HasPrefix(d.Name(), ".") || skippedDirs[d.Name()]) {
				return filepath.SkipDir
			}
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return fmt.Errorf("relative path of %s: %w", path, err)
		}
		files = append(files, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk %s: %w", root, err)
	}
	return files, nil
}

// scanGoFile records rel's top-level symbols and the ones holding a
// SQL-shaped string literal. A build-tagged file is parsed like any other.
func scanGoFile(rel string, src []byte, snap Snapshot) error {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, rel, src, parser.SkipObjectResolution)
	if err != nil {
		return fmt.Errorf("parse %s: %w", rel, err)
	}
	for _, decl := range file.Decls {
		for _, sym := range declSymbols(decl) {
			loc := rel + ":" + sym.name
			snap.Symbols[loc] = true
			if line := firstSQLLine(fset, sym.node); line > 0 {
				if prev, ok := snap.Sites[loc]; !ok || line < prev {
					snap.Sites[loc] = line
				}
			}
		}
	}
	return nil
}

type symbol struct {
	name string
	node ast.Node
}

// declSymbols names a declaration's symbols: a function, a method as
// Type.Method, or each spec of a const, var or type declaration.
func declSymbols(decl ast.Decl) []symbol {
	switch d := decl.(type) {
	case *ast.FuncDecl:
		return []symbol{{name: funcName(d), node: d}}
	case *ast.GenDecl:
		var syms []symbol
		for _, spec := range d.Specs {
			switch s := spec.(type) {
			case *ast.ValueSpec:
				for _, n := range s.Names {
					syms = append(syms, symbol{name: n.Name, node: s})
				}
			case *ast.TypeSpec:
				syms = append(syms, symbol{name: s.Name.Name, node: s})
			}
		}
		return syms
	}
	return nil
}

func funcName(d *ast.FuncDecl) string {
	if d.Recv == nil || len(d.Recv.List) == 0 {
		return d.Name.Name
	}
	t := d.Recv.List[0].Type
	if star, ok := t.(*ast.StarExpr); ok {
		t = star.X
	}
	switch x := t.(type) {
	case *ast.IndexExpr:
		t = x.X
	case *ast.IndexListExpr:
		t = x.X
	}
	if id, ok := t.(*ast.Ident); ok {
		return id.Name + "." + d.Name.Name
	}
	return d.Name.Name
}

func firstSQLLine(fset *token.FileSet, node ast.Node) int {
	line := 0
	ast.Inspect(node, func(n ast.Node) bool {
		lit, ok := n.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING || line > 0 {
			return line == 0
		}
		if s, err := strconv.Unquote(lit.Value); err == nil && sqlLiteral.MatchString(s) {
			line = fset.Position(lit.Pos()).Line
		}
		return true
	})
	return line
}
