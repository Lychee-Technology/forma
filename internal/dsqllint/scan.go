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
	"slices"
	"sort"
	"strings"
)

// Snapshot is what Scan found in the repository.
type Snapshot struct {
	// Sites maps each location that holds SQL text, calls a database
	// method or sets transaction options to the first such line.
	Sites map[string]Site
	// Symbols holds every top-level symbol location of the scanned Go files.
	Symbols map[string]bool
	// SQLFiles holds every checked-in .sql file outside testdata.
	SQLFiles map[string]bool
}

// Site is where and why the scan found a location.
type Site struct {
	Line int
	Why  string
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
	snap := Snapshot{Sites: map[string]Site{}, Symbols: map[string]bool{}, SQLFiles: map[string]bool{}}
	byDir := map[string][]string{}
	var dirs []string
	for _, rel := range files {
		if strings.HasSuffix(rel, ".sql") {
			snap.SQLFiles[rel] = true
			continue
		}
		dir := filepath.Dir(rel)
		if _, ok := byDir[dir]; !ok {
			dirs = append(dirs, dir)
		}
		byDir[dir] = append(byDir[dir], rel)
	}
	for _, dir := range dirs {
		if err := scanPackage(root, byDir[dir], snap); err != nil {
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

// scanPackage records the top-level symbols of one directory's Go files
// and the ones that are sites. The directory's files are read together so
// a statement split across its string constants is still recognised; a
// build-tagged file is parsed like any other.
func scanPackage(root string, rels []string, snap Snapshot) error {
	fset := token.NewFileSet()
	files := make([]*ast.File, len(rels))
	for i, rel := range rels {
		src, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			return fmt.Errorf("read %s: %w", rel, err)
		}
		if files[i], err = parser.ParseFile(fset, rel, src, parser.SkipObjectResolution); err != nil {
			return fmt.Errorf("parse %s: %w", rel, err)
		}
	}
	consts := packageConsts(files)
	for i, file := range files {
		scope := fileScope{fset: fset, imports: importNames(file), consts: consts}
		for _, decl := range file.Decls {
			for _, sym := range declSymbols(decl) {
				loc := rels[i] + ":" + sym.name
				snap.Symbols[loc] = true
				site := scope.siteOf(sym.node)
				if prev, ok := snap.Sites[loc]; site.Line > 0 && (!ok || site.Line < prev.Line) {
					snap.Sites[loc] = site
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
