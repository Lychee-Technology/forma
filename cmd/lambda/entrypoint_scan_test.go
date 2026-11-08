package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path"
	"path/filepath"
	"strconv"
	"strings"
)

// The two symbols the entry-point guard looks for, named by import path so an
// aliased or dot import is resolved instead of missed.
const (
	formaModulePath = "github.com/lychee-technology/forma"
	factoryImport   = formaModulePath + "/factory"
	bootstrapImport = formaModulePath + "/internal/bootstrap"

	// managerBuilderPrefix is a prefix, not one exact wrapper: WithConfig and
	// WithConfigContext both build a manager, and so will the next wrapper.
	managerBuilderPrefix = "NewEntityManager"
	overlayFunc          = "EntityConfigFromEnv"
)

// entryPointScan is the verdict on one cmd/<name> tree.
type entryPointScan struct {
	// builds: some file anywhere in the tree refers to factory.NewEntityManager*,
	// reachable or not, called or only taken as a value.
	builds bool
	// overlays: bootstrap.EntityConfigFromEnv is called from a declaration
	// reachable from the tree's main function or package initialisation.
	overlays bool
}

// declKey names a package-level declaration: a function, a type, a variable or
// constant, or a method as "Type.method". Package init functions and variable
// specs, which can have no usable name, get a synthetic key that no identifier
// can spell.
type declKey struct{ pkg, name string }

type scannedDecl struct {
	refs    []declKey
	overlay bool
}

type scannedPackage struct {
	isMain  bool
	imports map[string]bool // import paths of packages in the same tree
	decls   map[string]*scannedDecl
	methods map[string][]string // receiver type name -> method decl names
	roots   []string            // init functions and variable specs
}

// treeInfo describes the scanned tree: its import path, and the name each of
// its packages declares, which an unaliased import of it binds. A helper's
// directory need not match its package name: wiring/ may declare package
// config.
type treeInfo struct {
	path  string
	names map[string]string // import path -> declared package name
}

// fileImports maps a file's local package names to import paths; dot holds
// the dot-imported paths.
type fileImports struct {
	byName map[string]string
	dot    []string
}

// scanEntryPointTree parses every non-test Go file in dir's subtree and decides
// whether the tree builds an EntityManager and whether the overlay is applied
// on a reachable path. importPath is dir's own import path; it lets a call into
// a helper package in the same tree be followed.
//
// Comments and string literals never count: the files are parsed without
// comments and only identifiers in code are read (#429).
func scanEntryPointTree(dir, importPath string) (entryPointScan, error) {
	pkgs, builds, err := parseEntryPointTree(dir, importPath)
	if err != nil {
		return entryPointScan{}, err
	}
	return entryPointScan{builds: builds, overlays: overlayReachable(pkgs)}, nil
}

// parseEntryPointTree reads the packages of dir's subtree, keyed by import
// path. It skips what the go tool skips on every platform: testdata, and
// directories and files whose names start with "." or "_", which are not even
// parsed. It ignores build constraints: lambda builds only for linux/arm64,
// and a guard that read only the host's files would skip it.
func parseEntryPointTree(dir, importPath string) (map[string]*scannedPackage, bool, error) {
	files := map[string][]*ast.File{}
	fset := token.NewFileSet()
	err := filepath.WalkDir(dir, func(p string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := entry.Name()
		ignored := strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_")
		if entry.IsDir() {
			if p != dir && (name == "testdata" || ignored) {
				return filepath.SkipDir
			}
			return nil
		}
		if ignored || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, p, nil, parser.SkipObjectResolution)
		if err != nil {
			return fmt.Errorf("failed to parse %s: %w", p, err)
		}
		rel, err := filepath.Rel(dir, filepath.Dir(p))
		if err != nil {
			return fmt.Errorf("failed to relate %s to %s: %w", p, dir, err)
		}
		pkgPath := path.Join(importPath, filepath.ToSlash(rel))
		files[pkgPath] = append(files[pkgPath], file)
		return nil
	})
	if err != nil {
		return nil, false, fmt.Errorf("failed to walk %s: %w", dir, err)
	}

	tree := treeInfo{path: importPath, names: map[string]string{}}
	for pkgPath, pkgFiles := range files {
		tree.names[pkgPath] = pkgFiles[0].Name.Name
	}
	pkgs := map[string]*scannedPackage{}
	builds := false
	for pkgPath, pkgFiles := range files {
		pkg, pkgBuilds := scanPackage(pkgPath, pkgFiles, tree)
		pkgs[pkgPath] = pkg
		builds = builds || pkgBuilds
	}
	return pkgs, builds, nil
}

// scanPackage indexes one package's declarations and what each refers to.
func scanPackage(pkgPath string, files []*ast.File, tree treeInfo) (*scannedPackage, bool) {
	pkg := &scannedPackage{
		imports: map[string]bool{},
		decls:   map[string]*scannedDecl{},
		methods: map[string][]string{},
	}
	// First pass: every top-level name, so the second pass can tell a reference
	// to one from a reference to anything else.
	type pending struct {
		key  string
		node ast.Node
		file fileImports
	}
	var work []pending
	synthetic := 0
	for _, file := range files {
		pkg.isMain = pkg.isMain || file.Name.Name == "main"
		imports := readImports(file, tree, pkg.imports)
		for _, decl := range file.Decls {
			for _, d := range topLevelDecls(decl, &synthetic) {
				pkg.decls[d.key] = &scannedDecl{}
				if d.recv != "" {
					pkg.methods[d.recv] = append(pkg.methods[d.recv], d.key)
				}
				if d.root {
					pkg.roots = append(pkg.roots, d.key)
				}
				work = append(work, pending{key: d.key, node: d.node, file: imports})
			}
		}
	}
	// Second pass: references and calls.
	builds := false
	for _, w := range work {
		declBuilds := scanDecl(pkgPath, pkg, pkg.decls[w.key], w.node, w.file)
		builds = builds || declBuilds
	}
	return pkg, builds
}

// readImports records the file's imports, adding those inside the tree to
// treeImports. An unaliased import of a tree package binds the name that
// package declares; any other binds the last path element, which holds for
// the forma packages the guard resolves.
func readImports(file *ast.File, tree treeInfo, treeImports map[string]bool) fileImports {
	imports := fileImports{byName: map[string]string{}}
	for _, spec := range file.Imports {
		importPath, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			continue // the parser accepted it, so this cannot happen
		}
		if importPath == tree.path || strings.HasPrefix(importPath, tree.path+"/") {
			treeImports[importPath] = true
		}
		name := path.Base(importPath)
		if declared, ok := tree.names[importPath]; ok {
			name = declared
		}
		if spec.Name != nil {
			name = spec.Name.Name
		}
		switch name {
		case "_":
		case ".":
			imports.dot = append(imports.dot, importPath)
		default:
			imports.byName[name] = importPath
		}
	}
	return imports
}

type topLevelDecl struct {
	key  string
	recv string // receiver type name, for a method
	root bool   // run at package initialisation whether referenced or not
	node ast.Node
}

// topLevelDecls splits one file-level declaration into the units reachability
// is tracked at. Every variable spec is a root: its initialiser runs at package
// initialisation whether or not anything refers to it, so `var _ = f()` keeps f
// alive.
func topLevelDecls(decl ast.Decl, synthetic *int) []topLevelDecl {
	switch d := decl.(type) {
	case *ast.FuncDecl:
		if d.Recv != nil && len(d.Recv.List) > 0 {
			recv := receiverTypeName(d.Recv.List[0].Type)
			return []topLevelDecl{{key: recv + "." + d.Name.Name, recv: recv, node: d}}
		}
		if d.Name.Name == "init" {
			*synthetic++
			return []topLevelDecl{{key: "init#" + strconv.Itoa(*synthetic), root: true, node: d}}
		}
		return []topLevelDecl{{key: d.Name.Name, node: d}}
	case *ast.GenDecl:
		var out []topLevelDecl
		for _, spec := range d.Specs {
			switch s := spec.(type) {
			case *ast.TypeSpec:
				out = append(out, topLevelDecl{key: s.Name.Name, node: s})
			case *ast.ValueSpec:
				// One unit per spec: `var a, b = f()` initialises both from one call.
				*synthetic++
				key := "var#" + strconv.Itoa(*synthetic)
				out = append(out, topLevelDecl{key: key, root: d.Tok == token.VAR, node: s})
				for _, name := range s.Names {
					if name.Name != "_" {
						out = append(out, topLevelDecl{key: name.Name, node: s})
					}
				}
			}
		}
		return out
	}
	return nil
}

// receiverTypeName strips the pointer and type parameters from a receiver.
func receiverTypeName(expr ast.Expr) string {
	for {
		switch e := expr.(type) {
		case *ast.StarExpr:
			expr = e.X
		case *ast.ParenExpr:
			expr = e.X
		case *ast.IndexExpr:
			expr = e.X
		case *ast.IndexListExpr:
			expr = e.X
		case *ast.Ident:
			return e.Name
		default:
			return ""
		}
	}
}

// scanDecl fills decl's references and overlay flag from node, and reports
// whether node refers to a manager builder.
//
// References over-approximate: any identifier spelling a top-level name of the
// package counts, shadowed or not, including the selector of a method call,
// and so does any identifier against every dot-imported tree package, whose
// exported names it may spell.
// Too many edges can only make more code look reachable; too few would fail a
// wired entry point, and the guard prefers the false pass on contrived code to
// the false failure on ordinary code.
//
// A builder is recognised by any reference, the overlay only by a call. Both
// err toward failing: `build := factory.NewEntityManagerWithConfig` still
// marks the tree as an entry point, and `_ = bootstrap.EntityConfigFromEnv`
// does not count as wiring it.
func scanDecl(pkgPath string, pkg *scannedPackage, decl *scannedDecl, node ast.Node, imports fileImports) bool {
	builds := false
	ast.Inspect(node, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.CallExpr:
			if refersTo(x.Fun, imports, bootstrapImport, func(name string) bool { return name == overlayFunc }) {
				decl.overlay = true
			}
		case *ast.SelectorExpr:
			id, ok := x.X.(*ast.Ident)
			if !ok {
				return true
			}
			importPath, ok := imports.byName[id.Name]
			if !ok {
				return true
			}
			if importPath == factoryImport && strings.HasPrefix(x.Sel.Name, managerBuilderPrefix) {
				builds = true
			}
			if pkg.imports[importPath] {
				decl.refs = append(decl.refs, declKey{importPath, x.Sel.Name})
			}
			return false
		case *ast.Ident:
			if contains(imports.dot, factoryImport) && strings.HasPrefix(x.Name, managerBuilderPrefix) {
				builds = true
			}
			if _, ok := pkg.decls[x.Name]; ok {
				decl.refs = append(decl.refs, declKey{pkgPath, x.Name})
			}
			for _, dotPath := range imports.dot {
				if pkg.imports[dotPath] {
					decl.refs = append(decl.refs, declKey{dotPath, x.Name})
				}
			}
		}
		return true
	})
	return builds
}

// refersTo reports whether expr names a function of importPath that match
// accepts, through the file's local name for that package or a dot import.
func refersTo(expr ast.Expr, imports fileImports, importPath string, match func(string) bool) bool {
	for {
		paren, ok := expr.(*ast.ParenExpr)
		if !ok {
			break
		}
		expr = paren.X
	}
	switch e := expr.(type) {
	case *ast.SelectorExpr:
		id, ok := e.X.(*ast.Ident)
		return ok && imports.byName[id.Name] == importPath && match(e.Sel.Name)
	case *ast.Ident:
		return contains(imports.dot, importPath) && match(e.Name)
	}
	return false
}

func contains(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}
	return false
}

// overlayReachable walks from every main function, and from the initialisation
// of every package a main package links, and reports whether a reached
// declaration calls the overlay. A package in the tree that no main package
// imports contributes nothing: its code is not in the binary.
func overlayReachable(pkgs map[string]*scannedPackage) bool {
	var queue []declKey
	for _, pkgPath := range linkedPackages(pkgs) {
		pkg := pkgs[pkgPath]
		if pkg.isMain {
			queue = append(queue, declKey{pkgPath, "main"})
		}
		for _, root := range pkg.roots {
			queue = append(queue, declKey{pkgPath, root})
		}
	}

	seen := map[declKey]bool{}
	for len(queue) > 0 {
		key := queue[0]
		queue = queue[1:]
		if seen[key] {
			continue
		}
		seen[key] = true
		pkg, ok := pkgs[key.pkg]
		if !ok {
			continue
		}
		decl, ok := pkg.decls[key.name]
		if !ok {
			continue
		}
		if decl.overlay {
			return true
		}
		queue = append(queue, decl.refs...)
		// A reached type brings its methods: a value of it may be handed to code
		// outside the tree that calls them through an interface.
		for _, method := range pkg.methods[key.name] {
			queue = append(queue, declKey{key.pkg, method})
		}
	}
	return false
}

// linkedPackages lists the main packages of the tree and every tree package
// they import, transitively.
func linkedPackages(pkgs map[string]*scannedPackage) []string {
	var queue, linked []string
	for pkgPath, pkg := range pkgs {
		if pkg.isMain {
			queue = append(queue, pkgPath)
		}
	}
	seen := map[string]bool{}
	for len(queue) > 0 {
		pkgPath := queue[0]
		queue = queue[1:]
		pkg, ok := pkgs[pkgPath]
		if seen[pkgPath] || !ok {
			continue
		}
		seen[pkgPath] = true
		linked = append(linked, pkgPath)
		for imported := range pkg.imports {
			queue = append(queue, imported)
		}
	}
	return linked
}
