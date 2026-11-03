package sites

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
)

// Site is one place shipped code hands SQL (or a driver-written statement)
// to a database entry point.
type Site struct {
	ID       string            `json:"id"`
	IDs      map[string]string `json:"ids"`
	Package  string            `json:"package"`
	File     string            `json:"file"`
	Line     int               `json:"line"`
	EndLine  int               `json:"end_line"`
	Column   int               `json:"column"`
	Function string            `json:"function"`
	Family   string            `json:"family"`
	Callee   string            `json:"callee"`
	Via      string            `json:"via"`
	SQLArg   string            `json:"sql_arg,omitempty"`
	ArgKind  string            `json:"arg_kind"`
	Options  string            `json:"options,omitempty"`
	Builders []string          `json:"builders,omitempty"`
	// Closure is Builders plus what they call or read, transitively, up to
	// closureDepth levels: the generators a label may name.
	Closure  []string `json:"builder_closure,omitempty"`
	Modifies bool     `json:"modifies_param_sql,omitempty"`
}

// Finding is a pattern the inventory cannot resolve; each one fails closed.
type Finding struct {
	Kind     string `json:"kind"`
	Pos      string `json:"pos"`
	Function string `json:"function"`
	Detail   string `json:"detail"`
}

// Wrapper is a project function promoted to a sink because it forwards a
// SQL parameter.
type Wrapper struct {
	Func     string `json:"func"`
	Param    int    `json:"param"`
	Verbatim bool   `json:"verbatim"`
}

// Result is one inventory.
type Result struct {
	Mode       Mode              `json:"mode"`
	Packages   int               `json:"packages"`
	Sites      []Site            `json:"sites"`
	Forwarders int               `json:"forwarding_sites_folded"`
	Wrappers   []Wrapper         `json:"wrappers"`
	Interfaces []string          `json:"interface_sinks"`
	Findings   []Finding         `json:"findings"`
	Timings    map[string]string `json:"timings"`
	PeakRSSKB  int               `json:"peak_rss_kb"`
}

type derivedSink struct {
	param    int
	verbatim bool
}

type analyzer struct {
	fset       *token.FileSet
	pkgs       []*packages.Package
	module     string
	dir        string
	mode       Mode
	derived    map[*types.Func]derivedSink
	ifaceSinks map[*types.Func]sinkSpec
	ifaceNone  map[*types.Func]bool
	candidates []types.Type
}

func newAnalyzer(fset *token.FileSet, pkgs []*packages.Package, module, dir string, mode Mode) *analyzer {
	a := &analyzer{fset: fset, pkgs: pkgs, module: module, dir: dir, mode: mode, derived: map[*types.Func]derivedSink{}}
	a.candidates = a.implementerCandidates()
	return a
}

// implementerCandidates are the types a project interface method can be
// dispatched to: every named non-interface type of the loaded packages (T
// and *T) and the driver handle types.
func (a *analyzer) implementerCandidates() []types.Type {
	var out []types.Type
	seen := map[*types.Package]bool{}
	var visit func(p *types.Package)
	visit = func(p *types.Package) {
		if p == nil || seen[p] {
			return
		}
		seen[p] = true
		if names, ok := driverTypes[p.Path()]; ok {
			for _, n := range names {
				if tn, ok := p.Scope().Lookup(n).(*types.TypeName); ok {
					out = append(out, tn.Type(), types.NewPointer(tn.Type()))
				}
			}
		}
		for _, imp := range p.Imports() {
			visit(imp)
		}
	}
	for _, p := range a.pkgs {
		visit(p.Types)
		for _, name := range p.Types.Scope().Names() {
			tn, ok := p.Types.Scope().Lookup(name).(*types.TypeName)
			if !ok || types.IsInterface(tn.Type()) {
				continue
			}
			if named, ok := tn.Type().(*types.Named); ok && named.TypeParams().Len() > 0 {
				continue
			}
			out = append(out, tn.Type(), types.NewPointer(tn.Type()))
		}
	}
	return out
}

func (a *analyzer) run() *Result {
	// Fixpoint: wrappers and interface sinks feed each other.
	for {
		before := len(a.derived)
		a.ifaceSinks, a.ifaceNone = map[*types.Func]sinkSpec{}, map[*types.Func]bool{}
		a.eachBody(func(p *packages.Package, b body) { a.findWrappers(p, b) })
		if len(a.derived) == before {
			break
		}
	}
	res := &Result{}
	a.eachBody(func(p *packages.Package, b body) { a.collect(p, b, res) })
	for f, d := range a.derived {
		res.Wrappers = append(res.Wrappers, Wrapper{Func: a.symbol(f), Param: d.param, Verbatim: d.verbatim})
	}
	for f, s := range a.ifaceSinks {
		res.Interfaces = append(res.Interfaces, fmt.Sprintf("%s -> %s", a.symbol(f), s.Family))
	}
	sort.Slice(res.Wrappers, func(i, j int) bool { return res.Wrappers[i].Func < res.Wrappers[j].Func })
	sort.Strings(res.Interfaces)
	a.closeBuilders(res)
	a.assignIDs(res)
	sort.Slice(res.Findings, func(i, j int) bool { return res.Findings[i].Pos < res.Findings[j].Pos })
	return res
}

// body is one analysable unit: a function declaration, or the initialiser
// of a package-level var (whose function literals can call sinks).
type body struct {
	name   string
	self   *types.Func
	params *ast.FieldList
	node   ast.Node
}

func (a *analyzer) eachBody(fn func(p *packages.Package, b body)) {
	for _, p := range a.pkgs {
		for _, f := range p.Syntax {
			for _, d := range f.Decls {
				switch d := d.(type) {
				case *ast.FuncDecl:
					if d.Body == nil {
						continue
					}
					self, _ := p.TypesInfo.Defs[d.Name].(*types.Func)
					fn(p, body{name: funcName(d), self: self, params: d.Type.Params, node: d.Body})
				case *ast.GenDecl:
					for _, s := range d.Specs {
						if vs, ok := s.(*ast.ValueSpec); ok && len(vs.Values) > 0 {
							fn(p, body{name: vs.Names[0].Name, node: vs})
						}
					}
				}
			}
		}
	}
}

func funcName(d *ast.FuncDecl) string {
	if d.Recv == nil || len(d.Recv.List) == 0 {
		return d.Name.Name
	}
	t := d.Recv.List[0].Type
	if s, ok := t.(*ast.StarExpr); ok {
		t = s.X
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

// stringParams maps each string-kinded parameter object to its index.
func stringParams(info *types.Info, fl *ast.FieldList) map[types.Object]int {
	out := map[types.Object]int{}
	if fl == nil {
		return out
	}
	i := 0
	for _, f := range fl.List {
		if len(f.Names) == 0 {
			i++
			continue
		}
		for _, n := range f.Names {
			if obj := info.Defs[n]; obj != nil && isString(obj.Type()) {
				out[obj] = i
			}
			i++
		}
	}
	return out
}

func isString(t types.Type) bool {
	b, ok := t.Underlying().(*types.Basic)
	return ok && b.Info()&types.IsString != 0
}

func (a *analyzer) rel(pos token.Pos) (string, int, int) {
	p := a.fset.Position(pos)
	r, err := filepath.Rel(a.dir, p.Filename)
	if err != nil {
		r = p.Filename
	}
	return filepath.ToSlash(r), p.Line, p.Column
}

func (a *analyzer) symbol(f *types.Func) string {
	file, _, _ := a.rel(f.Pos())
	name := f.Name()
	if sig, ok := f.Type().(*types.Signature); ok && sig.Recv() != nil {
		rt := sig.Recv().Type()
		if p, ok := rt.(*types.Pointer); ok {
			rt = p.Elem()
		}
		if n, ok := types.Unalias(rt).(*types.Named); ok {
			name = n.Obj().Name() + "." + name
		}
	}
	return file + ":" + name
}

func shortHash(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])[:8]
}

// assignIDs computes the three identity schemes under evaluation and picks
// the recommended one as ID.
func (a *analyzer) assignIDs(res *Result) {
	sort.Slice(res.Sites, func(i, j int) bool {
		if res.Sites[i].File != res.Sites[j].File {
			return res.Sites[i].File < res.Sites[j].File
		}
		if res.Sites[i].Line != res.Sites[j].Line {
			return res.Sites[i].Line < res.Sites[j].Line
		}
		return res.Sites[i].Column < res.Sites[j].Column
	})
	ord := map[string]int{}
	exprSeen := map[string]int{}
	for i := range res.Sites {
		s := &res.Sites[i]
		owner := strings.TrimPrefix(strings.TrimPrefix(s.Package, a.module), "/")
		if owner == "" {
			owner = "forma"
		}
		base := owner + "." + s.Function + ":" + s.Callee
		ord[base]++
		ek := base + ":" + shortHash(s.SQLArg+"|"+s.Options)
		exprSeen[ek]++
		expr := ek
		if exprSeen[ek] > 1 {
			expr = fmt.Sprintf("%s#%d", ek, exprSeen[ek])
		}
		s.IDs = map[string]string{
			"pos":     fmt.Sprintf("%s:%d:%d", s.File, s.Line, s.Column),
			"ordinal": fmt.Sprintf("%s#%d", base, ord[base]),
			"expr":    expr,
		}
		s.ID = s.IDs["expr"]
	}
}

const closureDepth = 4

// closeBuilders extends each site's builders with the module functions and
// package-level values they reference, transitively, to closureDepth.
func (a *analyzer) closeBuilders(res *Result) {
	refs := map[string]map[string]bool{}
	a.eachBody(func(p *packages.Package, b body) {
		var key string
		if b.self != nil {
			key = a.symbol(b.self.Origin())
		} else if vs, ok := b.node.(*ast.ValueSpec); ok {
			file, _, _ := a.rel(vs.Pos())
			key = file + ":" + b.name
		}
		set := map[string]bool{}
		ast.Inspect(b.node, func(n ast.Node) bool {
			id, ok := n.(*ast.Ident)
			if !ok {
				return true
			}
			obj := p.TypesInfo.Uses[id]
			if obj == nil || obj.Pkg() == nil || !a.inModule(obj.Pkg().Path()) {
				return true
			}
			switch o := obj.(type) {
			case *types.Func:
				set[a.symbol(o.Origin())] = true
			case *types.Var, *types.Const:
				if o.Parent() == o.Pkg().Scope() {
					file, _, _ := a.rel(o.Pos())
					set[file+":"+o.Name()] = true
				}
			}
			return true
		})
		refs[key] = set
	})
	for i := range res.Sites {
		seen := map[string]bool{}
		frontier := append([]string(nil), res.Sites[i].Builders...)
		for d := 0; d < closureDepth && len(frontier) > 0; d++ {
			var next []string
			for _, f := range frontier {
				if seen[f] {
					continue
				}
				seen[f] = true
				for r := range refs[f] {
					next = append(next, r)
				}
			}
			frontier = next
		}
		for _, f := range frontier {
			seen[f] = true
		}
		for k := range seen {
			res.Sites[i].Closure = append(res.Sites[i].Closure, k)
		}
		sort.Strings(res.Sites[i].Closure)
	}
}
