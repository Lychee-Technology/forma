package sites

import (
	"fmt"
	"go/ast"
	"go/types"
	"sort"

	"golang.org/x/tools/go/packages"
)

// dbResultTypes are result types that mark a function-typed value as a
// database call when it is invoked indirectly.
var dbResultTypes = map[string]bool{
	"github.com/jackc/pgx/v5/pgconn.CommandTag": true, "github.com/jackc/pgx/v5.Rows": true,
	"github.com/jackc/pgx/v5.Row": true, "github.com/jackc/pgx/v5.Tx": true,
	"database/sql.Result": true, "*database/sql.Rows": true, "*database/sql.Row": true, "*database/sql.Tx": true,
}

// collect records b's sites and the patterns it cannot resolve.
func (a *analyzer) collect(p *packages.Package, b body, res *Result) {
	info := p.TypesInfo
	params := stringParams(info, b.params)
	var self *types.Func
	if b.self != nil {
		self = b.self.Origin()
	}
	_, isWrapper := a.derived[self]
	tainted := map[types.Object]bool{}
	if a.mode == Taint && isWrapper {
		tainted = taint(info, b.node, params)
	}
	callFuns := map[ast.Expr]bool{}
	selIdents := map[*ast.Ident]bool{}
	ast.Inspect(b.node, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.CallExpr:
			fun := ast.Unparen(x.Fun)
			if ix, ok := fun.(*ast.IndexExpr); ok {
				fun = ix.X
			}
			callFuns[fun] = true
		case *ast.SelectorExpr:
			selIdents[x.Sel] = true
		}
		return true
	})
	ast.Inspect(b.node, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.CallExpr:
			a.collectCall(p, b, x, params, tainted, isWrapper, res)
		case *ast.SelectorExpr:
			if !callFuns[x] {
				a.checkValue(p, b, x, x.Sel, res)
			}
		case *ast.Ident:
			if !callFuns[x] && !selIdents[x] {
				a.checkValue(p, b, x, x, res)
			}
		}
		return true
	})
}

func (a *analyzer) collectCall(p *packages.Package, b body, call *ast.CallExpr, params map[types.Object]int, tainted map[types.Object]bool, isWrapper bool, res *Result) {
	info := p.TypesInfo
	fn := callee(info, call.Fun)
	if fn == nil {
		a.checkIndirect(p, b, call, res)
		return
	}
	s, via, ok := a.sinkOf(fn)
	if !ok {
		a.checkUnresolvedInterface(p, b, call, fn, res)
		return
	}
	site := Site{Package: p.PkgPath, Function: b.name, Family: s.Family, Via: via, Callee: a.calleeName(fn, via)}
	site.File, site.Line, site.Column = a.rel(call.Pos())
	_, site.EndLine, _ = a.rel(call.End())
	site.ArgKind = "none"
	if s.SQLParam >= 0 && s.SQLParam < len(call.Args) {
		arg := ast.Unparen(call.Args[s.SQLParam])
		site.SQLArg = types.ExprString(arg)
		site.ArgKind = argKind(info, arg, params)
		if site.ArgKind == "forwarded-param" && isWrapper {
			res.Forwarders++
			return
		}
		if isWrapper && a.mode == Taint && mentions(info, arg, tainted) != nil {
			site.Modifies = true
		}
		site.Builders = a.builders(info, b, arg)
	}
	if s.SQLParam < 0 && len(call.Args) > 1 && (s.Family == "pgx-begin" || s.Family == "sql-begin") {
		site.Options = types.ExprString(call.Args[1])
	}
	res.Sites = append(res.Sites, site)
}

func (a *analyzer) calleeName(fn *types.Func, via string) string {
	fn = fn.Origin()
	if via == "wrapper" {
		return "wrap:" + a.symbol(fn)
	}
	name := fn.Name()
	if sig := fn.Type().(*types.Signature); sig.Recv() != nil {
		rt := sig.Recv().Type()
		if p, ok := rt.(*types.Pointer); ok {
			rt = p.Elem()
		}
		if n, ok := types.Unalias(rt).(*types.Named); ok {
			name = n.Obj().Name() + "." + name
		}
	}
	return fn.Pkg().Name() + "." + name
}

func argKind(info *types.Info, e ast.Expr, params map[types.Object]int) string {
	switch x := e.(type) {
	case *ast.BasicLit:
		return "literal"
	case *ast.Ident:
		obj := info.Uses[x]
		if _, ok := params[obj]; ok {
			return "forwarded-param"
		}
		if _, ok := obj.(*types.Const); ok {
			return "const"
		}
		return "local-or-var"
	case *ast.CallExpr:
		return "call"
	case *ast.BinaryExpr:
		return "concat"
	case *ast.SelectorExpr:
		return "selector"
	case *ast.IndexExpr:
		return "index"
	}
	return fmt.Sprintf("%T", e)
}

// builders answers the construction candidates of a site's SQL: functions
// called and package-level values read in the argument, and, for a local
// variable, in the right-hand sides that assign it (one level).
func (a *analyzer) builders(info *types.Info, b body, arg ast.Expr) []string {
	set := map[string]bool{}
	locals := map[types.Object]bool{}
	scanExpr := func(e ast.Expr) {
		ast.Inspect(e, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.CallExpr:
				if fn := callee(info, x.Fun); fn != nil && fn.Pkg() != nil && a.inModule(fn.Pkg().Path()) {
					set[a.symbol(fn.Origin())] = true
				}
			case *ast.Ident:
				obj := info.Uses[x]
				if obj == nil || obj.Pkg() == nil || !a.inModule(obj.Pkg().Path()) {
					return true
				}
				switch o := obj.(type) {
				case *types.Var:
					if o.Parent() == o.Pkg().Scope() {
						file, _, _ := a.rel(o.Pos())
						set[file+":"+o.Name()] = true
					} else if !o.IsField() {
						locals[o] = true
					}
				case *types.Const:
					if o.Parent() == o.Pkg().Scope() {
						file, _, _ := a.rel(o.Pos())
						set[file+":"+o.Name()] = true
					}
				}
			}
			return true
		})
	}
	scanExpr(arg)
	if len(locals) > 0 {
		ast.Inspect(b.node, func(n ast.Node) bool {
			if rs, ok := n.(*ast.RangeStmt); ok {
				for _, v := range []ast.Expr{rs.Key, rs.Value} {
					if id, ok := v.(*ast.Ident); ok && locals[info.Defs[id]] {
						scanExpr(rs.X)
					}
				}
			}
			if as, ok := n.(*ast.AssignStmt); ok {
				for i, l := range as.Lhs {
					id, ok := l.(*ast.Ident)
					if !ok {
						continue
					}
					obj := info.Defs[id]
					if obj == nil {
						obj = info.Uses[id]
					}
					if locals[obj] {
						if len(as.Rhs) == len(as.Lhs) {
							scanExpr(as.Rhs[i])
						} else if len(as.Rhs) == 1 {
							scanExpr(as.Rhs[0])
						}
					}
				}
			}
			return true
		})
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// checkValue flags a sink or wrapper used as a value (a method value, a
// method expression, a function value): its later calls are invisible.
func (a *analyzer) checkValue(p *packages.Package, b body, e ast.Expr, id *ast.Ident, res *Result) {
	fn, ok := p.TypesInfo.Uses[id].(*types.Func)
	if !ok {
		return
	}
	if _, _, ok := a.sinkOf(fn); !ok {
		return
	}
	file, line, col := a.rel(e.Pos())
	res.Findings = append(res.Findings, Finding{Kind: "sink-as-value", Pos: fmt.Sprintf("%s:%d:%d", file, line, col),
		Function: b.name, Detail: types.ExprString(e) + " is used as a value; calls through it cannot be attributed"})
}

// checkIndirect flags a call through a function value whose results are
// database results: it may send SQL the inventory cannot attribute.
func (a *analyzer) checkIndirect(p *packages.Package, b body, call *ast.CallExpr, res *Result) {
	t := p.TypesInfo.TypeOf(call.Fun)
	if t == nil {
		return
	}
	sig, ok := t.Underlying().(*types.Signature)
	if !ok {
		return
	}
	for i := 0; i < sig.Results().Len(); i++ {
		if dbResultTypes[sig.Results().At(i).Type().String()] {
			file, line, col := a.rel(call.Pos())
			res.Findings = append(res.Findings, Finding{Kind: "indirect-db-call", Pos: fmt.Sprintf("%s:%d:%d", file, line, col),
				Function: b.name, Detail: types.ExprString(call.Fun) + " returns " + sig.Results().At(i).Type().String()})
			return
		}
	}
}

// checkUnresolvedInterface flags a project interface method with a driver
// method's name and no implementer among the loaded types.
func (a *analyzer) checkUnresolvedInterface(p *packages.Package, b body, call *ast.CallExpr, fn *types.Func, res *Result) {
	sig := fn.Type().(*types.Signature)
	if sig.Recv() == nil || !types.IsInterface(sig.Recv().Type()) || fn.Pkg() == nil || !a.inModule(fn.Pkg().Path()) {
		return
	}
	if !sinkNames()[fn.Name()] || a.hasImplementer(fn) {
		return
	}
	file, line, col := a.rel(call.Pos())
	res.Findings = append(res.Findings, Finding{Kind: "unresolved-interface", Pos: fmt.Sprintf("%s:%d:%d", file, line, col),
		Function: b.name, Detail: a.symbol(fn) + " has a driver method's name but no implementer in the shipped packages"})
}
