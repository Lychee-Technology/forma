package sites

import (
	"go/ast"
	"go/types"
	"strings"

	"golang.org/x/tools/go/packages"
)

// callee resolves the function or method a call invokes, by type
// information; nil for a call through a function value.
func callee(info *types.Info, fun ast.Expr) *types.Func {
	switch f := ast.Unparen(fun).(type) {
	case *ast.SelectorExpr:
		if sel := info.Selections[f]; sel != nil {
			fn, _ := sel.Obj().(*types.Func)
			return fn
		}
		fn, _ := info.Uses[f.Sel].(*types.Func)
		return fn
	case *ast.Ident:
		fn, _ := info.Uses[f].(*types.Func)
		return fn
	case *ast.IndexExpr:
		return callee(info, f.X)
	case *ast.IndexListExpr:
		return callee(info, f.X)
	}
	return nil
}

// sinkOf answers whether f is a SQL entry point, and how it was resolved:
// a driver method or function, a project interface method dispatched to
// one, or a project wrapper.
func (a *analyzer) sinkOf(f *types.Func) (sinkSpec, string, bool) {
	if f == nil || f.Pkg() == nil {
		return sinkSpec{}, "", false
	}
	f = f.Origin()
	sig := f.Type().(*types.Signature)
	path := f.Pkg().Path()
	if sig.Recv() != nil {
		if s, ok := driverMethods[path][f.Name()]; ok && paramIsString(sig, s.SQLParam) {
			return s, "driver", true
		}
		if types.IsInterface(sig.Recv().Type()) && a.inModule(path) {
			if s, ok := a.interfaceSink(f); ok {
				return s, "interface", true
			}
		}
	} else if s, ok := driverFuncs[path][f.Name()]; ok {
		return s, "driver", true
	}
	if d, ok := a.derived[f]; ok {
		return sinkSpec{Family: "wrapper", SQLParam: d.param}, "wrapper", true
	}
	return sinkSpec{}, "", false
}

func (a *analyzer) inModule(path string) bool {
	return path == a.module || strings.HasPrefix(path, a.module+"/")
}

func paramIsString(sig *types.Signature, idx int) bool {
	if idx < 0 {
		return true
	}
	if idx >= sig.Params().Len() {
		return false
	}
	return isString(sig.Params().At(idx).Type())
}

// interfaceSink resolves a project interface method by its implementers
// among the loaded types and the driver handles: it is a sink when any
// implementer's method is one.
func (a *analyzer) interfaceSink(f *types.Func) (sinkSpec, bool) {
	if s, ok := a.ifaceSinks[f]; ok {
		return s, true
	}
	if a.ifaceNone[f] {
		return sinkSpec{}, false
	}
	iface, ok := f.Type().(*types.Signature).Recv().Type().Underlying().(*types.Interface)
	if !ok {
		return sinkSpec{}, false
	}
	a.ifaceNone[f] = true // guards recursion through interfaces of interfaces
	for _, c := range a.candidates {
		if !types.Implements(c, iface) {
			continue
		}
		obj, _, _ := types.LookupFieldOrMethod(c, true, f.Pkg(), f.Name())
		m, ok := obj.(*types.Func)
		if !ok || m == f {
			continue
		}
		if s, _, ok := a.sinkOf(m); ok {
			delete(a.ifaceNone, f)
			a.ifaceSinks[f] = s
			return s, true
		}
	}
	return sinkSpec{}, false
}

// hasImplementer reports whether any candidate implements f's interface.
func (a *analyzer) hasImplementer(f *types.Func) bool {
	iface, ok := f.Type().(*types.Signature).Recv().Type().Underlying().(*types.Interface)
	if !ok {
		return false
	}
	for _, c := range a.candidates {
		if types.Implements(c, iface) {
			return true
		}
	}
	return false
}

// findWrappers promotes b's function to a wrapper when a sink call in it
// takes its SQL from one of b's string parameters (verbatim, or in taint
// mode through local data flow).
func (a *analyzer) findWrappers(p *packages.Package, b body) {
	if b.self == nil {
		return
	}
	self := b.self.Origin()
	if _, done := a.derived[self]; done {
		return
	}
	params := stringParams(p.TypesInfo, b.params)
	if len(params) == 0 {
		return
	}
	tainted := map[types.Object]bool{}
	if a.mode == Taint {
		tainted = taint(p.TypesInfo, b.node, params)
	}
	ast.Inspect(b.node, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		s, _, ok := a.sinkOf(callee(p.TypesInfo, call.Fun))
		if !ok || s.SQLParam < 0 || s.SQLParam >= len(call.Args) {
			return true
		}
		arg := ast.Unparen(call.Args[s.SQLParam])
		if id, ok := arg.(*ast.Ident); ok {
			if idx, ok := params[p.TypesInfo.Uses[id]]; ok {
				a.derived[self] = derivedSink{param: idx, verbatim: true}
				return false
			}
		}
		if a.mode == Taint {
			if obj := mentions(p.TypesInfo, arg, tainted); obj != nil {
				idx := 0
				if i, ok := params[obj]; ok {
					idx = i
				}
				a.derived[self] = derivedSink{param: idx, verbatim: false}
				return false
			}
		}
		return true
	})
}

// taint answers the objects whose value depends on params through
// assignments, declarations and method calls on a receiver that take a
// tainted argument (strings.Builder writes).
func taint(info *types.Info, node ast.Node, params map[types.Object]int) map[types.Object]bool {
	t := map[types.Object]bool{}
	for o := range params {
		t[o] = true
	}
	for changed := true; changed; {
		changed = false
		mark := func(e ast.Expr) {
			if id, ok := ast.Unparen(e).(*ast.Ident); ok {
				obj := info.Defs[id]
				if obj == nil {
					obj = info.Uses[id]
				}
				if obj != nil && !t[obj] {
					t[obj] = true
					changed = true
				}
			}
		}
		ast.Inspect(node, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.AssignStmt:
				for _, r := range x.Rhs {
					if mentions(info, r, t) != nil {
						for _, l := range x.Lhs {
							mark(l)
						}
					}
				}
			case *ast.ValueSpec:
				for _, v := range x.Values {
					if mentions(info, v, t) != nil {
						for _, n := range x.Names {
							mark(n)
						}
					}
				}
			case *ast.CallExpr:
				if sel, ok := x.Fun.(*ast.SelectorExpr); ok {
					for _, arg := range x.Args {
						if mentions(info, arg, t) != nil {
							mark(sel.X)
						}
					}
				}
			}
			return true
		})
	}
	return t
}

// mentions answers a tainted object e refers to, or nil.
func mentions(info *types.Info, e ast.Expr, t map[types.Object]bool) types.Object {
	var hit types.Object
	ast.Inspect(e, func(n ast.Node) bool {
		if hit != nil {
			return false
		}
		if id, ok := n.(*ast.Ident); ok && t[info.Uses[id]] {
			hit = info.Uses[id]
		}
		return true
	})
	return hit
}
