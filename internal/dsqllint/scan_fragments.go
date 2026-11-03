package dsqllint

import (
	"go/ast"
	"go/token"
	"sort"
	"strconv"
	"strings"
)

// fragmentWindow is how many consecutive fragments the scan joins when it
// looks for a statement start: enough for the longest statement head
// sqlStatement matches when every keyword is its own fragment, such as
// CREATE OR REPLACE TRUSTED PROCEDURAL LANGUAGE, or UPDATE <table> AS
// <alias> SET.
const fragmentWindow = 8

// fragment is one piece of a symbol: a string literal, a reference to a
// same-package string constant, or a dynamic piece, an identifier whose
// value the scan cannot know (a variable, a parameter, a call), such as
// the table name in strings.Join([]string{"UPDATE ", table, " SET a = 1"}, "").
type fragment struct {
	pos     token.Pos
	text    string
	dynamic bool
}

// dynamicText stands in for a dynamic piece when the scan reads a window
// with its dynamic pieces kept: one word, which fills the slot a statement
// head leaves for a name (UPDATE <table> SET, WITH <name> AS, GRANT <priv>
// ON, SAVEPOINT <name>).
const dynamicText = "_"

// fragments collects a symbol's string pieces in source order. The scan
// has no data flow, so it cannot know which pieces end up in one
// statement; it assumes neighbours might, which finds a statement however
// its pieces are put together, at the price of an occasional symbol whose
// unrelated neighbouring strings only read like SQL (classify it not-dsql).
type fragments struct {
	list []fragment
	// skip holds identifiers that name something rather than refer to a
	// constant: a declared name, a composite literal's key, or the
	// selected name of a selector.
	skip map[*ast.Ident]bool
	// tags holds struct tags, which are metadata, never part of a statement.
	tags map[*ast.BasicLit]bool
}

func (f *fragments) collect(n ast.Node, consts map[string]string) {
	if f.skip == nil {
		f.skip, f.tags = map[*ast.Ident]bool{}, map[*ast.BasicLit]bool{}
	}
	switch x := n.(type) {
	case *ast.BasicLit:
		if x.Kind != token.STRING || f.tags[x] {
			return
		}
		if s, err := strconv.Unquote(x.Value); err == nil {
			f.list = append(f.list, fragment{pos: x.Pos(), text: s})
		}
	case *ast.Ident:
		if f.skip[x] {
			return
		}
		if s, ok := consts[x.Name]; ok {
			f.list = append(f.list, fragment{pos: x.Pos(), text: s})
		} else {
			f.list = append(f.list, fragment{pos: x.Pos(), dynamic: true})
		}
	case *ast.SelectorExpr:
		f.skip[x.Sel] = true
	case *ast.ValueSpec:
		for _, name := range x.Names {
			f.skip[name] = true
		}
	case *ast.KeyValueExpr:
		if key, ok := x.Key.(*ast.Ident); ok {
			f.skip[key] = true
		}
	case *ast.Field:
		if x.Tag != nil {
			f.tags[x.Tag] = true
		}
	}
}

// statementStart answers the position of the first fragment at which two
// or more fragments start a SQL statement (startsStatement). A window
// starts at a string fragment, never at a dynamic piece, and not at one
// that is a whole statement on its own: a literal one is a site already,
// and a constant one is the constant's site, not its user's. A fragment
// holding an error-wrapping verb is error text: no window starts at or
// crosses it. Only string fragments count against a window's size, but a
// dynamic piece is one of its fragments, so "SAVEPOINT " + name is a
// window of two.
func (f *fragments) statementStart() (token.Pos, bool) {
	list := f.ordered()
	for i := range list {
		if list[i].dynamic || LooksLikeSQL(list[i].text) {
			continue
		}
		var parts []fragment
		strs := 0
		for j := i; j < len(list) && strs < fragmentWindow; j++ {
			if strings.Contains(list[j].text, "%w") {
				break
			}
			parts = append(parts, list[j])
			if !list[j].dynamic {
				strs++
			}
			if len(parts) > 1 && startsStatement(parts) {
				return list[i].pos, true
			}
		}
	}
	return token.NoPos, false
}

// ordered answers the fragments in source order, with each run of
// adjacent dynamic pieces collapsed into one: pgx.Identifier{schema,
// table}.Sanitize() is one name in the statement, not three.
func (f *fragments) ordered() []fragment {
	list := f.list
	sort.SliceStable(list, func(i, j int) bool { return list[i].pos < list[j].pos })
	out := list[:0:0]
	for _, frag := range list {
		if frag.dynamic && len(out) > 0 && out[len(out)-1].dynamic {
			continue
		}
		out = append(out, frag)
	}
	return out
}

// startsStatement reports whether parts start a SQL statement, joined by a
// space or by nothing, with their dynamic pieces read as a name or left
// out. A space finds keywords kept in separate fragments ("CREATE",
// "TABLE t"), nothing finds a keyword split inside a word ("CREA",
// "TE TABLE t"); a dynamic piece read as a name finds a statement whose
// head holds a name only known at run time ("UPDATE ", table, " SET a =
// 1"), and one left out finds keywords a dynamic piece merely sits between.
func startsStatement(parts []fragment) bool {
	kept, dropped := make([]string, 0, len(parts)), make([]string, 0, len(parts))
	for _, p := range parts {
		if p.dynamic {
			kept = append(kept, dynamicText)
			continue
		}
		kept, dropped = append(kept, p.text), append(dropped, p.text)
	}
	for _, texts := range [][]string{dropped, kept} {
		if LooksLikeSQL(strings.Join(texts, " ")) || LooksLikeSQL(strings.Join(texts, "")) {
			return true
		}
	}
	return false
}
