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
// CREATE OR REPLACE UNIQUE TEMP TABLE, or UPDATE <table> AS <alias> SET.
const fragmentWindow = 8

// fragment is one string piece of a symbol: a string literal, or a
// reference to a same-package string constant.
type fragment struct {
	pos  token.Pos
	text string
}

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
		if s, ok := consts[x.Name]; ok && !f.skip[x] {
			f.list = append(f.list, fragment{pos: x.Pos(), text: s})
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
// does not start at a fragment that is a whole statement on its own: a
// literal one is a site already, and a constant one is the constant's
// site, not its user's. A fragment holding an error-wrapping verb is error
// text: no window starts at or crosses it.
func (f *fragments) statementStart() (token.Pos, bool) {
	list := f.list
	sort.SliceStable(list, func(i, j int) bool { return list[i].pos < list[j].pos })
	for i := range list {
		if LooksLikeSQL(list[i].text) {
			continue
		}
		var parts []string
		for j := i; j < len(list) && j < i+fragmentWindow; j++ {
			if strings.Contains(list[j].text, "%w") {
				break
			}
			parts = append(parts, list[j].text)
			if len(parts) > 1 && startsStatement(parts) {
				return list[i].pos, true
			}
		}
	}
	return token.NoPos, false
}

// startsStatement reports whether parts, joined by a space or by nothing,
// start a SQL statement: a space finds keywords kept in separate fragments
// ("CREATE", "TABLE t"), nothing finds a keyword split inside a word
// ("CREA", "TE TABLE t").
func startsStatement(parts []string) bool {
	return LooksLikeSQL(strings.Join(parts, " ")) || LooksLikeSQL(strings.Join(parts, ""))
}
