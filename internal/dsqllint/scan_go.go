package dsqllint

import (
	"go/ast"
	"go/token"
	"strconv"
	"strings"
)

// The scan marks a symbol as a site for any of four reasons. Each one
// alone can miss SQL that another catches, which is why there are four:
//
//   - it holds SQL text: a string literal, or a constant expression folded
//     from literals and same-package string constants, that starts a SQL
//     statement in any letter case, after any leading SQL comments, read
//     as written or as SQL tokens (SELECT(1) starts a SELECT);
//   - it assembles SQL text from fragments: its string literals and
//     same-package string constants, read in source order and joined by
//     spaces, start a SQL statement at one of them, however they are put
//     together (strings.Join, fmt.Sprintf arguments, a strings.Builder,
//     append), with each run of dynamic pieces between them (variables,
//     parameters, calls) read as one name or left out, so a generator
//     built from fragments, a run-time table name among them, is found
//     where it builds, even when it hands its result to an executor the
//     inventory already classifies;
//   - it calls a database method (Exec, Query, QueryRow, Begin, BeginTx,
//     SendBatch, CopyFrom, Prepare, Queue and their Context forms), so SQL
//     assembled from fragments no pattern recognises is still found where
//     it is sent;
//   - it names pgx or database/sql transaction options, which pgx turns
//     into a BEGIN ... ISOLATION LEVEL statement the code never spells.
const (
	whyLiteral = "holds SQL text"
	whyExec    = "calls a database method"
	whyTxOpts  = "sets transaction options"
	// whyFragments: the symbol's string fragments, read in source order,
	// start a SQL statement although no single expression does, such as
	// strings.Join([]string{"CREATE ", "TABLE t (a INT)"}, "").
	whyFragments = "assembles SQL text from fragments"
)

// LooksLikeSQL reports whether s starts a SQL statement once leading
// whitespace and SQL comments are skipped, read as written or in the token
// form sqlTokens writes, so a keyword that runs into punctuation, such as
// SELECT(1), still counts (#740 review R5-F1). Reading the text as written
// too keeps heads the token form splits, such as UPDATE %s SET. The text
// alone decides: error prose is told apart by where it is written (an
// argument of an error constructor, errorCalls), never by its characters,
// so SQL that holds %w, such as LIKE '%weekly%', is still SQL (#740
// review R9-F1).
func LooksLikeSQL(s string) bool {
	for _, form := range []string{skipLeadingComments(s), sqlTokens(s)} {
		if sqlStatement.MatchString(form) || beginStatement.MatchString(form) {
			return true
		}
	}
	return false
}

func skipLeadingComments(s string) string {
	for {
		s = strings.TrimLeft(s, " \t\r\n;")
		switch {
		case strings.HasPrefix(s, "--"):
			end := strings.IndexByte(s, '\n')
			if end < 0 {
				return ""
			}
			s = s[end+1:]
		case strings.HasPrefix(s, "/*"):
			end := strings.Index(s, "*/")
			if end < 0 {
				return ""
			}
			s = s[end+2:]
		default:
			return s
		}
	}
}

// sqlTokens rewrites s as its SQL tokens joined by single spaces, which is
// the form sqlStatement and beginStatement read: comments are dropped, each
// punctuation character is a token of its own, a quoted identifier is the
// name _ (dynamicText's stand-in), a string literal is an empty literal,
// and a dotted name (s.t, "s"."t") stays one token. SQL needs no space between a keyword
// and punctuation, so without this SELECT(1) would not read as a SELECT.
// Leading statement separators are dropped, too.
func sqlTokens(s string) string {
	var toks []string
	glue := false // the next name continues the previous one after a dot
	name := func(tok string) {
		if glue && len(toks) > 0 {
			toks[len(toks)-1] += "." + tok
		} else {
			toks = append(toks, tok)
		}
		glue = false
	}
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == '\f':
			i++
			glue = false
		case strings.HasPrefix(s[i:], "--"):
			end := strings.IndexByte(s[i:], '\n')
			if end < 0 {
				end = len(s) - i
			}
			i += end
			glue = false
		case strings.HasPrefix(s[i:], "/*"):
			end := strings.Index(s[i+2:], "*/")
			if end < 0 {
				i = len(s)
			} else {
				i += end + 4
			}
			glue = false
		case c == '"' || c == '\'':
			j := i + 1
			for j < len(s) {
				if s[j] == c {
					if j+1 < len(s) && s[j+1] == c {
						j += 2
						continue
					}
					break
				}
				j++
			}
			i = j + 1
			if c == '"' {
				name(dynamicText)
			} else {
				toks = append(toks, "''")
				glue = false
			}
		case isWordByte(c):
			j := i
			for j < len(s) && isWordByte(s[j]) {
				j++
			}
			name(s[i:j])
			i = j
		case c == '.' && len(toks) > 0 && i+1 < len(s) && (isWordByte(s[i+1]) && !isDigit(s[i+1]) || s[i+1] == '"') &&
			(i == 0 || s[i-1] == '"' || isWordByte(s[i-1])):
			glue = true
			i++
		default:
			if c != ';' || len(toks) > 0 {
				toks = append(toks, string(c))
			}
			glue = false
			i++
		}
	}
	return strings.Join(toks, " ")
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func isWordByte(c byte) bool {
	return c == '_' || c == '$' || isDigit(c) || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80
}

// dbMethods are the database methods whose call makes a symbol a site, with
// the fewest arguments a database call of that name takes. The scan has no
// type information, so a method of another type with one of these names
// and enough arguments is a site too: classify it not-dsql.
var dbMethods = map[string]int{
	"Exec": 1, "ExecContext": 2,
	"Query": 1, "QueryContext": 2,
	"QueryRow": 1, "QueryRowContext": 2,
	"Prepare": 1, "PrepareContext": 2,
	"Begin": 0, "BeginTx": 2,
	"SendBatch": 2, "CopyFrom": 3, "Queue": 1,
}

// txOptionNames are the pgx and database/sql identifiers that set how a
// transaction begins.
var txOptionNames = map[string]map[string]bool{
	"github.com/jackc/pgx/v5": {"TxOptions": true, "Serializable": true, "RepeatableRead": true, "ReadCommitted": true,
		"ReadUncommitted": true, "ReadOnly": true, "ReadWrite": true, "Deferrable": true, "NotDeferrable": true},
	"database/sql": {"TxOptions": true, "LevelDefault": true, "LevelReadUncommitted": true, "LevelReadCommitted": true,
		"LevelWriteCommitted": true, "LevelRepeatableRead": true, "LevelSnapshot": true, "LevelSerializable": true, "LevelLinearizable": true},
}

// errorCalls are the error constructors whose string arguments are error
// prose, never SQL, by package path: fmt.Errorf("create table %s: %w",
// name, err) only describes a failure. The scan reads them by where they
// are written, so a literal that merely holds an error-wrapping verb, such
// as a LIKE '%weekly%' pattern, is SQL like any other (#740 review R9-F1).
var errorCalls = map[string]map[string]bool{
	"fmt":    {"Errorf": true},
	"errors": {"New": true},
}

// errorArgs answers the arguments of call that are error prose: those of an
// error constructor (errorCalls) that are string constants.
func (fs fileScope) errorArgs(call *ast.CallExpr) []ast.Expr {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return nil
	}
	pkg, ok := sel.X.(*ast.Ident)
	if !ok || !errorCalls[fs.imports[pkg.Name]][sel.Sel.Name] {
		return nil
	}
	var args []ast.Expr
	for _, arg := range call.Args {
		if _, ok := foldString(arg, fs.consts); ok {
			args = append(args, arg)
		}
	}
	return args
}

// importNames maps each import's local name in file to its path.
func importNames(file *ast.File) map[string]string {
	names := make(map[string]string, len(file.Imports))
	for _, imp := range file.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			continue
		}
		name := path[strings.LastIndex(path, "/")+1:]
		if path == "github.com/jackc/pgx/v5" {
			name = "pgx"
		}
		if imp.Name != nil {
			name = imp.Name.Name
		}
		names[name] = path
	}
	return names
}

// packageConsts folds the package-level string constants of files, so a
// statement split across constants and literals is still recognised.
func packageConsts(files []*ast.File) map[string]string {
	consts := map[string]string{}
	for changed := true; changed; {
		changed = false
		for _, file := range files {
			for _, decl := range file.Decls {
				gen, ok := decl.(*ast.GenDecl)
				if !ok || gen.Tok != token.CONST {
					continue
				}
				for _, spec := range gen.Specs {
					changed = foldSpec(spec.(*ast.ValueSpec), consts) || changed
				}
			}
		}
	}
	return consts
}

func foldSpec(spec *ast.ValueSpec, consts map[string]string) bool {
	changed := false
	for i, name := range spec.Names {
		if _, done := consts[name.Name]; done || i >= len(spec.Values) {
			continue
		}
		if s, ok := foldString(spec.Values[i], consts); ok {
			consts[name.Name] = s
			changed = true
		}
	}
	return changed
}

// foldString answers the value of a constant string expression: literals,
// parentheses, + and the package's string constants.
func foldString(expr ast.Expr, consts map[string]string) (string, bool) {
	switch e := expr.(type) {
	case *ast.BasicLit:
		if e.Kind != token.STRING {
			return "", false
		}
		s, err := strconv.Unquote(e.Value)
		return s, err == nil
	case *ast.ParenExpr:
		return foldString(e.X, consts)
	case *ast.Ident:
		s, ok := consts[e.Name]
		return s, ok
	case *ast.BinaryExpr:
		if e.Op != token.ADD {
			return "", false
		}
		l, ok := foldString(e.X, consts)
		if !ok {
			return "", false
		}
		r, ok := foldString(e.Y, consts)
		return l + r, ok
	}
	return "", false
}

// fileScope is what analysing one file's symbols needs.
type fileScope struct {
	fset    *token.FileSet
	imports map[string]string
	consts  map[string]string
}

// siteOf answers the first reason node is a site, and its line; zero when
// it is none.
func (fs fileScope) siteOf(node ast.Node) Site {
	var site Site
	note := func(pos token.Pos, why string) {
		if line := fs.fset.Position(pos).Line; site.Line == 0 || line < site.Line {
			site = Site{Line: line, Why: why}
		}
	}
	var frags fragments
	prose := map[ast.Node]bool{}
	ast.Inspect(node, func(n ast.Node) bool {
		if prose[n] {
			frags.prose(n.Pos())
			return false
		}
		frags.collect(n, fs.consts)
		switch x := n.(type) {
		case *ast.BasicLit, *ast.BinaryExpr:
			if s, ok := foldString(x.(ast.Expr), fs.consts); ok && LooksLikeSQL(s) {
				note(x.Pos(), whyLiteral)
				return false
			}
		case *ast.CallExpr:
			for _, arg := range fs.errorArgs(x) {
				prose[arg] = true
			}
			if sel, ok := x.Fun.(*ast.SelectorExpr); ok {
				if min, db := dbMethods[sel.Sel.Name]; db && len(x.Args) >= min {
					note(sel.Sel.Pos(), whyExec)
				}
			}
		case *ast.SelectorExpr:
			if pkg, ok := x.X.(*ast.Ident); ok && txOptionNames[fs.imports[pkg.Name]][x.Sel.Name] {
				note(x.Pos(), whyTxOpts)
			}
		}
		return true
	})
	if pos, ok := frags.statementStart(); ok {
		note(pos, whyFragments)
	}
	return site
}
