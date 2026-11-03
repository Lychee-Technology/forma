package dsqllint

import (
	"go/ast"
	"go/token"
	"regexp"
	"strconv"
	"strings"
)

// The scan marks a symbol as a site for any of three reasons. Each one
// alone can miss SQL that another catches, which is why there are three:
//
//   - it holds SQL text: a string literal, or a constant expression folded
//     from literals and same-package string constants, that starts a SQL
//     statement in any letter case, after any leading SQL comments;
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
)

// sqlStatement matches text that starts a SQL statement, in any letter
// case. A leading BEGIN or START TRANSACTION only counts as the whole text
// (beginStatement), so error text such as "begin tx: ..." is not SQL.
var sqlStatement = regexp.MustCompile(`(?i)^(?:` +
	`CREATE\s+(?:OR\s+REPLACE\s+)?(?:UNIQUE\s+)?(?:TEMP(?:ORARY)?\s+)?(?:TABLE|INDEX|SEQUENCE|VIEW|SCHEMA|EXTENSION|TYPE|FUNCTION|TRIGGER|DOMAIN|ROLE)\b|` +
	`ALTER\s+(?:TABLE|INDEX|SEQUENCE|VIEW|SCHEMA|TYPE|ROLE)\b|` +
	`DROP\s+(?:TABLE|INDEX|SEQUENCE|VIEW|SCHEMA|TYPE|FUNCTION|ROLE)\b|` +
	`INSERT\s+INTO\b|` +
	`UPDATE\s+\S+\s+(?:(?:AS\s+)?\w+\s+)?SET\b|` +
	`DELETE\s+FROM\b|` +
	`SELECT\s|` +
	`WITH\s+(?:RECURSIVE\s+)?\w+\s+AS\b|` +
	`SET\s+(?:TRANSACTION|SESSION|LOCAL)\b|` +
	`TRUNCATE\b|LOCK\s+TABLE\b|COPY\s*[\w(]|GRANT\s+\w+\s+ON\b|REVOKE\s+\w+\s+ON\b|` +
	`COMMIT\s*;?\s*$|ROLLBACK\s*;?\s*$|SAVEPOINT\s+\w+\s*;?\s*$)`)

var beginStatement = regexp.MustCompile(`(?i)^(?:BEGIN|START\s+TRANSACTION)(?:\s+(?:TRANSACTION|WORK))?` +
	`(?:[\s,]+(?:ISOLATION\s+LEVEL\s+(?:SERIALIZABLE|REPEATABLE\s+READ|READ\s+COMMITTED|READ\s+UNCOMMITTED)|READ\s+(?:ONLY|WRITE)|(?:NOT\s+)?DEFERRABLE))*\s*;?\s*$`)

// LooksLikeSQL reports whether s starts a SQL statement once leading
// whitespace and SQL comments are skipped. Text holding an error-wrapping
// verb is never SQL.
func LooksLikeSQL(s string) bool {
	if strings.Contains(s, "%w") {
		return false
	}
	s = skipLeadingComments(s)
	return sqlStatement.MatchString(s) || beginStatement.MatchString(s)
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
	ast.Inspect(node, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.BasicLit, *ast.BinaryExpr:
			if s, ok := foldString(x.(ast.Expr), fs.consts); ok && LooksLikeSQL(s) {
				note(x.Pos(), whyLiteral)
				return false
			}
		case *ast.CallExpr:
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
	return site
}
