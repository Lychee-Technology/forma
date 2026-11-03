package dsqllint

// The regressions here each feed a generator whose statement head an
// earlier scan misread into the classified passthrough executor, and
// check that the executor's entry does not cover the generator.

import (
	"strings"
	"testing"
)

// dynamicGo holds generators whose statement head names a table, alias,
// privilege or savepoint only known at run time, each feeding the
// classified passthrough executor (#740 review R3-F1).
const dynamicGo = `package p

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"
)

// exec is the classified passthrough executor.
func exec(ctx context.Context, tx pgx.Tx, sql string) error {
	_, err := tx.Exec(ctx, sql)
	return err
}

func dynamicTable(ctx context.Context, tx pgx.Tx, table, column string) error {
	return exec(ctx, tx, strings.Join([]string{"UPDATE ", table, " SET ", column, " = $1"}, ""))
}

func sanitized(schema, table string) string {
	return "UPDATE " + pgx.Identifier{schema, table}.Sanitize() + " AS t SET value = $1"
}

func built(name string) string {
	var b strings.Builder
	b.WriteString("with ")
	b.WriteString(name)
	b.WriteString(" as (select 1) select * from x")
	return b.String()
}

func savepoint(name string) string { return "SAVEPOINT " + name }

func spaced(kw string) string { return strings.Join([]string{"INSERT", kw, "INTO t VALUES (1)"}, " ") }

func message(table string) string { return "updated " + table + " settings" }
`

// TestDynamicFragmentGeneratorsFeedingAClassifiedExecutorFail is the
// regression for a generator whose statement head holds a dynamic piece,
// such as a table name, between its string fragments: the scan reads the
// piece as a name, so the generator is a site the executor's entry does
// not cover, and the guard fails until it is classified itself.
func TestDynamicFragmentGeneratorsFeedingAClassifiedExecutorFail(t *testing.T) {
	snap := scanFixture(t, map[string]string{"p/p.go": dynamicGo})
	generators := []string{"p/p.go:dynamicTable", "p/p.go:sanitized", "p/p.go:built", "p/p.go:savepoint", "p/p.go:spaced"}
	for _, loc := range generators {
		if got := snap.Sites[loc]; got.Why != whyFragments {
			t.Errorf("Sites[%s] = %+v, want a site that %s", loc, got, whyFragments)
		}
	}
	if site, ok := snap.Sites["p/p.go:message"]; ok {
		t.Errorf("p/p.go:message assembles no SQL, but the scan reported %+v", site)
	}

	classified := []Entry{{Class: ClassPassthrough, Location: "p/p.go:exec", Line: 1}}
	joined := ""
	for _, p := range CheckInventory(classified, snap) {
		joined += p.Error() + "\n"
	}
	for _, loc := range generators {
		if !strings.Contains(joined, loc+" "+whyFragments) || !strings.Contains(joined, "no inventory entry classifies it") {
			t.Errorf("CheckInventory did not refuse the unclassified generator %s:\n%s", loc, joined)
		}
	}

	for i, loc := range generators {
		classified = append(classified, Entry{Class: ClassUncovered, Location: loc, Line: i + 2, Gap: "later"})
	}
	if problems := CheckInventory(classified, snap); len(problems) != 0 {
		t.Errorf("CheckInventory with every generator classified = %v, want none", problems)
	}
}

// cteGo holds generators whose statement is a WITH query with an explicit
// column list, written whole or built around a dynamic name or column
// list, each feeding the classified passthrough executor (#740 review
// R4-F1).
const cteGo = `package p

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// exec is the classified passthrough executor.
func exec(ctx context.Context, tx pgx.Tx, sql string) error {
	_, err := tx.Exec(ctx, sql)
	return err
}

func query() string { return "WITH ids(id) AS (SELECT 1) SELECT id FROM ids" }

func run(ctx context.Context, tx pgx.Tx) error { return exec(ctx, tx, query()) }

func recursive() string {
	return "with recursive walk (id, parent) as (select id, parent from t) select id from walk"
}

func quoted() string { return ` + "`" + `WITH "Ids" (id) AS NOT MATERIALIZED (SELECT 1) SELECT id FROM "Ids"` + "`" + ` }

func dynamicColumns(ctx context.Context, tx pgx.Tx, cols string) error {
	return exec(ctx, tx, "WITH ids("+cols+") AS (SELECT 1) SELECT * FROM ids")
}

func dynamicName(ctx context.Context, tx pgx.Tx, name string) error {
	return exec(ctx, tx, "WITH "+name+" (id) AS (SELECT 1) SELECT id FROM "+name)
}
`

// TestCTEColumnListGeneratorsFeedingAClassifiedExecutorFail is the
// regression for a generator whose WITH query names its columns: the head
// reads WITH <name>(<columns>) AS, which the scan recognises as a statement
// start whether the text is one literal or built around a dynamic name or
// column list, so the generator is a site the passthrough executor's entry
// does not cover, and the guard fails until it is classified itself.
func TestCTEColumnListGeneratorsFeedingAClassifiedExecutorFail(t *testing.T) {
	snap := scanFixture(t, map[string]string{"p/p.go": cteGo})
	want := map[string]string{
		"p/p.go:query":          whyLiteral,
		"p/p.go:recursive":      whyLiteral,
		"p/p.go:quoted":         whyLiteral,
		"p/p.go:dynamicColumns": whyFragments,
		"p/p.go:dynamicName":    whyFragments,
	}
	for loc, why := range want {
		if got := snap.Sites[loc]; got.Why != why {
			t.Errorf("Sites[%s] = %+v, want a site that %s", loc, got, why)
		}
	}
	if site, ok := snap.Sites["p/p.go:run"]; ok {
		t.Errorf("p/p.go:run only calls the generator and the executor, but the scan reported %+v", site)
	}

	classified := []Entry{{Class: ClassPassthrough, Location: "p/p.go:exec", Line: 1}}
	joined := ""
	for _, p := range CheckInventory(classified, snap) {
		joined += p.Error() + "\n"
	}
	for loc, why := range want {
		if !strings.Contains(joined, loc+" "+why) || !strings.Contains(joined, "no inventory entry classifies it") {
			t.Errorf("CheckInventory did not refuse the unclassified generator %s:\n%s", loc, joined)
		}
	}

	line := 2
	for loc := range want {
		classified = append(classified, Entry{Class: ClassUncovered, Location: loc, Line: line, Gap: "later"})
		line++
	}
	if problems := CheckInventory(classified, snap); len(problems) != 0 {
		t.Errorf("CheckInventory with every generator classified = %v, want none", problems)
	}
}

// punctGo holds generators whose statement keyword runs straight into
// punctuation, a comment or a parenthesis, each feeding the classified
// passthrough executor (#740 review R5-F1).
const punctGo = `package p

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"
)

// exec is the classified passthrough executor.
func exec(ctx context.Context, tx pgx.Tx, sql string) error {
	_, err := tx.Exec(ctx, sql)
	return err
}

func parenthesized() string { return "SELECT(1)" }

func run(ctx context.Context, tx pgx.Tx) error { return exec(ctx, tx, parenthesized()) }

func star() string { return "select*from t" }

func quotedTable() string { return ` + "`" + `UPDATE"t"SET a = 1` + "`" + ` }

func commented() string { return "INSERT/* v2 */INTO t VALUES (1)" }

func union() string { return "(SELECT 1) UNION (SELECT 2)" }

func dynamic(ctx context.Context, tx pgx.Tx, cols string) error {
	return exec(ctx, tx, strings.Join([]string{"SELECT", "(", cols, ") FROM t"}, ""))
}

func selection(n int) string { return "selection(" + "n" + ")" }
`

// TestPunctuatedGeneratorsFeedingAClassifiedExecutorFail is the regression
// for a generator whose statement keyword needs no space after it, such as
// SELECT(1): the scan reads SQL as tokens, so the generator is a site the
// passthrough executor's entry does not cover, and the guard fails until
// it is classified itself.
func TestPunctuatedGeneratorsFeedingAClassifiedExecutorFail(t *testing.T) {
	snap := scanFixture(t, map[string]string{"p/p.go": punctGo})
	want := map[string]string{
		"p/p.go:parenthesized": whyLiteral,
		"p/p.go:star":          whyLiteral,
		"p/p.go:quotedTable":   whyLiteral,
		"p/p.go:commented":     whyLiteral,
		"p/p.go:union":         whyLiteral,
		"p/p.go:dynamic":       whyFragments,
	}
	for loc, why := range want {
		if got := snap.Sites[loc]; got.Why != why {
			t.Errorf("Sites[%s] = %+v, want a site that %s", loc, got, why)
		}
	}
	for _, loc := range []string{"p/p.go:run", "p/p.go:selection"} {
		if site, ok := snap.Sites[loc]; ok {
			t.Errorf("%s holds no SQL of its own, but the scan reported %+v", loc, site)
		}
	}

	classified := []Entry{{Class: ClassPassthrough, Location: "p/p.go:exec", Line: 1}}
	joined := ""
	for _, p := range CheckInventory(classified, snap) {
		joined += p.Error() + "\n"
	}
	for loc, why := range want {
		if !strings.Contains(joined, loc+" "+why) || !strings.Contains(joined, "no inventory entry classifies it") {
			t.Errorf("CheckInventory did not refuse the unclassified generator %s:\n%s", loc, joined)
		}
	}

	line := 2
	for loc := range want {
		classified = append(classified, Entry{Class: ClassUncovered, Location: loc, Line: line, Gap: "later"})
		line++
	}
	if problems := CheckInventory(classified, snap); len(problems) != 0 {
		t.Errorf("CheckInventory with every generator classified = %v, want none", problems)
	}
}

// settingGo holds generators of configuration statements other than SET
// TRANSACTION, SET SESSION and SET LOCAL, each feeding the classified
// passthrough executor (#740 review R6-F1). DSQL's lint refuses a bare SET
// (rule set_transaction), so none may reach it unclassified.
const settingGo = `package p

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// exec is the classified passthrough executor.
func exec(ctx context.Context, tx pgx.Tx, sql string) error {
	_, err := tx.Exec(ctx, sql)
	return err
}

func setting() string { return "SET default_transaction_isolation = 'serializable'" }

func run(ctx context.Context, tx pgx.Tx) error { return exec(ctx, tx, setting()) }

func searchPath(schema string) string { return fmt.Sprintf("set search_path to %s", schema) }

func named(name, value string) string { return strings.Join([]string{"SET ", name, " = '", value, "'"}, "") }

func timeZone() string { return "SET TIME ZONE 'UTC'" }

func reset(name string) string { return "RESET " + name }

func show() string { return "SHOW transaction_isolation" }

func discard() string { return "DISCARD ALL" }

func label(name string) string { return "set up " + name + " settings" }
`

// TestSettingGeneratorsFeedingAClassifiedExecutorFail is the regression
// for a generator of a bare configuration statement, such as SET
// default_transaction_isolation = 'serializable': the scan recognises
// every SET form and a whole RESET, SHOW or DISCARD, so the generator is
// a site the passthrough executor's entry does not cover, and the guard
// fails until it is classified itself.
func TestSettingGeneratorsFeedingAClassifiedExecutorFail(t *testing.T) {
	snap := scanFixture(t, map[string]string{"p/p.go": settingGo})
	want := map[string]string{
		"p/p.go:setting":    whyLiteral,
		"p/p.go:searchPath": whyLiteral,
		"p/p.go:named":      whyFragments,
		"p/p.go:timeZone":   whyLiteral,
		"p/p.go:reset":      whyFragments,
		"p/p.go:show":       whyLiteral,
		"p/p.go:discard":    whyLiteral,
	}
	for loc, why := range want {
		if got := snap.Sites[loc]; got.Why != why {
			t.Errorf("Sites[%s] = %+v, want a site that %s", loc, got, why)
		}
	}
	for _, loc := range []string{"p/p.go:run", "p/p.go:label"} {
		if site, ok := snap.Sites[loc]; ok {
			t.Errorf("%s holds no SQL of its own, but the scan reported %+v", loc, site)
		}
	}

	classified := []Entry{{Class: ClassPassthrough, Location: "p/p.go:exec", Line: 1}}
	joined := ""
	for _, p := range CheckInventory(classified, snap) {
		joined += p.Error() + "\n"
	}
	for loc, why := range want {
		if !strings.Contains(joined, loc+" "+why) || !strings.Contains(joined, "no inventory entry classifies it") {
			t.Errorf("CheckInventory did not refuse the unclassified generator %s:\n%s", loc, joined)
		}
	}

	line := 2
	for loc := range want {
		classified = append(classified, Entry{Class: ClassUncovered, Location: loc, Line: line, Gap: "later"})
		line++
	}
	if problems := CheckInventory(classified, snap); len(problems) != 0 {
		t.Errorf("CheckInventory with every generator classified = %v, want none", problems)
	}
}

// createGo holds generators of CREATE statements whose object kind follows
// a modifier, or is one DSQL does not support, each feeding the
// classified passthrough executor (#740 review R7-F1). dsql-lint refuses
// CREATE MATERIALIZED VIEW (unsupported_materialized_view) and CREATE
// UNLOGGED TABLE (parse_error), so none may reach it unclassified.
const createGo = `package p

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// exec is the classified passthrough executor.
func exec(ctx context.Context, tx pgx.Tx, sql string) error {
	_, err := tx.Exec(ctx, sql)
	return err
}

func materialized() string { return "CREATE MATERIALIZED VIEW v AS SELECT 1" }

func run(ctx context.Context, tx pgx.Tx) error { return exec(ctx, tx, materialized()) }

func unlogged() string { return "CREATE UNLOGGED TABLE t (a INT)" }

func global(name string) string { return fmt.Sprintf("create global temporary table %s (a int)", name) }

func builtView(name string) string {
	return strings.Join([]string{"CREATE", "OR", "REPLACE", "MATERIALIZED", "VIEW", name, "AS SELECT 1"}, " ")
}

func language() string { return "CREATE OR REPLACE TRUSTED PROCEDURAL LANGUAGE plx" }

func foreign() string { return "CREATE FOREIGN TABLE f (a INT) SERVER s" }

func refresh(name string) string { return "REFRESH MATERIALIZED VIEW " + name }

func dropMaterialized() string { return "DROP MATERIALIZED VIEW IF EXISTS v" }

func dropExtension() string { return "DROP EXTENSION pg_trgm" }

func label(name string) string { return "create the " + name + " view" }
`

// TestCreateModifierGeneratorsFeedingAClassifiedExecutorFail is the
// regression for a generator whose CREATE statement puts a modifier before
// its object kind, or names a kind DSQL does not support, such as CREATE
// MATERIALIZED VIEW or CREATE UNLOGGED TABLE: the scan reads any run of
// CREATE modifiers and every object kind, so the generator is a site the
// passthrough executor's entry does not cover, and the guard fails until
// it is classified itself.
func TestCreateModifierGeneratorsFeedingAClassifiedExecutorFail(t *testing.T) {
	snap := scanFixture(t, map[string]string{"p/p.go": createGo})
	want := map[string]string{
		"p/p.go:materialized":     whyLiteral,
		"p/p.go:unlogged":         whyLiteral,
		"p/p.go:global":           whyLiteral,
		"p/p.go:builtView":        whyFragments,
		"p/p.go:language":         whyLiteral,
		"p/p.go:foreign":          whyLiteral,
		"p/p.go:refresh":          whyLiteral,
		"p/p.go:dropMaterialized": whyLiteral,
		"p/p.go:dropExtension":    whyLiteral,
	}
	for loc, why := range want {
		if got := snap.Sites[loc]; got.Why != why {
			t.Errorf("Sites[%s] = %+v, want a site that %s", loc, got, why)
		}
	}
	for _, loc := range []string{"p/p.go:run", "p/p.go:label"} {
		if site, ok := snap.Sites[loc]; ok {
			t.Errorf("%s holds no SQL of its own, but the scan reported %+v", loc, site)
		}
	}

	classified := []Entry{{Class: ClassPassthrough, Location: "p/p.go:exec", Line: 1}}
	joined := ""
	for _, p := range CheckInventory(classified, snap) {
		joined += p.Error() + "\n"
	}
	for loc, why := range want {
		if !strings.Contains(joined, loc+" "+why) || !strings.Contains(joined, "no inventory entry classifies it") {
			t.Errorf("CheckInventory did not refuse the unclassified generator %s:\n%s", loc, joined)
		}
	}

	line := 2
	for loc := range want {
		classified = append(classified, Entry{Class: ClassUncovered, Location: loc, Line: line, Gap: "later"})
		line++
	}
	if problems := CheckInventory(classified, snap); len(problems) != 0 {
		t.Errorf("CheckInventory with every generator classified = %v, want none", problems)
	}
}
