package dsqllint

import (
	"strings"
	"testing"
)

// evasiveGo holds SQL generators that a scan of upper-case statement
// literals alone misses (#740 review R1-F2), and two that hold no SQL.
const evasiveGo = `package p

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5"
)

const create = "CREATE "
const table = create + "TABLE"

func lowerCase() string { return "create table t (a int)" }

func commentPrefixed() string { return "-- the audit table\n/* v2 */ CREATE TABLE t (a INT)" }

func concatenated() string { return "CREATE " + "TABLE t (a INT)" }

func viaConstants() string { return table + " t (a INT)" }

func fragments(db *sql.DB, verb, noun string) error {
	_, err := db.ExecContext(context.Background(), fmt.Sprintf("%s %s t (a INT)", verb, noun))
	return err
}

func isolation() pgx.TxOptions { return pgx.TxOptions{IsoLevel: pgx.Serializable} }

func sqlIsolation() *sql.TxOptions { return &sql.TxOptions{Isolation: sql.LevelSerializable} }

func prose() error { return fmt.Errorf("begin tx: %w", context.Canceled) }

func urlQuery(u interface{ Query() map[string][]string }) int { return len(u.Query()) }
`

// TestUnclassifiedGeneratorsFailWhateverTheirSpelling is the regression for
// the inventory guard's discovery: a new generator fails it whether its SQL
// is lower-case, comment-prefixed, assembled from literals or constants,
// built from fragments no pattern recognises (found where it is executed),
// or written by pgx from transaction options.
func TestUnclassifiedGeneratorsFailWhateverTheirSpelling(t *testing.T) {
	snap := scanFixture(t, map[string]string{"p/p.go": evasiveGo})
	want := map[string]string{
		"p/p.go:lowerCase":       whyLiteral,
		"p/p.go:commentPrefixed": whyLiteral,
		"p/p.go:concatenated":    whyLiteral,
		"p/p.go:viaConstants":    whyLiteral,
		"p/p.go:table":           whyLiteral,
		"p/p.go:fragments":       whyExec,
		"p/p.go:isolation":       whyTxOpts,
		"p/p.go:sqlIsolation":    whyTxOpts,
	}
	for loc, why := range want {
		if got := snap.Sites[loc]; got.Why != why {
			t.Errorf("Sites[%s] = %+v, want a site that %s", loc, got, why)
		}
	}
	for _, loc := range []string{"p/p.go:create", "p/p.go:prose", "p/p.go:urlQuery"} {
		if site, ok := snap.Sites[loc]; ok {
			t.Errorf("%s holds no SQL, but the scan reported %+v", loc, site)
		}
	}

	// With only the harmless symbols classified, every generator is refused.
	entries := []Entry{{Class: ClassNotDSQL, Location: "p/p.go:prose", Line: 1}}
	problems := CheckInventory(entries, snap)
	joined := ""
	for _, p := range problems {
		joined += p.Error() + "\n"
	}
	for loc := range want {
		if !strings.Contains(joined, loc+" ") || !strings.Contains(joined, "no inventory entry classifies it") {
			t.Errorf("CheckInventory did not refuse unclassified %s:\n%s", loc, joined)
		}
	}
}

func TestLooksLikeSQL(t *testing.T) {
	for _, s := range []string{
		"SELECT 1", "select a from t", "  insert into t values (1)", "update t set a = 1", "Delete From t",
		"-- c\nCREATE INDEX ASYNC i ON t (a)", "/* x */ alter table t add column b int", "begin", "BEGIN ISOLATION LEVEL READ COMMITTED",
		"start transaction read only;", "commit", "with x as (select 1) select * from x", "copy (select 1) to stdout",
		"WITH ids(id) AS (SELECT 1) SELECT id FROM ids", "with recursive t (n, m) as (select 1, 2) select n from t",
		`WITH "Ids" AS (SELECT 1) SELECT 1`, `with "a""b"(x)as (select 1) select x from "a""b"`,
		"WITH ids AS MATERIALIZED (SELECT 1) SELECT 1", "WITH a AS (SELECT 1), b(x) AS (SELECT 2) SELECT x FROM b",
	} {
		if !LooksLikeSQL(s) {
			t.Errorf("LooksLikeSQL(%q) = false, want true", s)
		}
	}
	for _, s := range []string{
		"begin tx: %w", "select batch row ids: %w", "commit failed", "the table to create", "", "-- only a comment",
		"with alias x", "with ids(id) handling", "with the (optional) list",
	} {
		if LooksLikeSQL(s) {
			t.Errorf("LooksLikeSQL(%q) = true, want false", s)
		}
	}
}

// fragmentGo holds generators that build a statement from fragments no
// single expression completes, and an executor that sends whatever SQL its
// caller hands it (#740 review R2-F2).
const fragmentGo = `package p

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

const verb = "create"

type row struct {
	Name string ` + "`json:\"select\" db:\"name\"`" + `
}

// exec is the classified passthrough executor.
func exec(ctx context.Context, tx pgx.Tx, sql string) error {
	_, err := tx.Exec(ctx, sql)
	return err
}

func joined() string { return strings.Join([]string{"CREATE ", "TABLE t (id int)"}, "") }

func sprinted(name string) string { return fmt.Sprintf("%s %s %s (id int)", "create", "table", name) }

func built(name string) string {
	var b strings.Builder
	b.WriteString("INSERT")
	b.WriteString(" INTO ")
	b.WriteString(name)
	return b.String()
}

func split() string { return strings.Join([]string{"INS", "ERT INTO t VALUES (1)"}, "") }

func viaConstant() string { return strings.Join([]string{verb, "unique", "index", "i on t (a)"}, " ") }

func send(ctx context.Context, tx pgx.Tx) error { return exec(ctx, tx, joined()) }

func labels() []string { return []string{"create", "update", "delete", "copy"} }

func wrapped(err error) error { return fmt.Errorf("select %s from %w", "rows", err) }
`

// TestFragmentBuiltGeneratorsFeedingAClassifiedExecutorFail is the
// regression for a generator assembled from fragments that sends its SQL
// through an executor the inventory already classifies: the executor's
// entry does not cover the generator, which fails the guard until it is
// classified itself.
func TestFragmentBuiltGeneratorsFeedingAClassifiedExecutorFail(t *testing.T) {
	snap := scanFixture(t, map[string]string{"p/p.go": fragmentGo})
	generators := []string{"p/p.go:joined", "p/p.go:sprinted", "p/p.go:built", "p/p.go:split", "p/p.go:viaConstant"}
	for _, loc := range generators {
		if got := snap.Sites[loc]; got.Why != whyFragments {
			t.Errorf("Sites[%s] = %+v, want a site that %s", loc, got, whyFragments)
		}
	}
	for _, loc := range []string{"p/p.go:verb", "p/p.go:row", "p/p.go:labels", "p/p.go:wrapped", "p/p.go:send"} {
		if site, ok := snap.Sites[loc]; ok {
			t.Errorf("%s assembles no SQL, but the scan reported %+v", loc, site)
		}
	}

	classified := []Entry{{Class: ClassPassthrough, Location: "p/p.go:exec", Line: 1}}
	joined := ""
	for _, p := range CheckInventory(classified, snap) {
		joined += p.Error() + "\n"
	}
	for _, loc := range generators {
		if !strings.Contains(joined, loc+" "+whyFragments) {
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

func dynamicTable(ctx context.Context, tx pgx.Tx, table string) error {
	return exec(ctx, tx, strings.Join([]string{"UPDATE ", table, " SET value = $1"}, ""))
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
