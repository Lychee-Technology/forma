package dsqllint

import (
	"strings"
	"testing"
)

// nestedCommentGo holds generators whose SQL follows a nested block
// comment, each feeding the classified passthrough executor (#740 review
// R11-F1). PostgreSQL ends a block comment only after every comment nested
// in it, so the scan once took the text after the first "*/" ("outer */
// CREATE TABLE ...") for prose and missed each of them, although dsql-lint
// refuses the first with serial_type.
const nestedCommentGo = `package p

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// exec is the classified passthrough executor.
func exec(ctx context.Context, tx pgx.Tx, sql string) error {
	_, err := tx.Exec(ctx, sql)
	return err
}

func nested() string {
	return "/* outer /* inner */ outer */ CREATE TABLE t (id SERIAL PRIMARY KEY)"
}

func run(ctx context.Context, tx pgx.Tx) error { return exec(ctx, tx, nested()) }

func deeper() string { return "/* a /* b /* c */ b */ a */create table t (a int)" }

func overLines() string {
	return "/* v2\n * /* the audit table */\n */\nINSERT INTO t VALUES (1)"
}

func tokenised() string { return "/* a /* b */ c */SELECT(1)" }

func commentOnly() string { return "/* outer /* inner */ CREATE TABLE t (a INT) */" }
`

// TestNestedCommentPrefixedGeneratorsFeedingAClassifiedExecutorFail is the
// regression for a generator whose SQL starts after a nested block comment:
// the scan skips the whole comment, as PostgreSQL does, so the generator is
// a site the passthrough executor's entry does not cover, and the guard
// fails until it is classified itself. Text whose statement lies inside the
// comment is no SQL.
func TestNestedCommentPrefixedGeneratorsFeedingAClassifiedExecutorFail(t *testing.T) {
	snap := scanFixture(t, map[string]string{"p/p.go": nestedCommentGo})
	want := map[string]string{
		"p/p.go:nested":    whyLiteral,
		"p/p.go:deeper":    whyLiteral,
		"p/p.go:overLines": whyLiteral,
		"p/p.go:tokenised": whyLiteral,
	}
	for loc, why := range want {
		if got := snap.Sites[loc]; got.Why != why {
			t.Errorf("Sites[%s] = %+v, want a site that %s", loc, got, why)
		}
	}
	for _, loc := range []string{"p/p.go:run", "p/p.go:commentOnly"} {
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

// TestNestedBlockComments holds LooksLikeSQL to PostgreSQL's nesting of
// block comments, in both of its readings: SQL after a nested comment
// starts a statement, and text that is all one nested comment, or prose
// after one, does not.
func TestNestedBlockComments(t *testing.T) {
	for _, s := range []string{
		"/* outer /* inner */ outer */ CREATE TABLE t (id SERIAL PRIMARY KEY)",
		"/* a /* b */ c */SELECT(1)", "/* a /* b /* c */ */ */ delete from t",
		"-- c\n/* a /* b */ */ BEGIN; /* x /* y */ */ CREATE TABLE a (id INT)",
		"SELECT /* a /* b */ c */ 1", "INSERT/* a /* b */ c */INTO t VALUES (1)",
	} {
		if !LooksLikeSQL(s) {
			t.Errorf("LooksLikeSQL(%q) = false, want true", s)
		}
	}
	for _, s := range []string{
		"/* outer /* inner */ CREATE TABLE t (a INT) */",
		"/* a /* b */ c */ the audit table", "/* never /* closed */ CREATE TABLE t (a INT)",
	} {
		if LooksLikeSQL(s) {
			t.Errorf("LooksLikeSQL(%q) = true, want false", s)
		}
	}
}
