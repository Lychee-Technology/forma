package dsqllint

import (
	"strings"
	"testing"
)

// transactionGo holds generators of transaction-wrapped migrations, each
// feeding the classified passthrough executor (#740 review R10-F1). The
// first statement of each is one the scan only takes whole (BEGIN, START
// TRANSACTION, SAVEPOINT), so the scan once read none of them as SQL,
// although dsql-lint refuses the first with multi_ddl_transaction.
const transactionGo = `package p

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

func migration() string {
	return "BEGIN; CREATE TABLE a (id INT); CREATE TABLE b (id INT); COMMIT;"
}

func run(ctx context.Context, tx pgx.Tx) error { return exec(ctx, tx, migration()) }

func started() string {
	return "START TRANSACTION ISOLATION LEVEL SERIALIZABLE;\n-- the audit table\nCREATE TABLE a (id INT);\nCOMMIT;"
}

func commented() string { return "/* v2 */ begin;/* tables */create table a (id int); commit" }

func savepointed() string { return "SAVEPOINT s; RELEASE SAVEPOINT s; DROP TABLE a" }

func joined(table string) string {
	return strings.Join([]string{"BEGIN;", "CREATE TABLE " + table + " (id INT);", "COMMIT;"}, "\n")
}

func label() string { return "begin; then create the tables" }
`

// TestTransactionWrappedMigrationsFeedingAClassifiedExecutorFail is the
// regression for a generator whose SQL is a whole transaction, BEGIN;
// CREATE TABLE a ...; COMMIT;: the scan reads past a leading statement it
// only takes whole to the statements after it, so the generator is a site
// the passthrough executor's entry does not cover, and the guard fails
// until it is classified itself.
func TestTransactionWrappedMigrationsFeedingAClassifiedExecutorFail(t *testing.T) {
	snap := scanFixture(t, map[string]string{"p/p.go": transactionGo})
	want := map[string]string{
		"p/p.go:migration":   whyLiteral,
		"p/p.go:started":     whyLiteral,
		"p/p.go:commented":   whyLiteral,
		"p/p.go:savepointed": whyLiteral,
		"p/p.go:joined":      whyLiteral,
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

// TestMultiStatementText holds LooksLikeSQL to text of several statements:
// a leading statement it only takes whole starts SQL when SQL follows its
// semicolon, and not when prose does or no semicolon ends it.
func TestMultiStatementText(t *testing.T) {
	for _, s := range []string{
		"BEGIN; CREATE TABLE a (id INT); CREATE TABLE b (id INT); COMMIT;",
		"begin transaction; insert into t values (1); commit", "BEGIN WORK;\n\nUPDATE t SET a = 1;",
		"START TRANSACTION READ WRITE; DELETE FROM t", "BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY;SELECT 1",
		"BEGIN; -- the tables\nCREATE TABLE a (id INT)", "BEGIN;/* x */;CREATE TABLE a (id INT)",
		"BEGIN; BEGIN; CREATE TABLE a (id INT)", "COMMIT; CREATE TABLE a (id INT)", "SAVEPOINT s; DROP TABLE a",
		"ROLLBACK TO SAVEPOINT s; SELECT 1", "CHECKPOINT; VACUUM t", "RESET ALL; SET search_path TO app",
		"BEGIN;\nCOMMIT;", "begin ; select 1", "begin;select(1)",
	} {
		if !LooksLikeSQL(s) {
			t.Errorf("LooksLikeSQL(%q) = false, want true", s)
		}
	}
	for _, s := range []string{
		"begin; then create the tables", "begin tx: create table failed", "begin create table a (id int)",
		"commit; done", "savepoint reached; now continue", "end; of input",
	} {
		if LooksLikeSQL(s) {
			t.Errorf("LooksLikeSQL(%q) = true, want false", s)
		}
	}
}
