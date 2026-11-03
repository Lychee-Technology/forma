package dsqllint

import (
	"strings"
	"testing"
)

// percentGo holds generators whose SQL holds the characters %w, each
// feeding the classified passthrough executor, and error constructors whose
// prose reads like SQL (#740 review R9-F1). dsql-lint refuses the SERIAL
// column (serial_type), so none of the generators may reach it
// unclassified.
const percentGo = `package p

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// exec is the classified passthrough executor.
func exec(ctx context.Context, tx pgx.Tx, sql string) error {
	_, err := tx.Exec(ctx, sql)
	return err
}

func ddl() string {
	return "CREATE TABLE t (id SERIAL PRIMARY KEY, pattern TEXT DEFAULT '%weekly%')"
}

func like() string { return "SELECT * FROM t WHERE pattern LIKE '%weekly%'" }

func run(ctx context.Context, tx pgx.Tx) error {
	if err := exec(ctx, tx, ddl()); err != nil {
		return fmt.Errorf("create table t: %w", err)
	}
	return exec(ctx, tx, like())
}

func escaped(table string) string { return fmt.Sprintf("SELECT * FROM %s WHERE p LIKE '%%weekly%%'", table) }

func commented() string { return "-- 100%width\nCREATE TABLE t (a INT)" }

func joined() string { return strings.Join([]string{"delete", "from t where pattern like '%weekly%'"}, " ") }

const weekly = "SELECT id FROM t WHERE pattern LIKE '%w%'"

func wrapped(err error) error { return fmt.Errorf("select batch row ids: %w", err) }

func named(name string, err error) error { return fmt.Errorf("create table %s: %w", name, err) }

func split(name string, err error) error { return fmt.Errorf("%s %s: %w", "drop table", name, err) }

func plain() error { return errors.New("drop table failed") }
`

// TestPercentWSQLFeedingAClassifiedExecutorFails is the regression for SQL
// that holds the characters %w, such as a LIKE '%weekly%' pattern or a
// comment: the scan tells error prose apart by the error constructor it is
// written in, not by an error-wrapping verb, so the generator is a site the
// passthrough executor's entry does not cover, and the guard fails until it
// is classified itself, while an error constructor's prose stays no site.
func TestPercentWSQLFeedingAClassifiedExecutorFails(t *testing.T) {
	snap := scanFixture(t, map[string]string{"p/p.go": percentGo})
	want := map[string]string{
		"p/p.go:ddl":       whyLiteral,
		"p/p.go:like":      whyLiteral,
		"p/p.go:escaped":   whyLiteral,
		"p/p.go:commented": whyLiteral,
		"p/p.go:joined":    whyFragments,
		"p/p.go:weekly":    whyLiteral,
	}
	for loc, why := range want {
		if got := snap.Sites[loc]; got.Why != why {
			t.Errorf("Sites[%s] = %+v, want a site that %s", loc, got, why)
		}
	}
	for _, loc := range []string{"p/p.go:run", "p/p.go:wrapped", "p/p.go:named", "p/p.go:split", "p/p.go:plain"} {
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

// TestErrorProseIsNoSiteWhateverItsImport keeps an error constructor's
// prose from being a site under a renamed import, and keeps SQL in a call
// of another package's Errorf or New a site: only the standard library's
// error constructors write prose.
func TestErrorProseIsNoSiteWhateverItsImport(t *testing.T) {
	snap := scanFixture(t, map[string]string{"p/p.go": `package p

import (
	stderr "errors"
	format "fmt"

	"example.com/sqlbuilder"
)

func renamed(err error) error { return format.Errorf("select rows: %w", err) }

func renamedNew() error { return stderr.New("drop table failed") }

func other() string { return sqlbuilder.New("SELECT * FROM t WHERE p LIKE '%w'") }
`})
	for _, loc := range []string{"p/p.go:renamed", "p/p.go:renamedNew"} {
		if site, ok := snap.Sites[loc]; ok {
			t.Errorf("%s holds only error prose, but the scan reported %+v", loc, site)
		}
	}
	if got := snap.Sites["p/p.go:other"]; got.Why != whyLiteral {
		t.Errorf("Sites[p/p.go:other] = %+v, want a site that %s", got, whyLiteral)
	}
}
