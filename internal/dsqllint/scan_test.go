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
	} {
		if !LooksLikeSQL(s) {
			t.Errorf("LooksLikeSQL(%q) = false, want true", s)
		}
	}
	for _, s := range []string{
		"begin tx: %w", "select batch row ids: %w", "commit failed", "the table to create", "", "-- only a comment",
	} {
		if LooksLikeSQL(s) {
			t.Errorf("LooksLikeSQL(%q) = true, want false", s)
		}
	}
}
