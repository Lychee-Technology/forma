// Package textcollation holds the physical text contract of the tables Forma
// creates (#816): on PostgreSQL every collatable column of a Forma-owned table
// is declared COLLATE "C", whatever the database's default collation is.
//
// The C collation orders text by its UTF-8 bytes, the order Forma's text
// ranges take on every route and tier (#728). A column declared under it lets
// one ordinary btree index serve both equality and the range compares the
// PostgreSQL route renders as `<col> COLLATE "C" <op> $n`: the planner matches
// an index's collation to the compare's by OID, so an index under the
// database default cannot seek such a range, even in a database whose default
// is C, while an index on a C column inherits the OID the compare names.
//
// CREATE TABLE IF NOT EXISTS leaves an existing table as it is, so the
// contract has two halves: the DDL declares it (DDLDrift pins each copy of
// the DDL) and init-db reads the catalog of the tables it ensured (Check),
// refusing a column created before the contract instead of reporting success
// over it. Changing an existing column is an operator's decision, never
// init-db's: docs/text-collation.md describes the migration.
//
// Aurora DSQL's collation is fixed at C, and it refuses a COLLATE clause in a
// column definition (dsql-lint's collation rule), so its DDL declares plain
// TEXT: ForEngine derives that form from the PostgreSQL one. Whether DSQL's
// implicit collation lets a plain index seek a COLLATE "C" range is for a real
// cluster to show (#825), not this package.
package textcollation

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/lychee-technology/forma"
)

// Name is the collation's name as SQL spells it. sqlgen names it in every
// PostgreSQL text range compare; DDL names it on every text column.
const Name = `"C"`

// TextType is the type a Forma-owned text column is declared with on
// PostgreSQL.
const TextType = `TEXT COLLATE ` + Name

// ForEngine renders ddl, written for PostgreSQL, for engine: the statement
// itself on PostgreSQL, and on Aurora DSQL the statement without the
// COLLATE "C" clauses DSQL refuses, its own collation being C.
func ForEngine(engine forma.DatabaseEngine, ddl string) string {
	if engine.Resolved() == forma.EngineAuroraDSQL {
		return strings.ReplaceAll(ddl, " COLLATE "+Name, "")
	}
	return ddl
}

// collatableColumn matches one column definition of a CREATE TABLE statement,
// or the column an ALTER TABLE ... ADD COLUMN adds, whose type is collatable:
// the column name, the type and, when the definition names one, its
// collation. A definition starts a line or follows the opening parenthesis or
// a comma, so one-line DDL is read as well as DDL with a column per line.
var collatableColumn = regexp.MustCompile(
	`(?im)(?:^|[(,]|ADD\s+COLUMN\s+(?:IF\s+NOT\s+EXISTS\s+)?)\s*("[^"]+"|[a-z_][a-z0-9_]*)\s+` +
		`(text|character\s+varying|varchar|character|char|bpchar|citext|name)\b(?:\s*\(\s*\d+\s*\))?` +
		`(?:[^,()]*?\bCOLLATE\s+("[^"]+"|[a-z_][a-z0-9_]*))?`)

// DDLDrift returns one line per collatable column the statement declares
// without COLLATE "C", naming the column and what it declares instead. nil
// means every collatable column the statement declares is under the
// contract, or that it declares none.
func DDLDrift(ddl string) []string {
	var drift []string
	for _, m := range collatableColumn.FindAllStringSubmatch(ddl, -1) {
		switch got := m[3]; {
		case got == "":
			drift = append(drift, fmt.Sprintf("column %s is declared %s without COLLATE %s", m[1], strings.ToUpper(m[2]), Name))
		case got != Name:
			drift = append(drift, fmt.Sprintf("column %s is declared COLLATE %s, the contract is COLLATE %s", m[1], got, Name))
		}
	}
	sort.Strings(drift)
	return drift
}
