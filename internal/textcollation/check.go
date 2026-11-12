package textcollation

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// MigrationDoc is where an operator reads how to bring a column created
// before the contract under it.
const MigrationDoc = "docs/text-collation.md"

// ColumnsSQL lists the collatable columns of one table, named as SQL would
// spell it in $1, that are not under pg_catalog's "C" collation. It reads each
// column's own collation (pg_attribute.attcollation), not the database's
// default: a column that inherits the default holds the "default" collation,
// whose OID is not C's even in a database whose default is C, and an index on
// it cannot seek a COLLATE "C" range. POSIX orders like C but is another OID,
// so it is reported too. A table that does not exist answers no rows.
const ColumnsSQL = `SELECT a.attname::text, format_type(a.atttypid, a.atttypmod), quote_ident(n.nspname) || '.' || quote_ident(c.collname)
FROM pg_attribute a
JOIN pg_collation c ON c.oid = a.attcollation
JOIN pg_namespace n ON n.oid = c.collnamespace
WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped
  AND NOT (c.collname = 'C' AND n.nspname = 'pg_catalog')
ORDER BY a.attnum`

// Querier is the part of a pgx connection, pool or transaction Check reads
// the catalog through.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Violation is one collatable column of a Forma-owned table that is not under
// the C collation.
type Violation struct {
	Table, Column, Type, Collation string
}

func (v Violation) String() string {
	return fmt.Sprintf("table %s column %s (%s) has collation %s, expected pg_catalog.%s", v.Table, v.Column, v.Type, v.Collation, Name)
}

// Check reads the catalog for every collatable column of tables that is not
// under the C collation. Each table is named as SQL spells it, quoted where
// it needs quoting; one that does not exist contributes nothing.
func Check(ctx context.Context, q Querier, tables []string) ([]Violation, error) {
	var found []Violation
	for _, table := range tables {
		rows, err := q.Query(ctx, ColumnsSQL, table)
		if err != nil {
			return nil, fmt.Errorf("read the column collations of %s: %w", table, err)
		}
		v := Violation{Table: table}
		if _, err := pgx.ForEachRow(rows, []any{&v.Column, &v.Type, &v.Collation}, func() error {
			found = append(found, v)
			return nil
		}); err != nil {
			return nil, fmt.Errorf("scan the column collations of %s: %w", table, err)
		}
	}
	return found, nil
}

// DriftError reports the Forma-owned columns a catalog check found outside
// the contract. It is an operator-visible failure, not a caller's error.
type DriftError struct {
	Violations []Violation
}

func (e *DriftError) Error() string {
	lines := make([]string, 0, len(e.Violations))
	for _, v := range e.Violations {
		lines = append(lines, v.String())
	}
	return fmt.Sprintf("%d Forma-owned text column(s) are not under the C collation Forma's tables are created with (#816): %s. "+
		"These tables were created before the contract; CREATE TABLE IF NOT EXISTS does not change an existing column, "+
		"and init-db never rewrites one. Each statement below changes no stored value but holds an ACCESS EXCLUSIVE lock "+
		"on its table while it rebuilds every index on those columns; read %s before running them, then rerun: %s",
		len(e.Violations), strings.Join(lines, "; "), MigrationDoc, strings.Join(e.MigrationStatements(), " "))
}

// MigrationStatements answers one ALTER TABLE per table that brings its
// violating columns under C, keeping each column's type. PostgreSQL does not
// rewrite the table for it, since the type stays binary-compatible, but it
// rebuilds every index on a changed column, primary keys included.
func (e *DriftError) MigrationStatements() []string {
	var statements []string
	byTable := map[string][]string{}
	for _, v := range e.Violations {
		if _, seen := byTable[v.Table]; !seen {
			statements = append(statements, v.Table)
		}
		byTable[v.Table] = append(byTable[v.Table],
			fmt.Sprintf("ALTER COLUMN %s TYPE %s COLLATE %s", pgx.Identifier{v.Column}.Sanitize(), v.Type, Name))
	}
	for i, table := range statements {
		statements[i] = fmt.Sprintf("ALTER TABLE %s %s;", table, strings.Join(byTable[table], ", "))
	}
	return statements
}

// Require is Check that fails with a *DriftError when any column is outside
// the contract.
func Require(ctx context.Context, q Querier, tables []string) error {
	found, err := Check(ctx, q, tables)
	if err != nil {
		return err
	}
	if len(found) > 0 {
		return &DriftError{Violations: found}
	}
	return nil
}
