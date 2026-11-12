package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma/internal/textcollation"
)

// columnsWithStatistics answers, sorted, the columns of table that hold
// planner statistics, as pg_stats shows them to the table's owner.
func (tg *initDBTarget) columnsWithStatistics(t *testing.T, table string) []string {
	t.Helper()
	rows, err := tg.pool.Query(context.Background(), `
		SELECT s.attname::text FROM pg_stats s
		WHERE to_regclass(quote_ident(s.schemaname) || '.' || quote_ident(s.tablename)) = to_regclass($1)
		ORDER BY 1`, table)
	if err != nil {
		t.Fatalf("read the statistics of %s: %v", table, err)
	}
	columns, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("scan the statistics of %s: %v", table, err)
	}
	return columns
}

// statementTable answers the table an ALTER TABLE or ANALYZE statement of
// DriftError.MigrationStatements names, and whether it is the ANALYZE.
func statementTable(t *testing.T, stmt string) (string, bool) {
	t.Helper()
	fields := strings.Fields(stmt)
	switch {
	case len(fields) == 2 && fields[0] == "ANALYZE":
		return strings.TrimSuffix(fields[1], ";"), true
	case len(fields) > 2 && fields[0] == "ALTER" && fields[1] == "TABLE":
		return fields[2], false
	}
	t.Fatalf("unexpected migration statement %q", stmt)
	return "", false
}

// runCheckingStatistics runs statements in order and checks, after each one,
// which columns of its table hold statistics: an ALTER TABLE deletes exactly
// those of the columns it changes, and the ANALYZE after it brings back every
// one the table held before. Once all have run, every table holds statistics
// on the same columns as before them.
func (lt *legacyTarget) runCheckingStatistics(t *testing.T, statements []string, analyzed, changed map[string][]string) {
	t.Helper()
	for _, stmt := range statements {
		lt.exec(t, stmt)
		table, isAnalyze := statementTable(t, stmt)
		want := analyzed[table]
		if !isAnalyze {
			want = slices.DeleteFunc(slices.Clone(want), func(c string) bool { return slices.Contains(changed[table], c) })
		}
		if got := lt.columnsWithStatistics(t, table); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("after %s\n%s holds statistics on %v, want %v", stmt, table, got, want)
		}
	}
	for table, want := range analyzed {
		if got := lt.columnsWithStatistics(t, table); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("after the statements %s holds statistics on %v, want %v as before them", table, got, want)
		}
	}
}

// R1-F1 of #816's review: ALTER COLUMN ... TYPE deletes the changed columns'
// planner statistics even when only their collation changes, and nothing
// gathers them again until autovacuum's next analyze, so a migrated table
// would resume traffic with its range predicates estimated blind. The
// statements init-db prints analyze each table after altering it, and the
// rollback the docs describe does the same; both leave every changed column
// of a table that holds rows with statistics again.
func TestInitDBMigrationAndRollbackRestorePlannerStatistics(t *testing.T) {
	lt := newLegacyTarget(t)
	changed := map[string][]string{}
	for _, c := range lt.columns {
		changed[c.table] = append(changed[c.table], c.column)
	}
	// A legacy deployment has statistics: autovacuum or an operator analyzed it.
	analyzed := map[string][]string{}
	for table := range changed {
		lt.exec(t, "ANALYZE "+table)
		analyzed[table] = lt.columnsWithStatistics(t, table)
	}
	for _, table := range []string{lt.main, lt.eav} {
		for _, column := range changed[table] {
			if !slices.Contains(analyzed[table], column) {
				t.Fatalf("%s.%s has no statistics after ANALYZE, so this test would prove nothing: %v", table, column, analyzed[table])
			}
		}
	}

	out, err := lt.run(t)
	var drift *textcollation.DriftError
	if !errors.As(err, &drift) {
		t.Fatalf("init-db over text outside C: err = %v, want a *textcollation.DriftError\n%s", err, out)
	}
	migration := drift.MigrationStatements()
	lt.runCheckingStatistics(t, migration, analyzed, changed)
	if out, err := lt.run(t); err != nil {
		t.Fatalf("init-db after the migration: %v\n%s", err, out)
	}

	// The rollback: the same statements with the collation the refusal listed.
	rollback := make([]string, 0, len(migration))
	for _, stmt := range migration {
		rollback = append(rollback, strings.ReplaceAll(stmt, `COLLATE "C"`, `COLLATE "default"`))
	}
	lt.runCheckingStatistics(t, rollback, analyzed, changed)
	if out, err := lt.run(t); !errors.As(err, &drift) {
		t.Fatalf("init-db after the rollback: err = %v, want a *textcollation.DriftError\n%s", err, out)
	}
}
