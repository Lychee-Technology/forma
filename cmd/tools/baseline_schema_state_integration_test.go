package main

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/federated"
)

// duringAudit is a parquet tier whose audit runs during. The baseline calls
// it after the PostgreSQL census of the schema has read every table, inside
// the window in which a writer of the old release can still commit.
func duringAudit(during func(ctx context.Context) error) *parquetTier {
	return &parquetTier{
		objects: staticObjects(),
		audit: func(ctx context.Context, _ int16, _ []string, _ forma.SchemaAttributeCache) ([]federated.DomainFinding, error) {
			return nil, during(ctx)
		},
	}
}

// TestBaselineSchemaRefusesAWriteCommittedDuringTheAudit: a process of the
// old release that still writes can commit after the census read its tables,
// and the compare-and-set on the pointer cannot see it. The integer
// declaration admits 1001 and not 1001.5, so a baseline that recorded here
// would certify a value outside it. Each write runs the statements the write
// path runs on entity_main, eav_data and change_log, in one transaction.
func TestBaselineSchemaRefusesAWriteCommittedDuringTheAudit(t *testing.T) {
	const row = "'00000000-0000-0000-0000-000000000001'"
	create := []string{
		"INSERT INTO %[1]s (ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at) VALUES (100, " + row + ", 10, 10)",
		"INSERT INTO %[2]s (schema_id, row_id, attr_id, value_numeric) VALUES (100, " + row + ", 1, 1001)",
		"INSERT INTO %[3]s (schema_id, row_id, flushed_at, changed_at) VALUES (100, " + row + ", 0, 10)",
	}
	cases := []struct {
		name          string
		before, write []string
		moved         string
	}{
		{"create", nil, []string{
			"INSERT INTO %[1]s (ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at) VALUES (100, " + row + ", 20, 20)",
			"INSERT INTO %[2]s (schema_id, row_id, attr_id, value_numeric) VALUES (100, " + row + ", 1, 1001.5)",
			"INSERT INTO %[3]s (schema_id, row_id, flushed_at, changed_at) VALUES (100, " + row + ", 0, 20)",
		}, "entity_main went from 0 row(s) with version sum 0 to 1 row(s) with version sum 20"},
		{"update", create, []string{
			"UPDATE %[1]s SET ltbase_updated_at = GREATEST(20, ltbase_updated_at + 1) WHERE ltbase_schema_id = 100 AND ltbase_row_id = " + row,
			"UPDATE %[2]s SET value_numeric = 1001.5 WHERE schema_id = 100 AND row_id = " + row,
			"UPDATE %[3]s SET changed_at = 20 WHERE schema_id = 100 AND row_id = " + row + " AND flushed_at = 0",
		}, "entity_main went from 1 row(s) with version sum 10 to 1 row(s) with version sum 20"},
		{"delete", create, []string{
			"DELETE FROM %[1]s WHERE ltbase_schema_id = 100 AND ltbase_row_id = " + row,
			"DELETE FROM %[2]s WHERE schema_id = 100 AND row_id = " + row,
			"UPDATE %[3]s SET changed_at = 20, deleted_at = 20 WHERE schema_id = 100 AND row_id = " + row + " AND flushed_at = 0",
		}, "entity_main went from 1 row(s) with version sum 10 to 0 row(s) with version sum 0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lt := newLedgerTarget(t, map[string]string{"orders": eavIntegerAttrs})
			lt.forgetLedger(t)
			lt.exec(t, tc.before)
			write := func(ctx context.Context) error {
				tx, err := lt.pool.Begin(ctx)
				if err != nil {
					return fmt.Errorf("begin the write: %w", err)
				}
				defer func() { _ = tx.Rollback(ctx) }()
				for _, stmt := range lt.storageStatements(tc.write) {
					if _, err := tx.Exec(ctx, stmt); err != nil {
						return fmt.Errorf("%s: %w", stmt, err)
					}
				}
				return tx.Commit(ctx)
			}

			out, err := lt.baseline(t, duringAudit(write))
			if err == nil || !strings.Contains(out, "stored data changed while it was audited") ||
				!strings.Contains(out, "schema orders (id 100): "+tc.moved) {
				t.Fatalf("baseline = %v, want the write refused (%s)\n%s", err, tc.moved, out)
			}
			if _, ok := lt.pointer(t, 100); ok {
				t.Fatal("a baseline refused for a write during its audit recorded a generation")
			}
		})
	}
}

// TestBaselineSchemaRefusesAnObjectPublishedDuringTheAudit: an exporter or a
// compaction of the old release can change the live object set after the
// baseline resolved the set it audits, so an object it records over would
// be one it never read.
func TestBaselineSchemaRefusesAnObjectPublishedDuringTheAudit(t *testing.T) {
	lt := newLedgerTarget(t, map[string]string{"orders": eavIntegerAttrs})
	lt.forgetLedger(t)
	live := []string{"s3://bucket/delta/a.parquet"}
	tier := &parquetTier{
		objects: func(context.Context, int16) ([]string, error) { return live, nil },
		audit: func(_ context.Context, _ int16, paths []string, _ forma.SchemaAttributeCache) ([]federated.DomainFinding, error) {
			if len(paths) != 1 || paths[0] != "s3://bucket/delta/a.parquet" {
				return nil, fmt.Errorf("audited %v, want the set resolved before the audit", paths)
			}
			live = []string{"s3://bucket/delta/b.parquet", "s3://bucket/delta/c.parquet"}
			return nil, nil
		},
	}

	out, err := lt.baseline(t, tier)
	if err == nil || !strings.Contains(out, "schema orders (id 100): the live parquet objects changed (2 added, 1 removed)") {
		t.Fatalf("baseline = %v, want the object set change refused\n%s", err, out)
	}
	if _, ok := lt.pointer(t, 100); ok {
		t.Fatal("a baseline refused for an object set change recorded a generation")
	}

	clean := func(context.Context, int16, []string, forma.SchemaAttributeCache) ([]federated.DomainFinding, error) {
		return nil, nil
	}
	out, err = lt.baseline(t, &parquetTier{objects: staticObjects(live...), audit: clean})
	if err != nil || !strings.Contains(out, "Recorded generation 1 of schema orders") {
		t.Fatalf("a baseline over a set nothing changed = %v, want generation 1 recorded\n%s", err, out)
	}
}

// storageStatements fills each statement's %[1]s, %[2]s and %[3]s with the
// target's entity_main, eav_data and change_log tables.
func (lt *ledgerTarget) storageStatements(stmts []string) []string {
	out := make([]string, len(stmts))
	for i, stmt := range stmts {
		out[i] = fmt.Sprintf(stmt, quoteIdentifier(lt.opts.entityMain), quoteIdentifier(lt.opts.eavTable), quoteIdentifier(lt.opts.changeLog))
	}
	return out
}

func (lt *ledgerTarget) exec(t *testing.T, stmts []string) {
	t.Helper()
	for _, stmt := range lt.storageStatements(stmts) {
		if _, err := lt.pool.Exec(context.Background(), stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
}
