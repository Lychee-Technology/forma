package declaredindex

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// Conn is the connection surface Ensure needs; *pgx.Conn, *pgxpool.Conn and
// *pgxpool.Pool satisfy it. It must not be a pgx.Tx: CREATE INDEX
// CONCURRENTLY refuses to run inside a transaction block.
type Conn interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Existing is one index the catalog reports on the entity_main table.
type Existing struct {
	Name string
	// Columns are the key columns in order, INCLUDE columns left out; an
	// expression column reads "".
	Columns []string
	// Valid is pg_index.indisvalid. An interrupted CREATE INDEX CONCURRENTLY
	// leaves the index behind with it false, and the planner never uses it.
	Valid bool
	// Deviations names each way the index differs from the plain btree
	// CreateSQL builds over the same key, such as "brin access method" or
	// "INCLUDE columns"; empty for a plain btree.
	Deviations []string
	// BuildPID is the backend still building the index concurrently, 0 when
	// no build is in progress or the progress view hides it from this role.
	BuildPID int32
}

// Plain reports whether the index has the shape CreateSQL builds: a btree
// over plain key columns with default operator classes, collations and
// ordering, and nothing else.
func (e Existing) Plain() bool {
	return len(e.Deviations) == 0
}

// Report is what Ensure found and did. Every index Ensure is responsible for
// lands in exactly one of Created, Present or the returned error.
type Report struct {
	// Schema is the PostgreSQL schema the table name resolved to. Every index
	// on the table lives there, so DropSQL qualifies index names with it.
	Schema string
	// Created are planned indexes this run built; each ended up valid.
	Created []Index
	// Present are planned indexes that already existed, valid and with the
	// planned key, so this run issued no DDL for them.
	Present []Index
	// Unreferenced are valid declared-name indexes no current declaration
	// plans, typically left by a removed declaration. Ensure never drops one.
	Unreferenced []Existing
}

// catalogQuery lists every index on one table with what Ensure checks: the
// validity flag, the key columns in order, each way the index's shape differs
// from what CreateSQL builds, and the backend building it concurrently, if
// any. The key column list alone cannot tell a planned index from a BRIN one
// over the same columns, or from one carrying the trailing columns under
// INCLUDE, so every property CreateSQL leaves at its default is compared:
// access method, uniqueness, predicate, expressions, INCLUDE columns, and each
// key column's operator class, collation and ordering. indclass, indcollation
// and indoption cover the key columns only.
const catalogQuery = `SELECT c.relname::text,
       i.indisvalid,
       ARRAY(
           SELECT COALESCE(a.attname::text, '')
           FROM unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
           LEFT JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
           WHERE k.ord <= i.indnkeyatts
           ORDER BY k.ord
       ) AS columns,
       array_remove(ARRAY[
           CASE WHEN am.amname <> 'btree' THEN am.amname::text || ' access method' END,
           CASE WHEN i.indisunique THEN 'unique' END,
           CASE WHEN i.indpred IS NOT NULL THEN 'partial' END,
           CASE WHEN i.indexprs IS NOT NULL THEN 'expression key' END,
           CASE WHEN i.indnatts > i.indnkeyatts THEN 'INCLUDE columns' END,
           CASE WHEN EXISTS (
               SELECT 1 FROM unnest(i.indclass::oid[]) AS k(opclass)
               JOIN pg_opclass o ON o.oid = k.opclass
               WHERE NOT o.opcdefault
           ) THEN 'non-default operator class' END,
           CASE WHEN EXISTS (
               SELECT 1 FROM unnest(i.indkey::int2[], i.indcollation::oid[]) AS k(attnum, coll)
               JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
               WHERE k.coll IS NOT NULL AND k.coll <> a.attcollation
           ) THEN 'non-default collation' END,
           CASE WHEN EXISTS (
               SELECT 1 FROM unnest(i.indoption::int2[]) AS k(opt) WHERE k.opt <> 0
           ) THEN 'DESC or NULLS FIRST ordering' END
       ], NULL) AS deviations,
       COALESCE((SELECT p.pid FROM pg_stat_progress_create_index p WHERE p.index_relid = i.indexrelid LIMIT 1), 0) AS build_pid
FROM pg_index i
JOIN pg_class c ON c.oid = i.indexrelid
JOIN pg_am am ON am.oid = c.relam
WHERE i.indrelid = $1::regclass`

// tableSchemaQuery resolves the table the way catalogQuery does, through the
// connection's search_path when the name is unqualified, and returns the
// schema holding it.
const tableSchemaQuery = `SELECT n.nspname::text
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.oid = $1::regclass`

// EnsureFromRegistry plans the declarations of every schema registry lists and
// ensures them on table. It is FromRegistry, Plan and Ensure in one call: the
// path init-db and both e2e harnesses take, so they build the same indexes
// from the same declarations.
func EnsureFromRegistry(ctx context.Context, conn Conn, table string, registry forma.SchemaRegistry) (Report, error) {
	schemas, err := FromRegistry(registry)
	if err != nil {
		return Report{}, fmt.Errorf("read index declarations from the schema registry: %w", err)
	}
	planned, err := Plan(table, schemas)
	if err != nil {
		return Report{}, fmt.Errorf("plan declared indexes: %w", err)
	}
	return Ensure(ctx, conn, table, planned)
}

// Ensure builds every planned index missing from table and checks the result.
// Each build is its own autocommit CREATE INDEX CONCURRENTLY, so concurrent
// inserts, updates and deletes on the table are never blocked by it. A planned
// index that already exists gets no statement at all, which makes a re-run
// with unchanged declarations issue no DDL.
//
// Ensure never drops an index. It returns an error naming each declared-name
// index that is invalid (with the remediation), whose key differs from its
// declarations', or that could not be created under its name; the Report is
// filled either way. A build that fails stops the run: the error names that
// index, and the Report holds only the indexes built before it.
func Ensure(ctx context.Context, conn Conn, table string, planned []Index) (Report, error) {
	var schema string
	if err := conn.QueryRow(ctx, tableSchemaQuery, sqlutil.SanitizeIdentifier(table)).Scan(&schema); err != nil {
		return Report{}, fmt.Errorf("resolve the schema holding %s: %w", table, err)
	}
	before, err := listIndexes(ctx, conn, table)
	if err != nil {
		return Report{Schema: schema}, fmt.Errorf("read existing indexes before building: %w", err)
	}

	created := make(map[string]bool)
	var built []Index
	for _, ix := range planned {
		if _, exists := before[ix.Name]; exists {
			continue
		}
		if _, err := conn.Exec(ctx, ix.CreateSQL()); err != nil {
			return Report{Schema: schema, Created: built}, fmt.Errorf("build declared index %s on %s for %s: %w; a build that fails part-way leaves the index invalid: drop it with %s and rerun",
				ix.Name, table, FormatSources(ix.Sources), err, DropSQL(schema, ix.Name))
		}
		created[ix.Name] = true
		built = append(built, ix)
	}

	after := before
	if len(created) > 0 {
		if after, err = listIndexes(ctx, conn, table); err != nil {
			return Report{Schema: schema, Created: built}, fmt.Errorf("re-read indexes after building: %w", err)
		}
	}
	return classify(table, schema, planned, after, created)
}

// DropSQL renders the statement that drops the index name in schema without
// blocking writes: the remedy printed for a declared index Ensure will not use
// as it is, or that no declaration plans any more. Ensure never runs it. The
// name is schema-qualified because an unqualified DROP INDEX resolves through
// the session's search_path, which need not include the schema the index was
// created in.
func DropSQL(schema, name string) string {
	return "DROP INDEX CONCURRENTLY " + pgx.Identifier{schema, name}.Sanitize()
}

// classify sorts the post-build catalog state into the Report and the
// problems Ensure fails on.
func classify(table, schema string, planned []Index, after map[string]Existing, created map[string]bool) (Report, error) {
	report := Report{Schema: schema}
	var problems []error
	plannedNames := make(map[string]bool, len(planned))
	for _, ix := range planned {
		plannedNames[ix.Name] = true
		existing, ok := after[ix.Name]
		switch {
		case !ok:
			problems = append(problems, fmt.Errorf("declared index %s for %s is not on %s after CREATE INDEX CONCURRENTLY IF NOT EXISTS: another relation in schema %s already holds the name",
				ix.Name, FormatSources(ix.Sources), table, schema))
		case !existing.Valid:
			problems = append(problems, invalidError(table, schema, existing))
		case !existing.Plain() || !slices.Equal(existing.Columns, ix.Columns):
			problems = append(problems, fmt.Errorf("declared index %s on %s has key (%s)%s, but %s declare (%s) as a plain btree index; init-db never replaces an index: drop it with %s and rerun",
				ix.Name, table, strings.Join(existing.Columns, ", "), shapeNote(existing), FormatSources(ix.Sources),
				strings.Join(ix.Columns, ", "), DropSQL(schema, ix.Name)))
		case created[ix.Name]:
			report.Created = append(report.Created, ix)
		default:
			report.Present = append(report.Present, ix)
		}
	}

	for _, name := range sortedNames(after) {
		existing := after[name]
		if plannedNames[name] || !isDeclaredName(table, name) {
			continue
		}
		if !existing.Valid {
			problems = append(problems, invalidError(table, schema, existing))
			continue
		}
		report.Unreferenced = append(report.Unreferenced, existing)
	}
	return report, errors.Join(problems...)
}

// invalidError reports an index that is not valid: still being built by
// another session, or left behind by an interrupted concurrent build.
func invalidError(table, schema string, existing Existing) error {
	if existing.BuildPID != 0 {
		return fmt.Errorf("declared index %s on %s is still being built by backend pid %d; rerun once that build finishes",
			existing.Name, table, existing.BuildPID)
	}
	return fmt.Errorf("declared index %s on %s is invalid (pg_index.indisvalid = false), which an interrupted CREATE INDEX CONCURRENTLY leaves behind; unless another session is building it right now, drop it with %s and rerun",
		existing.Name, table, DropSQL(schema, existing.Name))
}

func shapeNote(existing Existing) string {
	if existing.Plain() {
		return ""
	}
	return " and is not a plain btree (" + strings.Join(existing.Deviations, ", ") + ")"
}

// listIndexes reads every index on table from the catalog, keyed by name.
func listIndexes(ctx context.Context, conn Conn, table string) (map[string]Existing, error) {
	rows, err := conn.Query(ctx, catalogQuery, sqlutil.SanitizeIdentifier(table))
	if err != nil {
		return nil, fmt.Errorf("list indexes on %s: %w", table, err)
	}
	indexes := make(map[string]Existing)
	var existing Existing
	if _, err := pgx.ForEachRow(rows, []any{&existing.Name, &existing.Valid, &existing.Columns, &existing.Deviations, &existing.BuildPID}, func() error {
		row := existing
		row.Columns = slices.Clone(existing.Columns)
		row.Deviations = slices.Clone(existing.Deviations)
		indexes[existing.Name] = row
		return nil
	}); err != nil {
		return nil, fmt.Errorf("scan indexes on %s: %w", table, err)
	}
	return indexes, nil
}

func sortedNames(indexes map[string]Existing) []string {
	names := make([]string, 0, len(indexes))
	for name := range indexes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// FormatSources renders sources as a comma-separated schema.index list.
func FormatSources(sources []Source) string {
	parts := make([]string, len(sources))
	for i, s := range sources {
		parts[i] = s.String()
	}
	return strings.Join(parts, ", ")
}
