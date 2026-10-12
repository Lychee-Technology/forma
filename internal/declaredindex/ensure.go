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
}

// Existing is one index the catalog reports on the entity_main table.
type Existing struct {
	Name string
	// Columns are the key columns in order; an expression column reads "".
	Columns []string
	// Valid is pg_index.indisvalid. An interrupted CREATE INDEX CONCURRENTLY
	// leaves the index behind with it false, and the planner never uses it.
	Valid bool
	// Plain is false for a partial, expression or unique index, none of which
	// a declaration produces.
	Plain bool
	// BuildPID is the backend still building the index concurrently, 0 when
	// no build is in progress or the progress view hides it from this role.
	BuildPID int32
}

// Report is what Ensure found and did. Every index Ensure is responsible for
// lands in exactly one of Created, Present or the returned error.
type Report struct {
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
// validity flag, the key columns in order, whether the index has a shape no
// declaration produces, and the backend building it concurrently, if any.
const catalogQuery = `SELECT c.relname::text,
       i.indisvalid,
       NOT (i.indisunique OR i.indpred IS NOT NULL OR i.indexprs IS NOT NULL) AS plain,
       ARRAY(
           SELECT COALESCE(a.attname::text, '')
           FROM unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
           LEFT JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
           ORDER BY k.ord
       ) AS columns,
       COALESCE((SELECT p.pid FROM pg_stat_progress_create_index p WHERE p.index_relid = i.indexrelid LIMIT 1), 0) AS build_pid
FROM pg_index i
JOIN pg_class c ON c.oid = i.indexrelid
WHERE i.indrelid = $1::regclass`

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
	before, err := listIndexes(ctx, conn, table)
	if err != nil {
		return Report{}, fmt.Errorf("read existing indexes before building: %w", err)
	}

	created := make(map[string]bool)
	var built []Index
	for _, ix := range planned {
		if _, exists := before[ix.Name]; exists {
			continue
		}
		if _, err := conn.Exec(ctx, ix.CreateSQL()); err != nil {
			return Report{Created: built}, fmt.Errorf("build declared index %s on %s for %s: %w; a build that fails part-way leaves the index invalid: drop it with DROP INDEX CONCURRENTLY %s and rerun",
				ix.Name, table, FormatSources(ix.Sources), err, pgx.Identifier{ix.Name}.Sanitize())
		}
		created[ix.Name] = true
		built = append(built, ix)
	}

	after := before
	if len(created) > 0 {
		if after, err = listIndexes(ctx, conn, table); err != nil {
			return Report{Created: built}, fmt.Errorf("re-read indexes after building: %w", err)
		}
	}
	return classify(table, planned, after, created)
}

// classify sorts the post-build catalog state into the Report and the
// problems Ensure fails on.
func classify(table string, planned []Index, after map[string]Existing, created map[string]bool) (Report, error) {
	var report Report
	var problems []error
	plannedNames := make(map[string]bool, len(planned))
	for _, ix := range planned {
		plannedNames[ix.Name] = true
		existing, ok := after[ix.Name]
		switch {
		case !ok:
			problems = append(problems, fmt.Errorf("declared index %s for %s is not on %s after CREATE INDEX CONCURRENTLY IF NOT EXISTS: another relation in the same schema already holds the name",
				ix.Name, FormatSources(ix.Sources), table))
		case !existing.Valid:
			problems = append(problems, invalidError(table, existing))
		case !existing.Plain || !slices.Equal(existing.Columns, ix.Columns):
			problems = append(problems, fmt.Errorf("declared index %s on %s has key (%s)%s, but %s declare (%s) as a plain index; init-db never replaces an index: drop it with DROP INDEX CONCURRENTLY %s and rerun",
				ix.Name, table, strings.Join(existing.Columns, ", "), shapeNote(existing), FormatSources(ix.Sources),
				strings.Join(ix.Columns, ", "), pgx.Identifier{ix.Name}.Sanitize()))
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
			problems = append(problems, invalidError(table, existing))
			continue
		}
		report.Unreferenced = append(report.Unreferenced, existing)
	}
	return report, errors.Join(problems...)
}

// invalidError reports an index that is not valid: still being built by
// another session, or left behind by an interrupted concurrent build.
func invalidError(table string, existing Existing) error {
	if existing.BuildPID != 0 {
		return fmt.Errorf("declared index %s on %s is still being built by backend pid %d; rerun once that build finishes",
			existing.Name, table, existing.BuildPID)
	}
	return fmt.Errorf("declared index %s on %s is invalid (pg_index.indisvalid = false), which an interrupted CREATE INDEX CONCURRENTLY leaves behind; unless another session is building it right now, drop it with DROP INDEX CONCURRENTLY %s and rerun",
		existing.Name, table, pgx.Identifier{existing.Name}.Sanitize())
}

func shapeNote(existing Existing) string {
	if existing.Plain {
		return ""
	}
	return " and is partial, unique or over an expression"
}

// listIndexes reads every index on table from the catalog, keyed by name.
func listIndexes(ctx context.Context, conn Conn, table string) (map[string]Existing, error) {
	rows, err := conn.Query(ctx, catalogQuery, sqlutil.SanitizeIdentifier(table))
	if err != nil {
		return nil, fmt.Errorf("list indexes on %s: %w", table, err)
	}
	indexes := make(map[string]Existing)
	var existing Existing
	if _, err := pgx.ForEachRow(rows, []any{&existing.Name, &existing.Valid, &existing.Plain, &existing.Columns, &existing.BuildPID}, func() error {
		row := existing
		row.Columns = slices.Clone(existing.Columns)
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
