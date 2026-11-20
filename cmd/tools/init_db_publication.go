package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// publicationDDL is init-db's share of Protocol V2's publication contract
// (#895, docs/publication-protocol-v2.md): the per-schema publication record,
// generation and lifecycle epoch tables, named after entity_main. The text comes from
// publication.Statements, the one source the publication package's own
// suites create them from, rendered for opts.engine.
//
// The tables come first in init-db's table transaction; the records of the
// registered schemas follow in the same transaction
// (ensurePublicationRecords, init_db_records.go).
func publicationDDL(opts initDBOptions) []tableDDL {
	record := sqlutil.SuffixedIdentifier(opts.entityMain, publication.RecordTableSuffix)
	generation := sqlutil.SuffixedIdentifier(opts.entityMain, publication.GenerationTableSuffix)
	epoch := sqlutil.SuffixedIdentifier(opts.entityMain, publication.EpochTableSuffix)
	stmts := publication.Statements(opts.engine, opts.entityMain)
	return []tableDDL{
		{
			statement: stmts[0],
			announce:  fmt.Sprintf("Created publication record table: %s\n", record),
			failure:   "ensure publication record table",
		},
		{
			statement: stmts[1],
			announce:  fmt.Sprintf("Created publication generation table: %s\n", generation),
			failure:   "ensure publication generation table",
		},
		{
			statement: stmts[2],
			announce:  fmt.Sprintf("Created publication epoch table: %s\n", epoch),
			failure:   "ensure publication epoch table",
		},
	}
}

// The roles ownedTableRoles gives the publication tables.
const (
	publicationRecordRole     = "the publication record table"
	publicationGenerationRole = "the publication generation table"
	publicationEpochRole      = "the publication epoch table"
)

// requireDistinctPublicationTables refuses a configuration under which a
// publication table, derived from --entity-main-table, is the same relation
// as another table init-db ensures. CREATE TABLE IF NOT EXISTS would skip
// the occupied name without an error, and init-db would report the
// publication table ready while the relation under its name has another
// table's columns.
//
// Two spellings name one relation when PostgreSQL resolves them to the same
// schema and name, so the server resolves each one, not Go: an unqualified
// name goes to current_schema(), the schema CREATE TABLE uses under the
// connection's search_path, and each part is truncated as the server
// truncates an identifier (::name clips at NAMEDATALEN-1 bytes on a
// character boundary). "entity_main_publication" and
// "public"."entity_main_publication" are then one table under the default
// search path, and two under a search path that puts another schema first.
//
// It runs before the table transaction and sends no DDL, so a refusal leaves
// every relation as it was, the conflicting one included, and the remedy,
// renaming one of the two, is the operator's. On Aurora DSQL, where init-db
// cannot run yet (#690), it checks nothing, like requireTextCollation.
func requireDistinctPublicationTables(ctx context.Context, q tableResolver, opts initDBOptions) error {
	if opts.engine.Resolved() == forma.EngineAuroraDSQL {
		return nil
	}
	roles := ownedTableRoles(opts)
	resolved, err := resolveTables(ctx, q, roles)
	if err != nil {
		return err
	}
	return publicationTableCollision(opts, roles, resolved)
}

// tableResolver is the part of a connection resolveTables needs.
type tableResolver interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// resolvedTable is the relation a table spelling names: its schema and its
// name, as the server stores them.
type resolvedTable struct {
	schema, name string
}

// String spells the resolved relation as SQL would, both parts quoted.
func (r resolvedTable) String() string {
	return pgx.Identifier{r.schema, r.name}.Sanitize()
}

// resolveTablesSQL resolves each spelling of $1, in order, to the schema
// CREATE TABLE would create it in and the name it would get. parse_ident
// applies the parser's quoting and case folding, and the ::name casts its
// truncation. A spelling with no schema takes current_schema(), the first
// existing schema of the search path, or an empty schema when there is
// none, and then CREATE TABLE fails on its own.
const resolveTablesSQL = `SELECT COALESCE(CASE WHEN cardinality(p) > 1 THEN p[cardinality(p) - 1]::name END, current_schema(), '')::text,
       p[cardinality(p)]::name::text
  FROM (SELECT ord, parse_ident(spelling) AS p
          FROM unnest($1::text[]) WITH ORDINALITY AS t(spelling, ord)) AS s
 ORDER BY ord`

// resolveTables resolves every owned table's spelling on the server.
func resolveTables(ctx context.Context, q tableResolver, roles []ownedTable) ([]resolvedTable, error) {
	spellings := make([]string, 0, len(roles))
	for _, table := range roles {
		spellings = append(spellings, table.sql)
	}
	rows, err := q.Query(ctx, resolveTablesSQL, spellings)
	if err != nil {
		return nil, fmt.Errorf("resolve the table names %v: %w", spellings, err)
	}
	resolved, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (resolvedTable, error) {
		var r resolvedTable
		err := row.Scan(&r.schema, &r.name)
		return r, err
	})
	if err != nil {
		return nil, fmt.Errorf("read the resolved table names of %v: %w", spellings, err)
	}
	if len(resolved) != len(roles) {
		return nil, fmt.Errorf("resolving %d table names returned %d rows", len(roles), len(resolved))
	}
	return resolved, nil
}

// publicationTableCollision reports the first publication table whose
// resolved relation, resolved[i] for roles[i], is also another owned
// table's.
func publicationTableCollision(opts initDBOptions, roles []ownedTable, resolved []resolvedTable) error {
	for i, pub := range roles {
		if pub.role != publicationRecordRole && pub.role != publicationGenerationRole && pub.role != publicationEpochRole {
			continue
		}
		for j, other := range roles {
			if i == j || resolved[i] != resolved[j] {
				continue
			}
			remedy := "--entity-main-table"
			if other.flag != remedy {
				remedy += " or " + other.flag
			}
			return fmt.Errorf("%s is %s, derived from --entity-main-table %s, and %s is %s, which %s names; "+
				"PostgreSQL resolves both to %s, so CREATE TABLE IF NOT EXISTS would skip one and leave the other in its place; "+
				"change %s so the two differ",
				pub.sql, pub.role, opts.entityMain, other.sql, other.role, other.flag, resolved[i], remedy)
		}
	}
	return nil
}
