// Package sqlgentest provides shared helpers for tests that pin the hot
// leg's PostgreSQL scan contract on both sides: the runtime template
// (internal/sqlgen) and the executable §5 sketch in
// docs/federated-query/design.md (internal/federated).
package sqlgentest

import "regexp"

// postgresTableRefRe matches one hot-leg scan: a reference into the attached
// PostgreSQL catalog, <catalog>."schema"."table". The catalog is the
// runtime alias (forma_pg_<hex>, sqlgen.PostgresCatalogAlias) or the §5
// sketch's $PG placeholder. Schema and table are plain identifiers, so a
// predicate or a subquery smuggled into the reference does not match.
var postgresTableRefRe = regexp.MustCompile(
	`(forma_pg_[0-9a-f]{16}|\$PG)\."([A-Za-z_][A-Za-z0-9_]*)"\."([A-Za-z_][A-Za-z0-9_]*)"`)

// PostgresTableRef is one catalog-qualified scan found in SQL text.
type PostgresTableRef struct {
	Raw, Catalog, Schema, Table string
}

// FindPostgresTableRefs returns every hot-leg scan in sqlText, in order.
func FindPostgresTableRefs(sqlText string) []PostgresTableRef {
	var refs []PostgresTableRef
	for _, m := range postgresTableRefRe.FindAllStringSubmatch(sqlText, -1) {
		refs = append(refs, PostgresTableRef{Raw: m[0], Catalog: m[1], Schema: m[2], Table: m[3]})
	}
	return refs
}

// CollapsePostgresTableRefs rewrites every hot-leg scan to its bare table
// name, so the query runs on a local DuckDB against same-named relations.
func CollapsePostgresTableRefs(sqlText string) string {
	return postgresTableRefRe.ReplaceAllString(sqlText, "$3")
}
