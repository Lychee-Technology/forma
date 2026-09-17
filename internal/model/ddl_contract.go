package model

import (
	"fmt"
	"regexp"
	"sort"
)

// entityMainDDLColumn matches one column definition of a CREATE TABLE
// statement: the column name and its SQL type. Constraint lines (PRIMARY
// KEY) start with an upper-case keyword and do not match.
var entityMainDDLColumn = regexp.MustCompile(`(?m)^\s*([a-z][a-z0-9_]*)\s+(DOUBLE PRECISION|TEXT|SMALLINT|INTEGER|BIGINT|UUID)\b`)

// columnKindSQLType is the Postgres type a column of each kind is declared
// with in entity_main.
var columnKindSQLType = map[ColumnKind]string{
	ColumnKindText:     "TEXT",
	ColumnKindSmallint: "SMALLINT",
	ColumnKindInteger:  "INTEGER",
	ColumnKindBigint:   "BIGINT",
	ColumnKindDouble:   "DOUBLE PRECISION",
	ColumnKindUUID:     "UUID",
}

// EntityMainDDLDrift compares the column definitions of a CREATE TABLE
// statement for entity_main with EntityMainColumnDescriptors and returns one
// line per discrepancy: a runtime column the DDL lacks, a DDL column the
// runtime cannot write, project or flush, or a type that differs from the
// descriptor's kind. nil means the DDL declares exactly the runtime set.
//
// The DDL is hand-maintained in three places (#440): cmd/tools/init_db.go,
// internal/e2e_harness/production/ddl.go and
// internal/e2e_harness/federated/ddl.go. Each copy's package pins it with
// this so the set the writer's allowlist, the read projection and the CDC
// column order derive from cannot drift from what the table has (#585). The
// toy entity_main in internal/e2e_harness/fixtures.go is deliberately
// minimal and is not a copy of the production DDL, so it is not pinned.
func EntityMainDDLDrift(ddl string) []string {
	declared := make(map[string]string)
	for _, m := range entityMainDDLColumn.FindAllStringSubmatch(ddl, -1) {
		declared[m[1]] = m[2]
	}
	var drift []string
	for _, desc := range EntityMainColumnDescriptors {
		sqlType, ok := declared[desc.Name]
		switch {
		case !ok:
			drift = append(drift, fmt.Sprintf("runtime column %s is not declared by the DDL", desc.Name))
		case sqlType != columnKindSQLType[desc.Kind]:
			drift = append(drift, fmt.Sprintf("column %s is declared %s, runtime kind expects %s", desc.Name, sqlType, columnKindSQLType[desc.Kind]))
		}
	}
	for name := range declared {
		if !IsMainTableColumn(name) {
			drift = append(drift, fmt.Sprintf("DDL column %s is not a runtime column: never written, projected or flushed", name))
		}
	}
	sort.Strings(drift)
	return drift
}
