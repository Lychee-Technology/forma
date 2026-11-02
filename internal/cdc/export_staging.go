package cdc

import (
	"fmt"
	"strings"

	"github.com/lychee-technology/forma"
)

// stageColumnType is the DuckDB type a PostgreSQL column is staged as. It
// mirrors the DuckDB Postgres scanner's mapping, which the export read
// through before #785 and whose types the projection's casts were written
// against: smallint, integer, bigint and uuid keep their type, text becomes
// VARCHAR, and both double precision and the unconstrained NUMERIC of
// eav_data.value_numeric become DOUBLE (design.md, unbound dates). An exact
// numeric transport (#621) would change this mapping and nothing else.
type stageColumnType string

const (
	stageSmallint stageColumnType = "SMALLINT"
	stageInteger  stageColumnType = "INTEGER"
	stageBigint   stageColumnType = "BIGINT"
	stageDouble   stageColumnType = "DOUBLE"
	stageVarchar  stageColumnType = "VARCHAR"
	stageUUID     stageColumnType = "UUID"
)

// stageColumn is one column of a PostgreSQL read and of the DuckDB table it
// is staged in.
type stageColumn struct {
	name string
	typ  stageColumnType
}

// exportStage is one PostgreSQL read of an export: the query, the DuckDB
// temp table its rows are appended to, and the CTE name the projection
// reads them under.
type exportStage struct {
	alias   string
	table   string
	columns []stageColumn
	query   string
}

// The staged tables are temp tables, so they belong to the one DuckDB
// connection an export pins, and no two exports share a connection at once.
// Fixed names are therefore safe; CREATE OR REPLACE also covers a drop that
// failed at the end of an earlier export on the same connection.
const (
	stageTableChangeLog = "cdc_export_change_log"
	stageTableMain      = "cdc_export_main"
	stageTableEAV       = "cdc_export_eav"
)

// exportTransport is how an export's PostgreSQL rows reach its projection:
// the row-id filter each PostgreSQL query applies, and the CTE that exposes
// one stage's rows to the projection. The projection, its casts and the
// COPY are the same for every transport.
type exportTransport struct {
	rowFilter func(column string) string
	source    func(stage exportStage) string
}

// stagedTransport reads PostgreSQL over pgx with the batch's row ids bound
// as one uuid[] parameter, and the projection reads the DuckDB temp tables
// the rows were appended to (#785).
var stagedTransport = exportTransport{
	rowFilter: func(column string) string {
		return column + " = ANY($1::uuid[])"
	},
	source: func(stage exportStage) string {
		return fmt.Sprintf("%s AS (SELECT * FROM temp.main.%s)", stage.alias, stage.table)
	},
}

var (
	changeLogStageColumns = []stageColumn{
		{"schema_id", stageSmallint},
		{"row_id", stageUUID},
		{"changed_at", stageBigint},
		{"deleted_at", stageBigint},
	}
	mainSystemStageColumns = []stageColumn{
		{"ltbase_row_id", stageUUID},
		{"ltbase_schema_id", stageSmallint},
		{"ltbase_created_at", stageBigint},
		{"ltbase_updated_at", stageBigint},
		{"ltbase_deleted_at", stageBigint},
	}
	eavStageColumns = []stageColumn{
		{"schema_id", stageSmallint},
		{"row_id", stageUUID},
		{"attr_id", stageSmallint},
		{"array_indices", stageVarchar},
		{"value_text", stageVarchar},
		{"value_numeric", stageDouble},
	}
)

// stageTypeForMainColumn stages a bound entity_main column by the type its
// name declares (forma.MainColumnBinding.ColumnType).
func stageTypeForMainColumn(binding *forma.MainColumnBinding) stageColumnType {
	switch binding.ColumnType() {
	case forma.MainColumnTypeSmallint:
		return stageSmallint
	case forma.MainColumnTypeInteger:
		return stageInteger
	case forma.MainColumnTypeBigint:
		return stageBigint
	case forma.MainColumnTypeDouble:
		return stageDouble
	case forma.MainColumnTypeUUID:
		return stageUUID
	default:
		return stageVarchar
	}
}

func stageColumnNames(columns []stageColumn) []string {
	names := make([]string, len(columns))
	for i, c := range columns {
		names[i] = c.name
	}
	return names
}

// buildStageSetupSQL renders the statements that run on the export's pinned
// connection before any row is staged: the memory limit, then one empty
// temp table per stage with the full column set, so a stage PostgreSQL
// returns no rows for still projects every column (#519).
func buildStageSetupSQL(memoryLimit string, stages []exportStage) string {
	var b strings.Builder
	fmt.Fprintf(&b, "PRAGMA memory_limit='%s';\n", memoryLimit)
	for _, stage := range stages {
		defs := make([]string, len(stage.columns))
		for i, c := range stage.columns {
			defs[i] = fmt.Sprintf("%s %s", c.name, c.typ)
		}
		fmt.Fprintf(&b, "CREATE OR REPLACE TEMP TABLE %s (%s);\n", stage.table, strings.Join(defs, ", "))
	}
	return b.String()
}

// buildStageDropSQL drops every staged table of an export.
func buildStageDropSQL(stages []exportStage) string {
	var b strings.Builder
	for _, stage := range stages {
		fmt.Fprintf(&b, "DROP TABLE IF EXISTS temp.main.%s;\n", stage.table)
	}
	return b.String()
}
