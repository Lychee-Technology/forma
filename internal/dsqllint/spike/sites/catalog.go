// Package sites is the EXPERIMENTAL type-aware SQL call-site inventory of the
// DSQL typed call-site spike (#740). It is not production code.
package sites

// sinkSpec describes one driver entry point: its family and which parameter
// (0-based, receiver excluded) carries SQL text; -1 when the call sends SQL
// the driver writes itself (BEGIN from options, COPY, a batch).
type sinkSpec struct {
	Family   string
	SQLParam int
}

// driverMethods are the database entry points, keyed by the package that
// declares the method and then the method name. A call matches only when the
// resolved method object is declared in one of these packages, so a project
// type with its own Exec or Query never matches by name.
var driverMethods = map[string]map[string]sinkSpec{
	"github.com/jackc/pgx/v5": {
		"Exec": {"pgx-exec", 1}, "Query": {"pgx-query", 1}, "QueryRow": {"pgx-query", 1},
		"Prepare": {"pgx-prepare", 2}, "CopyFrom": {"pgx-copy", -1}, "SendBatch": {"pgx-batch-send", -1},
		"Begin": {"pgx-begin", -1}, "BeginTx": {"pgx-begin", -1}, "Queue": {"pgx-batch-queue", 0},
		"Commit": {"tx-end", -1}, "Rollback": {"tx-end", -1},
	},
	"github.com/jackc/pgx/v5/pgxpool": {
		"Exec": {"pgx-exec", 1}, "Query": {"pgx-query", 1}, "QueryRow": {"pgx-query", 1},
		"CopyFrom": {"pgx-copy", -1}, "SendBatch": {"pgx-batch-send", -1},
		"Begin": {"pgx-begin", -1}, "BeginTx": {"pgx-begin", -1},
	},
	"github.com/jackc/pgx/v5/pgconn": {
		"Exec": {"pgconn-exec", 1}, "ExecParams": {"pgconn-exec", 1}, "Prepare": {"pgconn-prepare", 2},
		"CopyFrom": {"pgconn-copy", 2}, "CopyTo": {"pgconn-copy", 2},
	},
	"database/sql": {
		"Exec": {"sql-exec", 0}, "ExecContext": {"sql-exec", 1},
		"Query": {"sql-query", 0}, "QueryContext": {"sql-query", 1},
		"QueryRow": {"sql-query", 0}, "QueryRowContext": {"sql-query", 1},
		"Prepare": {"sql-prepare", 0}, "PrepareContext": {"sql-prepare", 1},
		"Begin": {"sql-begin", -1}, "BeginTx": {"sql-begin", -1},
		"Commit": {"tx-end", -1}, "Rollback": {"tx-end", -1},
	},
}

// driverFuncs are package-level driver functions that send SQL.
var driverFuncs = map[string]map[string]sinkSpec{
	"github.com/jackc/pgx/v5": {"BeginFunc": {"pgx-begin", -1}, "BeginTxFunc": {"pgx-begin", -1}},
}

// driverTypes are the concrete or interface driver types considered as
// implementers when a project-declared interface method is resolved.
var driverTypes = map[string][]string{
	"github.com/jackc/pgx/v5":         {"Conn", "Tx"},
	"github.com/jackc/pgx/v5/pgxpool": {"Pool", "Conn", "Tx"},
	"database/sql":                    {"DB", "Tx", "Conn"},
}

// sinkNames is every driver method name: a project interface method with one
// of these names that resolves to no implementer is reported as unresolved.
func sinkNames() map[string]bool {
	names := map[string]bool{}
	for _, m := range driverMethods {
		for n := range m {
			names[n] = true
		}
	}
	return names
}
