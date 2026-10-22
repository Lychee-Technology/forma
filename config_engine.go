package forma

import "fmt"

// DatabaseEngine names the database Forma's write path runs on (#633). The
// two engines speak the same wire protocol and SQL dialect, but they keep two
// rows consistent under concurrency differently, so Forma excludes concurrent
// writers of a row differently on each:
//
//   - EnginePostgreSQL waits. Every write takes a per-row advisory lock and
//     runs at READ COMMITTED, so a writer that waited for a row reads the
//     previous writer's commit before it writes. Claims skip rows other
//     transactions hold.
//   - EngineAuroraDSQL takes no locks and runs at its fixed REPEATABLE READ.
//     Every write of a row first writes that row's guard row, so of two
//     transactions that write one row, one is aborted with SQLSTATE 40001,
//     which Forma reports as ErrRetryableConflict. Nothing waits and nothing
//     is skipped.
//
// The engine is configuration rather than detection: a PostgreSQL deployment
// detected as DSQL would silently change how it locks. See
// docs/aurora-dsql.md.
type DatabaseEngine string

const (
	// EnginePostgreSQL is PostgreSQL 16, the default.
	EnginePostgreSQL DatabaseEngine = "postgresql"
	// EngineAuroraDSQL is Amazon Aurora DSQL.
	EngineAuroraDSQL DatabaseEngine = "aurora-dsql"
)

// Resolved answers the engine e names: the zero value is EnginePostgreSQL.
func (e DatabaseEngine) Resolved() DatabaseEngine {
	if e == "" {
		return EnginePostgreSQL
	}
	return e
}

// Validate refuses a name that is neither engine.
func (e DatabaseEngine) Validate() error {
	switch e.Resolved() {
	case EnginePostgreSQL, EngineAuroraDSQL:
		return nil
	}
	return &ConfigError{
		Field:   "database.engine",
		Message: fmt.Sprintf("must be %q or %q, got %q", EnginePostgreSQL, EngineAuroraDSQL, string(e)),
	}
}
