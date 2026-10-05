package bootstrap

import "github.com/lychee-technology/forma"

// ApplyLimitsFromEnv overlays the operator-settable request limits (#465) on
// cfg in place: the body/entity size cap, the batch cap, the pagination depth
// limit (#598), and the three per-request budgets. It touches nothing else, so
// it composes with the other overlays in this package in any order.
//
//	MAX_ENTITY_SIZE_BYTES          Entity.MaxEntitySize (HTTP body cap)
//	MAX_BATCH_SIZE                 Performance.MaxBatchSize
//	MAX_QUERY_ROWS                 Query.MaxRows
//	QUERY_TIMEOUT_SECONDS          Query.DefaultTimeout
//	TRANSACTION_TIMEOUT_SECONDS    Transaction.DefaultTimeout
//	DUCKDB_QUERY_TIMEOUT_SECONDS   DuckDB.QueryTimeout
//
// The overlay itself only parses; the range rules live in forma.Config.Validate
// (a timeout or MAX_QUERY_ROWS of 0 disables that bound and may not be
// negative, and a positive MAX_QUERY_ROWS must cover one full page; the size
// and batch caps must stay positive), and both cmd/server and cmd/lambda call it
// on the overlaid config before opening the database, so an out-of-range
// value fails at boot rather than silently widening a limit. A set but
// unparsable value fails here instead, with an *EnvError per bad variable
// (#600), and cfg is left untouched; unset keeps the value cfg already holds.
func ApplyLimitsFromEnv(cfg *forma.Config) error {
	if cfg == nil {
		return nil
	}
	var env envOverlay
	entitySize := env.integer("MAX_ENTITY_SIZE_BYTES", cfg.Entity.MaxEntitySize)
	batchSize := env.integer("MAX_BATCH_SIZE", cfg.Performance.MaxBatchSize)
	maxRows := env.integer("MAX_QUERY_ROWS", cfg.Query.MaxRows)
	queryTimeout := env.seconds("QUERY_TIMEOUT_SECONDS", cfg.Query.DefaultTimeout)
	txTimeout := env.seconds("TRANSACTION_TIMEOUT_SECONDS", cfg.Transaction.DefaultTimeout)
	duckDBTimeout := env.seconds("DUCKDB_QUERY_TIMEOUT_SECONDS", cfg.DuckDB.QueryTimeout)
	if err := env.err(); err != nil {
		return err
	}
	cfg.Entity.MaxEntitySize = entitySize
	cfg.Performance.MaxBatchSize = batchSize
	cfg.Query.MaxRows = maxRows
	cfg.Query.DefaultTimeout = queryTimeout
	cfg.Transaction.DefaultTimeout = txTimeout
	cfg.DuckDB.QueryTimeout = duckDBTimeout
	return nil
}
