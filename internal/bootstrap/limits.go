package bootstrap

import "github.com/lychee-technology/forma"

// ApplyLimitsFromEnv overlays the operator-settable request limits (#465) on
// cfg in place: the body/entity size cap, the batch cap, and the three
// per-request budgets. It touches nothing else, so it composes with the other
// overlays in this package in any order.
//
//	MAX_ENTITY_SIZE_BYTES          Entity.MaxEntitySize (HTTP body cap)
//	MAX_BATCH_SIZE                 Performance.MaxBatchSize
//	QUERY_TIMEOUT_SECONDS          Query.DefaultTimeout
//	TRANSACTION_TIMEOUT_SECONDS    Transaction.DefaultTimeout
//	DUCKDB_QUERY_TIMEOUT_SECONDS   DuckDB.QueryTimeout
//
// The overlay itself only parses; the range rules live in forma.Config.Validate
// (a timeout of 0 disables that bound and may not be negative; the size and
// batch caps must stay positive), and both cmd/server and cmd/lambda call it
// on the overlaid config before opening the database, so an out-of-range
// value fails at boot rather than silently widening a limit. An unparsable
// value keeps the default, like every other EnvInt overlay in this package.
func ApplyLimitsFromEnv(cfg *forma.Config) {
	if cfg == nil {
		return
	}
	cfg.Entity.MaxEntitySize = EnvInt("MAX_ENTITY_SIZE_BYTES", cfg.Entity.MaxEntitySize)
	cfg.Performance.MaxBatchSize = EnvInt("MAX_BATCH_SIZE", cfg.Performance.MaxBatchSize)
	cfg.Query.DefaultTimeout = envSeconds("QUERY_TIMEOUT_SECONDS", cfg.Query.DefaultTimeout)
	cfg.Transaction.DefaultTimeout = envSeconds("TRANSACTION_TIMEOUT_SECONDS", cfg.Transaction.DefaultTimeout)
	cfg.DuckDB.QueryTimeout = envSeconds("DUCKDB_QUERY_TIMEOUT_SECONDS", cfg.DuckDB.QueryTimeout)
}
