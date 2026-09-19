package bootstrap

import "github.com/lychee-technology/forma"

// ApplyLimitsFromEnv overlays the operator-settable request limits (#465) on
// cfg in place: the body/entity size cap, the batch cap, and the three
// per-request budgets. It touches nothing else, so it composes with the other
// overlays in this package in any order; config.Validate still runs after it
// in every entry point, so an out-of-range value fails at boot rather than at
// the first request.
//
//	MAX_ENTITY_SIZE_BYTES          Entity.MaxEntitySize (HTTP body cap)
//	MAX_BATCH_SIZE                 Performance.MaxBatchSize
//	QUERY_TIMEOUT_SECONDS          Query.DefaultTimeout
//	TRANSACTION_TIMEOUT_SECONDS    Transaction.DefaultTimeout
//	DUCKDB_QUERY_TIMEOUT_SECONDS   DuckDB.QueryTimeout
//
// A timeout of 0 disables that bound; the size and batch caps must stay
// positive, which Validate enforces for the batch cap and the HTTP layer
// enforces for the body cap by falling back to the default on zero.
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
