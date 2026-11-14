package factory

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

// warnPgxPoolSizing logs once, at construction, when the pgx hot leg could
// hold every connection of the PostgreSQL pool. The engine admits at most
// duckdb.maxConnections pgx passes at once, and each holds one pooled
// connection for its whole duration (internal/federated/pgx_pass_slots.go),
// so a pool no larger than that leaves the OLTP route nothing while
// federated reads are in flight. It is a warning, not a refusal: the pool is
// the caller's, and a deployment that never routes to DuckDB is unaffected.
func warnPgxPoolSizing(pool *pgxpool.Pool, cfg forma.DuckDBConfig) {
	if pool == nil {
		return
	}
	if msg := pgxPoolSizingWarning(pool.Config().MaxConns, cfg); msg != "" {
		zap.S().Warn(msg)
	}
}

// pgxPoolSizingWarning is the warning for a pool of poolMaxConns
// connections under cfg, or "" when the pool has room beyond the passes.
func pgxPoolSizingWarning(poolMaxConns int32, cfg forma.DuckDBConfig) string {
	if !cfg.Enabled {
		return ""
	}
	passes := max(1, cfg.MaxConnections)
	if int(poolMaxConns) > passes {
		return ""
	}
	return fmt.Sprintf("the PostgreSQL pool holds %d connections, and the pgx hot leg admits up to %d concurrent federated passes (duckdb.maxConnections), each holding one connection for its whole duration: "+
		"size the pool above duckdb.maxConnections so the OLTP route keeps a connection",
		poolMaxConns, passes)
}
