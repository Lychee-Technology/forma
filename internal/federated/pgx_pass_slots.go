package federated

import (
	"context"
	"fmt"
)

// A pgx pass holds one pooled PostgreSQL connection from its snapshot to
// its page read, the DuckDB statement included (design.md §6.1). It opens
// the snapshot before it takes a DuckDB connection, so without a bound every
// request queued on the DuckDB pool would hold a PostgreSQL connection while
// it waits, and a burst of federated reads could take every connection the
// OLTP route needs (pgx-hot-leg-plan.md, Risks: Connection use).
//
// The engine therefore admits at most DuckDBConfig.MaxConnections pgx passes
// at once, the number of DuckDB connections that can serve them. A pass
// waits for its slot before it opens the snapshot, so the passes in flight
// hold at most that many PostgreSQL connections, and the wait costs nothing
// the DuckDB pool would not have made the pass wait anyway. Waiting only
// widens the pass's flush-grace window, whose cutoff was stamped at path
// resolution, before the wait: rows marked meanwhile stay in the hot leg.

// newPgxPassSlots sizes the bound: one slot per DuckDB connection, and at
// least one, as the DuckDB client opens at least one connection.
func newPgxPassSlots(maxConnections int) chan struct{} {
	return make(chan struct{}, max(1, maxConnections))
}

// acquirePgxPassSlot waits for a slot and returns its release. A request
// whose context ends first opens no snapshot, and fails with the context's
// error.
func (e *DBFederatedQueryEngine) acquirePgxPassSlot(ctx context.Context) (func(), error) {
	select {
	case e.pgxPassSlots <- struct{}{}:
		return func() { <-e.pgxPassSlots }, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("wait for one of the engine's %d pgx pass slots (duckdb.maxConnections): %w", cap(e.pgxPassSlots), ctx.Err())
	}
}
