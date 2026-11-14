package forma

import "fmt"

// HotLegTransport names how a federated pass reads the hot tier
// (DuckDBConfig.HotLegTransport, design.md §6.1). Only pgx remains: the
// attach transport, which read PostgreSQL from inside DuckDB through the
// postgres_scanner extension, was retired in step 7 of pgx-hot-leg-plan.md
// (#858).
type HotLegTransport string

// HotLegTransportPgx reads the hot tier over the engine's pgx pool, in one
// REPEATABLE READ READ ONLY snapshot per pass: the dirty set, the hot keys
// and the page's hot rows. DuckDB reads only the parquet tiers, and Go merges
// the two results.
const HotLegTransportPgx HotLegTransport = "pgx"

// retiredHotLegTransportAttach is the value that selected the retired attach
// transport. ValidateHotLeg names it so a deployment that rolled back to it
// learns at startup that the rollback is gone.
const retiredHotLegTransportAttach HotLegTransport = "attach"

// ValidateHotLeg rejects a transport other than pgx or empty, and a negative
// dirty-set cap. Like ValidateManifestRead, validation is independent of
// Enabled.
func (d DuckDBConfig) ValidateHotLeg() error {
	switch d.HotLegTransport {
	case "", HotLegTransportPgx:
	case retiredHotLegTransportAttach:
		return &ConfigError{
			Field: "duckdb.hotLegTransport",
			Message: fmt.Sprintf("the %q transport was retired (#858); federated reads always use %q, so unset the field or set it to %q",
				retiredHotLegTransportAttach, HotLegTransportPgx, HotLegTransportPgx),
		}
	default:
		return &ConfigError{
			Field:   "duckdb.hotLegTransport",
			Message: fmt.Sprintf("must be %q or empty, got %q", HotLegTransportPgx, string(d.HotLegTransport)),
		}
	}
	if d.HotLegMaxDirtyRows < 0 {
		return &ConfigError{Field: "duckdb.hotLegMaxDirtyRows", Message: "must be greater than or equal to 0 (0 sets no cap)"}
	}
	return nil
}
