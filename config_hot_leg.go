package forma

import "fmt"

// HotLegTransport names how a federated pass reads the hot tier
// (DuckDBConfig.HotLegTransport, design.md §6.1.1).
type HotLegTransport string

const (
	// HotLegTransportAttach reads the hot tier from inside DuckDB, through
	// the PostgreSQL catalog the engine attaches (design.md §6.1). It was the
	// default until #857 and is the rollback until step 7 of
	// pgx-hot-leg-plan.md retires it.
	HotLegTransportAttach HotLegTransport = "attach"
	// HotLegTransportPgx reads the hot tier over the engine's pgx pool, in
	// one REPEATABLE READ READ ONLY snapshot per pass: the dirty set, the
	// hot keys and the page's hot rows. DuckDB reads only the parquet tiers,
	// and Go merges the two results. DefaultConfig selects it since #857.
	HotLegTransportPgx HotLegTransport = "pgx"
)

// IsPgx reports whether t selects the pgx hot leg. Empty, the field's zero
// value, selects attach (see DuckDBConfig.HotLegTransport).
func (t HotLegTransport) IsPgx() bool { return t == HotLegTransportPgx }

// ValidateHotLeg rejects a transport other than attach, pgx or empty, and a
// negative dirty-set cap. Like ValidateManifestRead, validation is
// independent of Enabled.
func (d DuckDBConfig) ValidateHotLeg() error {
	switch d.HotLegTransport {
	case "", HotLegTransportAttach, HotLegTransportPgx:
	default:
		return &ConfigError{
			Field:   "duckdb.hotLegTransport",
			Message: fmt.Sprintf("must be %q or %q, got %q", HotLegTransportAttach, HotLegTransportPgx, string(d.HotLegTransport)),
		}
	}
	if d.HotLegMaxDirtyRows < 0 {
		return &ConfigError{Field: "duckdb.hotLegMaxDirtyRows", Message: "must be greater than or equal to 0 (0 sets no cap)"}
	}
	return nil
}
