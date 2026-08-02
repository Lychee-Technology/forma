package cdc

import (
	"github.com/lychee-technology/forma/internal/pgdsn"
)

// PGDSNParams are the inputs for a libpq keyword/value connection string.
type PGDSNParams = pgdsn.Params

// BuildPGDSN renders a libpq keyword/value DSN with every string value quoted
// (single-quote wrapped, backslash and single-quote escaped) so passwords with
// spaces or quotes survive parsing by pgx and DuckDB's postgres scanner.
//
// The implementation moved to internal/pgdsn so internal/federated can quote the
// postgres_scan DSN the same way (#301). Behaviour is unchanged; the tests in
// pgdsn_test.go and redact_test.go remain the contract.
func BuildPGDSN(p PGDSNParams) string {
	return pgdsn.Build(p)
}
