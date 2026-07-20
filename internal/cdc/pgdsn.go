package cdc

import (
	"fmt"
	"strings"
)

// PGDSNParams are the inputs for a libpq keyword/value connection string.
type PGDSNParams struct {
	Host     string
	Port     int
	User     string
	Password string
	DB       string
	SSLMode  string
}

// BuildPGDSN renders a libpq keyword/value DSN with every string value quoted
// (single-quote wrapped, backslash and single-quote escaped) so passwords with
// spaces or quotes survive parsing by pgx and DuckDB's postgres scanner.
func BuildPGDSN(p PGDSNParams) string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		quotePGConnValue(p.Host), p.Port, quotePGConnValue(p.User),
		quotePGConnValue(p.Password), quotePGConnValue(p.DB), quotePGConnValue(p.SSLMode))
}

func quotePGConnValue(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `'`, `\'`)
	return "'" + v + "'"
}
