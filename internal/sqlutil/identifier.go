package sqlutil

import (
	"strings"

	"github.com/jackc/pgx/v5"
)

func SanitizeIdentifier(name string) string {
	if name == "" {
		return ""
	}
	parts := strings.Split(name, ".")
	clean := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.Trim(part, " \"")
		if trimmed == "" {
			continue
		}
		clean = append(clean, trimmed)
	}
	if len(clean) == 0 {
		clean = []string{name}
	}
	return pgx.Identifier(clean).Sanitize()
}

// SuffixedIdentifier names a table derived from table by appending suffix to
// its last part, inside the closing quote when it is quoted: "entity_main"
// becomes "entity_main_unique_key". A schema-qualified name keeps its
// qualifier. The row-guard table is named this way, so a deployment that
// names its entity_main table names it too. The unique-key tables are named
// by uniquekey, which also bounds a name to PostgreSQL's identifier limit.
func SuffixedIdentifier(table, suffix string) string {
	if quoted, ok := strings.CutSuffix(table, `"`); ok {
		return quoted + suffix + `"`
	}
	return table + suffix
}
