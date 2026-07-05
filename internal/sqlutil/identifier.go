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
