package internal

import (
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

func tryParseNumber(s string) any {
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return s
}

func sanitizeIdentifier(name string) string {
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
