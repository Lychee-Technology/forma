package internal

import "github.com/lychee-technology/forma/internal/sqlutil"

// sanitizeIdentifier delegates to the canonical sqlutil implementation; kept as
// a package-local shorthand for the repository's many call sites.
func sanitizeIdentifier(name string) string {
	return sqlutil.SanitizeIdentifier(name)
}
