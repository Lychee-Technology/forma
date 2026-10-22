package sqlutil

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"unicode/utf8"

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

// MaxIdentifierLen is PostgreSQL's NAMEDATALEN-1. The server truncates a
// longer identifier, quoted or not, so two derived names that differ only
// past it would name one relation, and a derived name of a long enough table
// would name the table itself.
const MaxIdentifierLen = 63

// digestLen is the number of hex characters of a table name's SHA-256 a
// shortened name carries: 64 bits, so long table names that share their
// start do not collide in practice.
const digestLen = 16

// SuffixedIdentifier names a table derived from table by appending suffix to
// its last part, inside the closing quote when it is quoted: "entity_main"
// becomes "entity_main_unique_key". A schema-qualified name keeps its
// qualifier. The last part is bounded to MaxIdentifierLen (see boundedName).
// The unique-key tables and the row-guard table are named this way, so a
// deployment that names its entity_main table names them too.
func SuffixedIdentifier(table, suffix string) string {
	qualifier, name, quoted := splitTableName(table)
	name = boundedName(name, suffix)
	if quoted {
		name = `"` + name + `"`
	}
	return qualifier + name
}

// SuffixedName is SuffixedIdentifier's last part alone, with no qualifier
// and no quotes: the name of an index, which lives in its table's schema.
// The caller quotes it if it needs quoting.
func SuffixedName(table, suffix string) string {
	_, name, _ := splitTableName(table)
	return boundedName(name, suffix)
}

// splitTableName splits a table name into its qualifier, final dot included,
// and its last part without quotes.
func splitTableName(table string) (qualifier, name string, quoted bool) {
	if i := strings.LastIndex(table, "."); i >= 0 {
		qualifier, table = table[:i+1], table[i+1:]
	}
	if len(table) >= 2 && strings.HasPrefix(table, `"`) && strings.HasSuffix(table, `"`) {
		return qualifier, table[1 : len(table)-1], true
	}
	return qualifier, table, false
}

// boundedName joins name and suffix when the result fits an identifier.
// Otherwise the server would truncate it: two tables derived from one long
// name, which differ only in their suffixes, would be one table, and from
// MaxIdentifierLen bytes on a derived name would be name itself. A longer
// join keeps the start of name, then "_" and a digest of all of name, then
// the whole suffix: the suffix keeps one table's derived names apart, the
// digest keeps long table names apart. The digest covers name alone, so
// every spelling of the table, qualified or quoted or neither, derives the
// same names.
func boundedName(name, suffix string) string {
	if len(name)+len(suffix) <= MaxIdentifierLen {
		return name + suffix
	}
	sum := sha256.Sum256([]byte(name))
	digest := "_" + hex.EncodeToString(sum[:])[:digestLen]
	keep := MaxIdentifierLen - len(digest) - len(suffix)
	for keep > 0 && !utf8.RuneStart(name[keep]) {
		keep--
	}
	return name[:keep] + digest + suffix
}
