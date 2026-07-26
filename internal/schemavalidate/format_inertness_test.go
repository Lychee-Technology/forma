package schemavalidate

// A tripwire rather than a behaviour test: it asserts what the dependency does
// not do, so that a version bump which starts doing it turns red here instead of
// in production.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFormatKeywordStaysInert pins that "format" is annotation-only.
//
// github.com/google/jsonschema-go exposes no assertion switch for format, so
// every format in a shipped schema is documentation today. Create is enforcing
// and has no escape hatch — no report-only mode, no per-schema opt-out — so a
// library that began asserting would start rejecting live uuid, email and
// date-time values on the write path the moment the dependency was bumped.
//
// The values below are malformed for their declared format and well-formed for
// their declared type, so only a format assertion can reject them.
func TestFormatKeywordStaysInert(t *testing.T) {
	const schema = `{
		"type": "object",
		"properties": {
			"at":   {"type": "string", "format": "date-time"},
			"id":   {"type": "string", "format": "uuid"},
			"mail": {"type": "string", "format": "email"}
		}
	}`
	v, err := New(registryWith(t, "ev", schema, 3), t.TempDir())
	require.NoError(t, err)

	err = v.Validate(3, map[string]any{
		"at":   "not-a-timestamp",
		"id":   "not-a-uuid",
		"mail": "not-an-email",
	})
	require.NoError(t, err,
		"format is annotation-only in jsonschema-go; a failure here means the library "+
			"started asserting it, and shipped uuid/email/date-time values would now be "+
			"rejected on create — see docs/error-handling.md before changing this test")
}

// TestShippedSchemasUseFormat keeps the tripwire above load-bearing. If no
// shipped schema declared a format, an assertive library would cost nothing and
// the test would be pinning an irrelevance.
func TestShippedSchemasUseFormat(t *testing.T) {
	dir := shippedSchemaDir(t)

	var withFormat []string
	for _, fileName := range shippedSchemaNames(t, dir) {
		body, err := os.ReadFile(filepath.Join(dir, fileName))
		require.NoError(t, err)
		if strings.Contains(string(body), `"format"`) {
			withFormat = append(withFormat, fileName)
		}
	}

	require.NotEmpty(t, withFormat,
		"no shipped schema declares a format, so the inertness tripwire guards nothing")
}
