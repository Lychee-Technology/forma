package schemavalidate

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// shippedSchemaDir is the real server schema directory. Resolving it is the
// point: two schemas there use cross-file $refs, and a nil Loader (the state
// before #314) cannot resolve them at all.
func shippedSchemaDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.Abs("../../cmd/server/schemas")
	require.NoError(t, err)
	return dir
}

// TestResolveShippedSchemas is the fail-closed contract: every schema the
// server ships must resolve, including the cross-file $refs in visit.json and
// log.json. It fails before the lead.json repair with a dangling-$defs error.
func TestResolveShippedSchemas(t *testing.T) {
	dir := shippedSchemaDir(t)
	for _, name := range []string{"lead", "lead_full", "visit", "visit_full", "log", "log_full"} {
		t.Run(name, func(t *testing.T) {
			_, err := resolveSchemaFile(dir, name+".json")
			require.NoError(t, err, "shipped schema %s must resolve", name)
		})
	}
}

// TestValidateRejectsEnumViolation is the issue's own example: lead.json
// declares status as a four-value enum and "banana" is not one of them.
func TestValidateRejectsEnumViolation(t *testing.T) {
	dir := shippedSchemaDir(t)
	resolved, err := resolveSchemaFile(dir, "lead.json")
	require.NoError(t, err)

	err = resolved.Validate(map[string]any{"status": "banana"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "banana")
}
