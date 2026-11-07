package schemameta

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestListInitDBSchemaNamesOrdersByFileName pins the order that fixes a first
// init-db run's ids: by file name, as before #643, not by the trimmed name
// ("-" sorts before ".", so lead-v2.json comes before lead.json).
func TestListInitDBSchemaNamesOrdersByFileName(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"lead.json", "lead-v2.json", "lead_attributes.json", "account.json"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(`{}`), 0o644))
	}

	got, err := ListInitDBSchemaNames(dir)
	require.NoError(t, err)
	require.Equal(t, []string{"account", "lead-v2", "lead"}, got)
}

// TestListInitDBSchemaNamesKeepsFullSchemas pins the one difference from the
// directory registry: init-db registers *_full.json, which
// NewFileSchemaRegistryFromDirectory leaves out. Metadata files, directories
// and non-JSON files are left out by both.
func TestListInitDBSchemaNamesKeepsFullSchemas(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{
		"lead.json", "lead_attributes.json",
		"lead_full.json", "lead_full_attributes.json",
		"README.md",
	} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(`{}`), 0o644))
	}
	require.NoError(t, os.Mkdir(filepath.Join(dir, "nested.json"), 0o755))

	got, err := ListInitDBSchemaNames(dir)
	require.NoError(t, err)
	require.Equal(t, []string{"lead", "lead_full"}, got)
}

func TestListInitDBSchemaNamesReportsUnreadableDirectory(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "absent")
	_, err := ListInitDBSchemaNames(missing)
	require.ErrorContains(t, err, missing)
}
