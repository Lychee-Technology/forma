package schemameta

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
)

// outboxAttributes is the attributes file the declaration tests load: three
// column-bound attributes, one EAV attribute, a retired bound one, and one
// bound to the row-id key column.
func outboxAttributes() map[string]any {
	bound := func(id float64, valueType, col string) map[string]any {
		return map[string]any{"attributeID": id, "valueType": valueType, "column_binding": map[string]any{"col_name": col}}
	}
	retired := bound(6, "text", "text_03")
	retired["retired"] = true
	return map[string]any{
		"status":          bound(1, "text", "text_01"),
		"next_attempt_at": bound(2, "datetime", "bigint_01"),
		"partition_key":   bound(3, "text", "text_02"),
		"payload":         map[string]any{"attributeID": float64(4), "valueType": "text"},
		"message_id":      bound(5, "uuid", "ltbase_row_id"),
		"old_status":      retired,
	}
}

// loadOutboxRegistry writes outbox_message.json declaring indexes and loads it
// through the directory registry, the same load path the server takes.
func loadOutboxRegistry(t *testing.T, indexes any) (forma.SchemaRegistry, error) {
	t.Helper()
	dir := t.TempDir()
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"status":          map[string]any{"type": "string"},
			"next_attempt_at": map[string]any{"type": "string", "format": "date-time"},
			"partition_key":   map[string]any{"type": "string"},
			"payload":         map[string]any{"type": "string"},
		},
	}
	if indexes != nil {
		schema[IndexDeclarationsKey] = indexes
	}
	writeJSONFile(t, filepath.Join(dir, "outbox_message.json"), schema)
	writeJSONFile(t, filepath.Join(dir, "outbox_message_attributes.json"), outboxAttributes())
	return NewFileSchemaRegistryFromDirectory(dir)
}

func index(name string, attrs ...any) map[string]any {
	return map[string]any{"name": name, "attributes": attrs}
}

func TestIndexDeclarations_ParsedInFileOrder(t *testing.T) {
	registry, err := loadOutboxRegistry(t, []any{
		index("due", "status", "next_attempt_at"),
		index("fifo", "partition_key"),
	})
	require.NoError(t, err)

	_, schema, err := registry.GetSchemaByName("outbox_message")
	require.NoError(t, err)
	assert.Equal(t, []forma.IndexDeclaration{
		{Name: "due", Attributes: []string{"status", "next_attempt_at"}},
		{Name: "fifo", Attributes: []string{"partition_key"}},
	}, schema.Indexes)
}

func TestIndexDeclarations_AbsentKeyDeclaresNothing(t *testing.T) {
	registry, err := loadOutboxRegistry(t, nil)
	require.NoError(t, err)
	_, schema, err := registry.GetSchemaByName("outbox_message")
	require.NoError(t, err)
	assert.Nil(t, schema.Indexes)
}

// Every refused declaration fails the schema load, and the message names the
// schema, the index and the offending attribute(s), so the author can find the
// line without re-reading the rules.
func TestIndexDeclarations_RefusedAtSchemaLoad(t *testing.T) {
	cases := []struct {
		name    string
		indexes any
		want    []string
	}{
		{"missing name", []any{map[string]any{"attributes": []any{"status"}}},
			[]string{"x-forma-indexes entry 0", "[status]", "has no name"}},
		{"empty name", []any{index("", "status")},
			[]string{"x-forma-indexes entry 0", "[status]", "has no name"}},
		{"name not a string", []any{map[string]any{"name": float64(7), "attributes": []any{"status"}}},
			[]string{"x-forma-indexes entry 0", "[status]", "a number"}},
		{"name breaks the pattern", []any{index("Due-Scan", "status")},
			[]string{`index "Due-Scan"`, "[status]", "^[a-z][a-z0-9_]{0,30}$"}},
		{"name longer than 31 bytes", []any{index("a234567890123456789012345678901x", "status")},
			[]string{"a234567890123456789012345678901x", "[status]", "invalid name"}},
		{"duplicate name", []any{index("due", "status"), index("due", "next_attempt_at")},
			[]string{"index due", "[next_attempt_at]", "declared more than once"}},
		{"no attributes", []any{map[string]any{"name": "due", "attributes": []any{}}},
			[]string{"index due", "lists 0 attributes []", "1 to 4"}},
		{"five attributes", []any{index("due", "a", "b", "c", "d", "e")},
			[]string{"index due", "lists 5 attributes [a, b, c, d, e]", "1 to 4"}},
		{"duplicate attribute", []any{index("due", "status", "next_attempt_at", "status")},
			[]string{"index due", "attribute status is listed more than once"}},
		{"attribute not a string", []any{index("due", "status", true)},
			[]string{"index due", "attribute 1", "a boolean"}},
		{"attributes not an array", []any{map[string]any{"name": "due", "attributes": "status"}},
			[]string{"index due: attributes must be an array of attribute names", "a string"}},
		{"unsupported key", []any{map[string]any{"name": "due", "attributes": []any{"status"}, "unique": true}},
			[]string{"index due", "[status]", `unsupported key "unique"`}},
		{"declarations not an array", map[string]any{"due": []any{"status"}},
			[]string{"x-forma-indexes must be an array", "an object"}},
		{"entry not an object", []any{"status"},
			[]string{"x-forma-indexes entry 0", "must be an object", "a string"}},
		{"unknown attribute", []any{index("due", "status", "nope")},
			[]string{"index due: attribute nope is not defined", "only column-bound attributes can be indexed"}},
		{"EAV attribute", []any{index("due", "status", "payload")},
			[]string{"index due: attribute payload is stored in EAV", "only column-bound attributes can be indexed"}},
		{"retired attribute", []any{index("due", "old_status")},
			[]string{"index due: attribute old_status is retired"}},
		{"attribute bound to a key column", []any{index("due", "message_id")},
			[]string{"index due: attribute message_id is bound to ltbase_row_id", "key column"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadOutboxRegistry(t, tc.indexes)
			require.Error(t, err)
			msg := err.Error()
			assert.Contains(t, msg, "schema outbox_message")
			assert.Contains(t, msg, "outbox_message.json", "the error names the schema file to edit")
			for _, want := range tc.want {
				assert.Contains(t, msg, want)
			}
		})
	}
}

// The binding checks see the attributes file: a binding to a column
// entity_main lacks is reported as that, not through the index naming it.
func TestIndexDeclarations_BrokenBindingReportedFirst(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "log.json"), map[string]any{
		"type":               "object",
		"properties":         map[string]any{"leadId": map[string]any{"type": "string"}},
		IndexDeclarationsKey: []any{index("by_lead", "leadId")},
	})
	writeJSONFile(t, filepath.Join(dir, "log_attributes.json"), map[string]any{
		"leadId": map[string]any{"attributeID": float64(4), "valueType": "text",
			"column_binding": map[string]any{"col_name": "text_99"}},
	})
	_, err := NewFileSchemaRegistryFromDirectory(dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown main column text_99")
	assert.NotContains(t, err.Error(), "by_lead")
}
