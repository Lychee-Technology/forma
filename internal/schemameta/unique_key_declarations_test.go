package schemameta

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
)

// loadKeyedRegistry writes outbox_message.json declaring unique keys and
// loads it through the directory registry, the load path the server takes.
// Besides outboxAttributes it carries a list attribute and an attribute
// inside an array of objects, which the attributes file names by its dotted
// path alone.
func loadKeyedRegistry(t *testing.T, keys any) (forma.SchemaRegistry, error) {
	t.Helper()
	dir := t.TempDir()
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"status":  map[string]any{"type": "string"},
			"payload": map[string]any{"type": "string"},
			"tags":    map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			"seen_at": map[string]any{"type": "string", "format": "date-time"},
			"opened":  map[string]any{"type": "string", "format": "date-time"},
			"lines": map[string]any{"type": "array", "items": map[string]any{
				"type": "object", "properties": map[string]any{"sku": map[string]any{"type": "string"}},
			}},
		},
	}
	if keys != nil {
		schema[UniqueKeyDeclarationsKey] = keys
	}
	attributes := outboxAttributes()
	attributes["tags"] = map[string]any{"attributeID": float64(20), "valueType": "list", "items_type": "text"}
	attributes["lines.sku"] = map[string]any{"attributeID": float64(21), "valueType": "text"}
	attributes["seen_at"] = map[string]any{"attributeID": float64(22), "valueType": "datetime",
		"column_binding": map[string]any{"col_name": "ltbase_updated_at"}}
	attributes["opened"] = map[string]any{"attributeID": float64(23), "valueType": "datetime",
		"column_binding": map[string]any{"col_name": "ltbase_created_at"}}
	writeJSONFile(t, filepath.Join(dir, "outbox_message.json"), schema)
	writeJSONFile(t, filepath.Join(dir, "outbox_message_attributes.json"), attributes)
	return NewFileSchemaRegistryFromDirectory(dir)
}

func uniqueKey(id any, name string, attrs ...any) map[string]any {
	return map[string]any{"id": id, "name": name, "attributes": attrs}
}

func TestUniqueKeyDeclarations_ParsedInFileOrder(t *testing.T) {
	registry, err := loadKeyedRegistry(t, []any{
		uniqueKey(2, "natural", "status", "payload"),
		uniqueKey(1, "idempotency", "partition_key"),
	})
	require.NoError(t, err)

	_, schema, err := registry.GetSchemaByName("outbox_message")
	require.NoError(t, err)
	assert.Equal(t, []forma.UniqueKeyDeclaration{
		{ID: 2, Name: "natural", Attributes: []string{"status", "payload"}},
		{ID: 1, Name: "idempotency", Attributes: []string{"partition_key"}},
	}, schema.UniqueKeys)
}

func TestUniqueKeyDeclarations_AbsentKeyDeclaresNothing(t *testing.T) {
	registry, err := loadKeyedRegistry(t, nil)
	require.NoError(t, err)
	_, schema, err := registry.GetSchemaByName("outbox_message")
	require.NoError(t, err)
	assert.Nil(t, schema.UniqueKeys)
}

// Every refused declaration fails the schema load with a message naming the
// schema, the key and, where one is at fault, the attribute.
func TestUniqueKeyDeclarations_RefusedAtSchemaLoad(t *testing.T) {
	cases := []struct {
		name string
		keys any
		want []string
	}{
		{"declarations not an array", map[string]any{"natural": []any{"status"}},
			[]string{"x-forma-unique-keys must be an array", "an object"}},
		{"declarations null", json.RawMessage("null"),
			[]string{"x-forma-unique-keys must be an array", "got null"}},
		{"entry not an object", []any{"status"},
			[]string{"x-forma-unique-keys entry 0", "must be an object", "a string"}},
		{"missing name", []any{map[string]any{"id": 1, "attributes": []any{"status"}}},
			[]string{"x-forma-unique-keys entry 0", "[status]", "needs a name matching"}},
		{"name breaks the pattern", []any{uniqueKey(1, "Natural-Key", "status")},
			[]string{"unique key Natural-Key", "[status]", "^[a-z][a-z0-9_]{0,30}$"}},
		{"missing id", []any{map[string]any{"name": "natural", "attributes": []any{"status"}}},
			[]string{"unique key natural", "[status]", "id must be an integer from 1 to 32767", "got null"}},
		{"id zero", []any{uniqueKey(0, "natural", "status")},
			[]string{"unique key natural", "[status]", "id must be an integer from 1 to 32767, got 0"}},
		{"id above smallint", []any{uniqueKey(32768, "natural", "status")},
			[]string{"unique key natural", "got 32768"}},
		{"id not whole", []any{uniqueKey(1.5, "natural", "status")},
			[]string{"unique key natural", "got 1.5"}},
		{"id a string", []any{uniqueKey("1", "natural", "status")},
			[]string{"unique key natural", "id must be an integer", "a string"}},
		{"duplicate id", []any{uniqueKey(1, "natural", "status"), uniqueKey(1, "other", "payload")},
			[]string{"unique key other", "[payload]", "has id 1, which unique key natural already has"}},
		{"duplicate name", []any{uniqueKey(1, "natural", "status"), uniqueKey(2, "natural", "payload")},
			[]string{"unique key natural", "[payload]", "declared more than once"}},
		{"no attributes", []any{map[string]any{"id": 1, "name": "natural", "attributes": []any{}}},
			[]string{"unique key natural", "lists 0 attributes []", "1 to 4"}},
		{"five attributes", []any{uniqueKey(1, "natural", "a", "b", "c", "d", "e")},
			[]string{"unique key natural", "lists 5 attributes [a, b, c, d, e]", "1 to 4"}},
		{"duplicate attribute", []any{uniqueKey(1, "natural", "status", "status")},
			[]string{"unique key natural", "attribute status is listed more than once"}},
		{"unsupported key", []any{map[string]any{"id": 1, "name": "natural", "attributes": []any{"status"}, "partial": true}},
			[]string{"unique key natural", "[status]", `unsupported key "partial"`}},
		{"unknown attribute", []any{uniqueKey(1, "natural", "status", "nope")},
			[]string{"unique key natural: attribute nope is not defined"}},
		{"retired attribute", []any{uniqueKey(1, "natural", "old_status")},
			[]string{"unique key natural: attribute old_status is retired"}},
		{"list attribute", []any{uniqueKey(1, "natural", "tags")},
			[]string{"unique key natural: attribute tags is a list"}},
		{"attribute inside an array", []any{uniqueKey(1, "natural", "status", "lines.sku")},
			[]string{"unique key natural: attribute lines.sku is inside an array"}},
		{"attribute bound to ltbase_updated_at", []any{uniqueKey(1, "natural", "status", "seen_at")},
			[]string{"unique key natural: attribute seen_at is bound to ltbase_updated_at, which Forma rewrites"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadKeyedRegistry(t, tc.keys)
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

// Keys may span column-bound and EAV attributes of any scalar type, the
// column bound to the row ID included.
func TestUniqueKeyDeclarations_AcceptEveryScalarStorage(t *testing.T) {
	_, err := loadKeyedRegistry(t, []any{
		uniqueKey(1, "mixed", "status", "payload", "next_attempt_at", "message_id"),
		uniqueKey(2, "flags", "archived", "acked"),
		uniqueKey(3, "opened", "opened"),
	})
	require.NoError(t, err)
}
