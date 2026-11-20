package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/uniquekey"
)

// writeAccountKeyedOver writes account.json with the text attributes handle
// and region and unique key 1, handle, over keyAttrs.
func (tg *initDBTarget) writeAccountKeyedOver(t *testing.T, keyAttrs ...string) {
	t.Helper()
	doc := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"handle": map[string]any{"type": "string"}, "region": map[string]any{"type": "string"},
		},
		"x-forma-unique-keys": []any{map[string]any{"id": 1, "name": "handle", "attributes": keyAttrs}},
	}
	attributes := map[string]any{
		"handle": map[string]any{"attributeID": 1, "valueType": "text"},
		"region": map[string]any{"attributeID": 2, "valueType": "text"},
	}
	for file, v := range map[string]any{"account.json": doc, "account_attributes.json": attributes} {
		data, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("marshal %s: %v", file, err)
		}
		if err := os.WriteFile(filepath.Join(tg.opts.schemaDir, file), data, 0o600); err != nil {
			t.Fatalf("write %s: %v", file, err)
		}
	}
}

// keyStateOf counts schema id's entries in the key table and its rows in the
// key build-state table.
func (tg *initDBTarget) keyStateOf(t *testing.T, id int16) (entries, built int) {
	t.Helper()
	err := tg.pool.QueryRow(context.Background(), fmt.Sprintf(
		"SELECT (SELECT COUNT(*) FROM %s WHERE ltbase_schema_id = $1), (SELECT COUNT(*) FROM %s WHERE ltbase_schema_id = $1)",
		quoteIdentifier(uniquekey.KeyTable(tg.opts.entityMain)), quoteIdentifier(uniquekey.StateTable(tg.opts.entityMain))),
		id).Scan(&entries, &built)
	if err != nil {
		t.Fatalf("count schema %d's unique-key state: %v", id, err)
	}
	return entries, built
}

// reserve inserts value's entry of key 1 for row as the write path reserves
// it (INSERT ... ON CONFLICT DO NOTHING), and answers whether it took it.
func (tg *initDBTarget) reserve(t *testing.T, id int16, row uuid.UUID, value string) bool {
	t.Helper()
	var taken int
	err := tg.pool.QueryRow(context.Background(), fmt.Sprintf(`WITH ins AS (
		INSERT INTO %s (ltbase_schema_id, key_id, key_value, ltbase_row_id) VALUES ($1, 1, $2, $3)
		ON CONFLICT (ltbase_schema_id, key_id, key_value) DO NOTHING RETURNING 1) SELECT COUNT(*) FROM ins`,
		quoteIdentifier(uniquekey.KeyTable(tg.opts.entityMain))), id, value, row).Scan(&taken)
	if err != nil {
		t.Fatalf("reserve %q for schema %d: %v", value, id, err)
	}
	return taken == 1
}

// encode answers the key value of a tuple of text attributes.
func (tg *initDBTarget) encode(t *testing.T, texts ...string) string {
	t.Helper()
	components := make([]uniquekey.Component, len(texts))
	for i, text := range texts {
		components[i] = uniquekey.Component{Type: forma.ValueTypeText, Value: text}
	}
	value, err := uniquekey.Encode(components)
	if err != nil {
		t.Fatalf("encode %v: %v", texts, err)
	}
	return value
}

// requireKeysBuilt runs the server's startup check of the unique keys.
func (tg *initDBTarget) requireKeysBuilt(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	registry, err := loadDeclaringRegistry(ctx, tg.pool, tg.opts)
	if err != nil {
		t.Fatalf("load the registry: %v", err)
	}
	catalog, err := internal.NewUniqueKeyCatalog(registry)
	if err != nil {
		t.Fatalf("build the key catalog: %v", err)
	}
	if err := catalog.CheckUniqueKeysBuilt(ctx, tg.pool, tg.opts.entityMain); err != nil {
		t.Fatalf("the startup check refuses the re-registered schema: %v", err)
	}
}

// A dropped schema's unique-key entries and key build state are purged with
// its rows, and init-db refuses to activate the id while any remain. The id
// registered again, under the same key or another declaration of key 1,
// starts with its key built afresh over no rows, passes the startup check,
// and takes a value a row of the dropped schema had reserved.
func TestDropSchemaPurgesUniqueKeyStateBeforeReRegistration(t *testing.T) {
	for _, tc := range []struct {
		name     string
		keyAttrs []string
	}{
		{"same declaration", []string{"handle"}},
		{"another declaration", []string{"handle", "region"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tg := newInitDBTarget(t)
			tg.writeAccountKeyedOver(t, "handle")
			if out, err := tg.run(t); err != nil {
				t.Fatalf("init-db: %v\n%s", err, out)
			}
			const id = firstSchemaID
			old := uuid.New()
			tg.insertHandleRow(t, id, old.String(), "taken")
			if out, err := tg.buildKeys(t, "account", true); err != nil {
				t.Fatalf("rebuild: %v\n%s", err, out)
			}
			if entries, built := tg.keyStateOf(t, id); entries != 1 || built != 1 {
				t.Fatalf("before the drop the key holds %d entries and %d build-state rows, want 1 and 1", entries, built)
			}

			if out, err := tg.drop(t, dropTarget{name: "account"}); err != nil {
				t.Fatalf("drop-schema: %v\n%s", err, out)
			}
			if entries, built := tg.keyStateOf(t, id); entries != 0 || built != 0 || tg.rowsOf(t, id) != 0 {
				t.Fatalf("drop-schema left %d key entries, %d build-state rows and %d rows", entries, built, tg.rowsOf(t, id))
			}

			// A purge interrupted before the key tables: init-db refuses to
			// activate the id, and a rerun of the drop finishes the purge.
			taken := tg.encode(t, "taken")
			if !tg.reserve(t, id, old, taken) {
				t.Fatalf("the purged key table still holds the dropped schema's value")
			}
			if _, err := tg.pool.Exec(context.Background(), fmt.Sprintf(
				`INSERT INTO %s (ltbase_schema_id, key_id, attributes_hash, storage_hash, built_at) VALUES ($1, 1, 'stale', 'stale', 1)`,
				quoteIdentifier(uniquekey.StateTable(tg.opts.entityMain))), id); err != nil {
				t.Fatalf("write a stale build-state row: %v", err)
			}
			tg.writeAccountKeyedOver(t, tc.keyAttrs...)
			out, err := tg.run(t)
			if !errors.Is(err, publication.ErrResidue) || !strings.Contains(err.Error(), uniquekey.KeyTable(tg.opts.entityMain)) {
				t.Fatalf("init-db activated the id over the dropped schema's key entries: %v\n%s", err, out)
			}
			if _, ok := tg.records(t)["account"]; ok {
				t.Fatalf("the refused registration of account committed")
			}
			if out, err := tg.drop(t, dropTarget{id: id}); err != nil {
				t.Fatalf("resume drop-schema: %v\n%s", err, out)
			}
			if entries, built := tg.keyStateOf(t, id); entries != 0 || built != 0 {
				t.Fatalf("the resumed drop left %d key entries and %d build-state rows", entries, built)
			}

			out, err = tg.run(t)
			if err != nil {
				t.Fatalf("init-db re-registers account: %v\n%s", err, out)
			}
			if rec, _ := tg.record(t, id); rec.State != publication.Unpublished {
				t.Fatalf("the re-registered id has record %+v, want UNPUBLISHED", rec)
			}
			if !strings.Contains(out, "Built unique key handle of schema account (no rows to scan)") {
				t.Errorf("init-db did not build the re-registered schema's key afresh:\n%s", out)
			}
			tg.requireKeysBuilt(t)

			value := taken
			if len(tc.keyAttrs) == 2 {
				value = tg.encode(t, "taken", "eu")
			}
			if !tg.reserve(t, id, uuid.New(), value) {
				t.Fatalf("the re-registered schema cannot take %q, which a dropped row had reserved", value)
			}
		})
	}
}
