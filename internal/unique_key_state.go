package internal

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/uniquekey"
)

// UniqueKeyStore is the database surface the unique-key build and the
// startup check use; *pgxpool.Pool satisfies it. Every statement runs on its
// own, so no transaction outgrows a bounded batch.
type UniqueKeyStore interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// DeclaresKeys says whether any schema declares a unique key.
func (c *UniqueKeyCatalog) DeclaresKeys() bool {
	return len(c.keyedSchemas()) > 0
}

// unbuiltKey is one key whose entries the write path cannot trust: declared
// but not built, declared over other attributes than it was built for, built
// while its attributes were stored elsewhere, or built but no longer
// declared.
// A key that is no longer declared stops being maintained, so a later
// declaration of the same id over the same attributes would read as built
// over stale entries; the startup check refuses it until a build drops it.
type unbuiltKey struct {
	schema string
	// key is "" for a key the schema no longer declares.
	key    string
	keyID  int16
	reason string
	// changed says the declaration lists other attributes than the key was
	// built for, which no build resolves: a key's attributes never change
	// under its id (#651).
	changed bool
}

const changedKeyAdvice = "a key's attributes cannot change under its id; declare the new attribute list under a new id"

// command is the build-unique-keys invocation that resolves k.
func (k unbuiltKey) command() string {
	if k.key == "" {
		return "forma-tools build-unique-keys --schema " + k.schema
	}
	return fmt.Sprintf("forma-tools build-unique-keys --schema %s --key %s", k.schema, k.key)
}

func (k unbuiltKey) String() string {
	if k.changed {
		return fmt.Sprintf("unique key %s of schema %s (id %d) %s: %s", k.key, k.schema, k.keyID, k.reason, changedKeyAdvice)
	}
	if k.key == "" {
		return fmt.Sprintf("schema %s %s (key id %d): run %s", k.schema, k.reason, k.keyID, k.command())
	}
	return fmt.Sprintf("unique key %s of schema %s %s: run %s", k.key, k.schema, k.reason, k.command())
}

// builtKey is what the state table records of one built key: the
// uniquekey.AttributesHash and uniquekey.StorageHash it was built for.
type builtKey struct {
	attributes string
	storage    string
}

// readUniqueKeyState answers the built keys the state table records, by
// (schema, key).
func readUniqueKeyState(ctx context.Context, db UniqueKeyStore, entityMain string) (map[[2]int16]builtKey, error) {
	stateTable := uniquekey.StateTable(entityMain)
	rows, err := db.Query(ctx, fmt.Sprintf("SELECT ltbase_schema_id, key_id, attributes_hash, storage_hash FROM %s", sanitizeIdentifier(stateTable)))
	if err != nil {
		return nil, fmt.Errorf("read built unique keys from %s (forma-tools init-db creates it): %w", stateTable, err)
	}
	built := map[[2]int16]builtKey{}
	var schemaID, keyID int16
	var state builtKey
	if _, err := pgx.ForEachRow(rows, []any{&schemaID, &keyID, &state.attributes, &state.storage}, func() error {
		built[[2]int16{schemaID, keyID}] = state
		return nil
	}); err != nil {
		return nil, fmt.Errorf("read built unique keys from %s: %w", stateTable, err)
	}
	return built, nil
}

// unbuiltKeys answers every key of a registered schema the write path cannot
// enforce yet, ordered by schema ID, then key ID. A state row of a schema the
// registry does not hold is left alone: no writer of this registry can write
// that schema's rows.
func (c *UniqueKeyCatalog) unbuiltKeys(ctx context.Context, db UniqueKeyStore, entityMain string) ([]unbuiltKey, error) {
	built, err := readUniqueKeyState(ctx, db, entityMain)
	if err != nil {
		return nil, fmt.Errorf("read which unique keys are built: %w", err)
	}
	var unbuilt []unbuiltKey
	for _, schemaID := range c.registeredSchemas() {
		specs := c.keysOf(schemaID)
		for _, spec := range specs {
			state, ok := built[[2]int16{schemaID, spec.id}]
			switch {
			case !ok:
				unbuilt = append(unbuilt, unbuiltKey{schema: c.names[schemaID], key: spec.name, keyID: spec.id, reason: "is not built"})
			case state.attributes != spec.hash:
				unbuilt = append(unbuilt, unbuiltKey{schema: c.names[schemaID], key: spec.name, keyID: spec.id,
					reason: "was built for another attribute list", changed: true})
			case state.storage != spec.storage:
				unbuilt = append(unbuilt, unbuiltKey{schema: c.names[schemaID], key: spec.name, keyID: spec.id,
					reason: "was built while its attributes were stored elsewhere (a column binding or attribute id changed)"})
			}
		}
		for _, keyID := range builtKeyIDs(built, schemaID) {
			if !slices.ContainsFunc(specs, func(s uniqueKeySpec) bool { return s.id == keyID }) {
				unbuilt = append(unbuilt, unbuiltKey{schema: c.names[schemaID], keyID: keyID,
					reason: "no longer declares a unique key that is still built"})
			}
		}
	}
	return unbuilt, nil
}

// builtKeyIDs answers the key IDs of schemaID that built records, ascending.
func builtKeyIDs(built map[[2]int16]builtKey, schemaID int16) []int16 {
	var ids []int16
	for k := range built {
		if k[0] == schemaID {
			ids = append(ids, k[1])
		}
	}
	slices.Sort(ids)
	return ids
}

// CheckUniqueKeysBuilt is the startup refusal of #651: it fails while any
// registered schema has a key the write path cannot enforce, naming each
// schema and key and the build-unique-keys command that builds it. Every
// writer must be stopped while that command runs.
func (c *UniqueKeyCatalog) CheckUniqueKeysBuilt(ctx context.Context, db UniqueKeyStore, entityMain string) error {
	unbuilt, err := c.unbuiltKeys(ctx, db, entityMain)
	if err != nil {
		return fmt.Errorf("check the unique keys are built: %w", err)
	}
	if len(unbuilt) == 0 {
		return nil
	}
	lines := make([]string, len(unbuilt))
	for i, k := range unbuilt {
		lines[i] = k.String()
	}
	return fmt.Errorf("unique keys are not built; stop every writer, then: %s", strings.Join(lines, "; "))
}

// BuildUniqueKeysOfEmptySchemas builds every unbuilt key of a registered
// schema that has no live row, which takes no scan, so init-db leaves a
// fresh schema ready to serve. It answers the keys it built and, as the
// startup check words them, those still unbuilt: keys of schemas with rows,
// which need build-unique-keys, and every key of a schema that declares a
// changed key, which needs a new id.
func (c *UniqueKeyCatalog) BuildUniqueKeysOfEmptySchemas(
	ctx context.Context, db UniqueKeyStore, tables model.StorageTables,
) (built []UniqueKeyBuildResult, pending []string, err error) {
	unbuilt, err := c.unbuiltKeys(ctx, db, tables.EntityMain)
	if err != nil {
		return nil, nil, err
	}
	var schemas []string
	for _, k := range unbuilt {
		if !slices.Contains(schemas, k.schema) {
			schemas = append(schemas, k.schema)
		}
	}
	for _, schema := range schemas {
		ofSchema := slices.DeleteFunc(slices.Clone(unbuilt), func(k unbuiltKey) bool { return k.schema != schema })
		buildable := !slices.ContainsFunc(ofSchema, func(k unbuiltKey) bool { return k.changed })
		if buildable {
			if buildable, err = c.schemaIsEmpty(ctx, db, tables.EntityMain, schema); err != nil {
				return built, pending, err
			}
		}
		if !buildable {
			for _, k := range ofSchema {
				pending = append(pending, k.String())
			}
			continue
		}
		report, err := c.BuildUniqueKeys(ctx, db, tables, UniqueKeyBuildRequest{Schema: schema})
		if err != nil {
			return built, pending, fmt.Errorf("build the unique keys of empty schema %s: %w", schema, err)
		}
		built = append(built, report.Keys...)
	}
	return built, pending, nil
}

// schemaIsEmpty says whether schema has no live row.
func (c *UniqueKeyCatalog) schemaIsEmpty(ctx context.Context, db UniqueKeyStore, entityMain, schema string) (bool, error) {
	schemaID, _ := c.schemaIDNamed(schema)
	query := fmt.Sprintf("SELECT NOT EXISTS (SELECT 1 FROM %s WHERE ltbase_schema_id = $1 AND ltbase_deleted_at IS NULL)",
		sanitizeIdentifier(entityMain))
	var empty bool
	if err := db.QueryRow(ctx, query, schemaID).Scan(&empty); err != nil {
		return false, fmt.Errorf("check whether schema %s has live rows: %w", schema, err)
	}
	return empty, nil
}

// registeredSchemas answers the IDs of every schema the registry holds,
// ascending.
func (c *UniqueKeyCatalog) registeredSchemas() []int16 {
	if c == nil {
		return nil
	}
	ids := make([]int16, 0, len(c.names))
	for id := range c.names {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

// schemaIDNamed answers the ID of the registered schema called name.
func (c *UniqueKeyCatalog) schemaIDNamed(name string) (int16, bool) {
	if c == nil {
		return 0, false
	}
	for id, n := range c.names {
		if n == name {
			return id, true
		}
	}
	return 0, false
}
