package factory

import (
	"context"
	"fmt"
	"slices"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
	"github.com/lychee-technology/forma/internal/uniquekey"
)

// requireUniqueKeysBuilt refuses to start while the write path cannot
// enforce a declared unique key (#651): its tables are missing, or a key is
// not built for its current attributes. It runs where buildSchemaGuards
// does, before any read surface opens, so a refusal has nothing to release.
//
// A database without the state table and a registry that declares no key is
// a deployment that never used unique keys, which needs neither table. With
// the state table present the check runs even when nothing is declared,
// because a key whose declaration was removed is no longer maintained and
// must be dropped before the same declaration could return.
func requireUniqueKeysBuilt(
	ctx context.Context, pool internal.UniqueKeyStore, cfg *forma.Config, tables []string, catalog *internal.UniqueKeyCatalog,
) error {
	entityMain := cfg.Database.TableNames.EntityMain
	keyTable, stateTable := uniquekey.KeyTable(entityMain), uniquekey.StateTable(entityMain)
	hasState := slices.Contains(tables, normalizeTableName(stateTable))
	if !hasState && !catalog.DeclaresKeys() {
		return nil
	}
	for _, table := range []string{keyTable, stateTable} {
		if !slices.Contains(tables, normalizeTableName(table)) {
			return fmt.Errorf("table %s is missing, which the schemas' unique keys need; run forma-tools init-db to create it", table)
		}
	}
	if err := catalog.CheckUniqueKeysBuilt(ctx, pool, entityMain); err != nil {
		return fmt.Errorf("unique key check failed: %w", err)
	}
	return nil
}
