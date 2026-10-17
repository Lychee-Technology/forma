package internal

import (
	"context"
	"fmt"
	"slices"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/uniquekey"
)

// Unique-key enforcement in the write cores (#651). Every write of a row of a
// keyed schema runs in three steps inside its transaction, after the row
// locks:
//
//  1. plan: compute the entries the row holds once written and diff them
//     against the entries it holds now;
//  2. reserve: insert each new entry, refusing one another row holds with
//     ErrUniqueConflict and removing the entries it inserted, before any
//     entity write of the operation (see reserveUniqueKeys);
//  3. apply: after the entity write, delete the entries the row released.
//
// Writers of one value serialize on the key table's primary key, not on a
// lock of their own: an insert that meets another transaction's uncommitted
// entry waits for that transaction to end. A schema without keys sends none
// of these statements and never names the key table.

// uniqueKeyClaim is what one row's write does to the key table.
type uniqueKeyClaim struct {
	schemaID int16
	rowID    uuid.UUID
	take     []uniqueKeyEntry
	release  []uniqueKeyEntry
}

func (c *uniqueKeyClaim) empty() bool {
	return c == nil || (len(c.take) == 0 && len(c.release) == 0)
}

// planUniqueKeys answers the claim of writing record, nil for a schema
// without keys. record must already carry the creation stamp the row is
// stored with, since a key may name an attribute bound to ltbase_created_at.
// stored says the row exists, so its current entries are read and diffed; a
// created row holds none.
func (r *DBPersistentRecordRepository) planUniqueKeys(
	ctx context.Context, tx pgx.Tx, tables model.StorageTables, record *model.PersistentRecord, stored bool,
) (*uniqueKeyClaim, error) {
	specs := r.uniqueKeys.keysOf(record.SchemaID)
	if len(specs) == 0 {
		return nil, nil
	}
	desired, err := uniqueKeyEntriesOf(specs, record)
	if err != nil {
		return nil, fmt.Errorf("compute the unique keys of row %s: %w", record.RowID, err)
	}
	claim := &uniqueKeyClaim{schemaID: record.SchemaID, rowID: record.RowID}
	if !stored {
		claim.take = desired
		return claim, nil
	}
	held, err := heldUniqueKeys(ctx, tx, tables, specs, record.SchemaID, record.RowID)
	if err != nil {
		return nil, fmt.Errorf("read the unique keys row %s holds: %w", record.RowID, err)
	}
	claim.take = entriesMissingFrom(desired, held)
	claim.release = entriesMissingFrom(held, desired)
	return claim, nil
}

// entriesMissingFrom answers the entries of from that other lacks, in a new
// slice: neither argument is modified.
func entriesMissingFrom(from, other []uniqueKeyEntry) []uniqueKeyEntry {
	var missing []uniqueKeyEntry
	for _, e := range from {
		if !slices.ContainsFunc(other, func(o uniqueKeyEntry) bool { return o.keyID == e.keyID && o.value == e.value }) {
			missing = append(missing, e)
		}
	}
	return missing
}

// heldUniqueKeys reads the entries of the declared keys that the row holds.
func heldUniqueKeys(ctx context.Context, tx pgx.Tx, tables model.StorageTables, specs []uniqueKeySpec, schemaID int16, rowID uuid.UUID) ([]uniqueKeyEntry, error) {
	query := fmt.Sprintf("SELECT key_id, key_value FROM %s WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2",
		sanitizeIdentifier(uniquekey.KeyTable(tables.EntityMain)))
	rows, err := tx.Query(ctx, query, schemaID, rowID)
	if err != nil {
		return nil, fmt.Errorf("read unique key entries of row %s: %w", rowID, err)
	}
	defer rows.Close()
	var held []uniqueKeyEntry
	for rows.Next() {
		var entry uniqueKeyEntry
		if err := rows.Scan(&entry.keyID, &entry.value); err != nil {
			return nil, fmt.Errorf("scan unique key entry of row %s: %w", rowID, err)
		}
		i := slices.IndexFunc(specs, func(s uniqueKeySpec) bool { return s.id == entry.keyID })
		if i < 0 {
			continue // a key the schema no longer declares
		}
		entry.keyName = specs[i].name
		held = append(held, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read unique key entries of row %s: %w", rowID, err)
	}
	return held, nil
}

// uniqueKeyConflict is the ErrUniqueConflict of a key value another row
// holds. The published message names the schema and the key, never the
// value, which may be sensitive (#651).
func (r *DBPersistentRecordRepository) uniqueKeyConflict(c claimedEntry, owner uuid.UUID) error {
	return forma.WithOperatorDetail(r.uniqueKeyConflictMessage(c),
		fmt.Errorf("key id %d is held by row %s; the write of row %s wanted it", c.entry.keyID, owner, c.rowID))
}

// batchUniqueKeyConflict is the ErrUniqueConflict of two operations of one
// batch that take the same key value.
func (r *DBPersistentRecordRepository) batchUniqueKeyConflict(c claimedEntry, other uuid.UUID) error {
	return forma.WithOperatorDetail(r.uniqueKeyConflictMessage(c),
		fmt.Errorf("the batch's writes of rows %s and %s both take one value of key id %d", other, c.rowID, c.entry.keyID))
}

func (r *DBPersistentRecordRepository) uniqueKeyConflictMessage(c claimedEntry) error {
	return forma.UniqueConflictf("schema %s already has a row with this value of unique key %s",
		r.uniqueKeys.schemaName(c.schemaID), c.entry.keyName)
}

// rowIDConflict is the ErrUniqueConflict of a create whose row ID a live row
// already has.
func (r *DBPersistentRecordRepository) rowIDConflict(schemaID int16, rowID uuid.UUID) error {
	return forma.UniqueConflictf("schema %s already has a row with ID %s", r.uniqueKeys.schemaName(schemaID), rowID)
}

// applyUniqueKeys finishes a reserved claim after the entity write: it
// deletes the entries the row released. The reservation inserted the taken
// ones.
func (r *DBPersistentRecordRepository) applyUniqueKeys(ctx context.Context, tx pgx.Tx, tables model.StorageTables, claim *uniqueKeyClaim) error {
	if claim.empty() {
		return nil
	}
	release := fmt.Sprintf("DELETE FROM %s WHERE ltbase_schema_id = $1 AND key_id = $2 AND key_value = $3 AND ltbase_row_id = $4",
		sanitizeIdentifier(uniquekey.KeyTable(tables.EntityMain)))
	for _, entry := range claim.release {
		if _, err := tx.Exec(ctx, release, claim.schemaID, entry.keyID, entry.value, claim.rowID); err != nil {
			return fmt.Errorf("release unique key %s of row %s: %w", entry.keyName, claim.rowID, err)
		}
	}
	return nil
}

// releaseRowUniqueKeys deletes every entry a deleted row held, so its values
// can be taken again once the delete commits. A schema without keys sends
// nothing.
func (r *DBPersistentRecordRepository) releaseRowUniqueKeys(ctx context.Context, tx pgx.Tx, tables model.StorageTables, schemaID int16, rowID uuid.UUID) error {
	if len(r.uniqueKeys.keysOf(schemaID)) == 0 {
		return nil
	}
	query := fmt.Sprintf("DELETE FROM %s WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2",
		sanitizeIdentifier(uniquekey.KeyTable(tables.EntityMain)))
	if _, err := tx.Exec(ctx, query, schemaID, rowID); err != nil {
		return fmt.Errorf("release unique keys of row %s: %w", rowID, err)
	}
	return nil
}
