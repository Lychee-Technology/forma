package internal

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/uniquekey"
)

// maxReserveRounds bounds reserveUniqueKeys' retries of entries whose owner
// was gone by the time it looked: each round needs another transaction to
// release the value between this one's insert and its lookup.
const maxReserveRounds = 3

// claimedEntry is one entry a write takes, with the row that takes it.
type claimedEntry struct {
	schemaID int16
	rowID    uuid.UUID
	entry    uniqueKeyEntry
}

func sameEntry(a, b claimedEntry) bool { return a.key() == b.key() }

// entryKey identifies a key-table entry by its primary key.
type entryKey struct {
	schemaID, keyID int16
	value           string
}

func (c claimedEntry) key() entryKey { return entryKey{c.schemaID, c.entry.keyID, c.entry.value} }

// uniqueKeyReservation is the entries a reservation inserted, which undo
// removes when the operation fails after reserving.
type uniqueKeyReservation struct {
	inserted []claimedEntry
}

// reserveUniqueKeys inserts every entry the claims take. It refuses an entry
// another row holds, or that two claims of one batch both take, with
// ErrUniqueConflict, and then leaves none of the claims' entries behind.
//
// The entries go in one INSERT ... ON CONFLICT DO NOTHING, sorted by (schema,
// key, value). An insert that meets another transaction's uncommitted entry
// waits for that transaction to end, so writers of one value serialize on the
// key table's primary key: the loser finds the winner's committed entry, and
// a waiter for a rolled-back writer takes the value. The sort keeps two
// operations taking overlapping values from waiting on each other. It needs
// READ COMMITTED: a snapshot level answers 40001 where the entry's owner
// committed after the snapshot.
func (r *DBPersistentRecordRepository) reserveUniqueKeys(
	ctx context.Context, tx pgx.Tx, tables model.StorageTables, claims ...*uniqueKeyClaim,
) (*uniqueKeyReservation, error) {
	pending, err := r.takenEntries(claims)
	if err != nil {
		return nil, fmt.Errorf("reserve unique keys: %w", err)
	}
	reservation := &uniqueKeyReservation{}
	for round := 0; len(pending) > 0; round++ {
		if round == maxReserveRounds {
			return nil, r.undoAfter(ctx, tx, tables, reservation,
				fmt.Errorf("unique key %s: its value was released and taken again %d times while this write took it", pending[0].entry.keyName, round))
		}
		inserted, err := insertKeyEntries(ctx, tx, tables, pending)
		if err != nil {
			return nil, fmt.Errorf("reserve unique keys: %w", err)
		}
		reservation.inserted = append(reservation.inserted, inserted...)
		missing := entriesNotIn(pending, inserted)
		if len(missing) == 0 {
			return reservation, nil
		}
		pending, err = r.settleMissing(ctx, tx, tables, reservation, missing)
		if err != nil {
			return nil, fmt.Errorf("reserve unique keys: %w", err)
		}
	}
	return reservation, nil
}

// takenEntries answers the claims' taken entries sorted by (schema, key,
// value), refusing a value two claims both take.
func (r *DBPersistentRecordRepository) takenEntries(claims []*uniqueKeyClaim) ([]claimedEntry, error) {
	var taken []claimedEntry
	for _, claim := range claims {
		if claim == nil {
			continue
		}
		for _, entry := range claim.take {
			taken = append(taken, claimedEntry{schemaID: claim.schemaID, rowID: claim.rowID, entry: entry})
		}
	}
	slices.SortStableFunc(taken, func(a, b claimedEntry) int {
		return cmp.Or(cmp.Compare(a.schemaID, b.schemaID), cmp.Compare(a.entry.keyID, b.entry.keyID), cmp.Compare(a.entry.value, b.entry.value))
	})
	for i := 1; i < len(taken); i++ {
		if sameEntry(taken[i-1], taken[i]) && taken[i-1].rowID != taken[i].rowID {
			return nil, r.batchUniqueKeyConflict(taken[i], taken[i-1].rowID)
		}
	}
	return slices.CompactFunc(taken, sameEntry), nil
}

// settleMissing judges the entries the insert skipped by their current
// owner: one the claimer holds already is taken, one another row holds is
// the conflict, and one nobody holds any more (its owner released it after
// the insert looked) is answered for another round.
//
// The lookup does not wait for a release under way, as the insert does: an
// owner that began deleting its entry after the insert passed it, and has not
// committed, still holds the value here, so the write answers
// ErrUniqueConflict as if it ran just before the release. Waiting would lock
// the owner's entry (FOR SHARE, or ON CONFLICT DO UPDATE) until this
// transaction ends, and an owner's later release would then wait for it.
func (r *DBPersistentRecordRepository) settleMissing(
	ctx context.Context, tx pgx.Tx, tables model.StorageTables, reservation *uniqueKeyReservation, missing []claimedEntry,
) ([]claimedEntry, error) {
	owners, err := keyEntryOwners(ctx, tx, tables, missing)
	if err != nil {
		// No undo, as after a failed insert: the error poisons a Tx and rolls
		// back any other write, which removes the entries. After a failed
		// statement an undo would fail too, and its error would hide this one.
		return nil, err
	}
	var again []claimedEntry
	for i, c := range missing {
		owner, held := owners[i], owners[i] != uuid.Nil
		switch {
		case !held:
			again = append(again, c)
		case owner != c.rowID:
			return nil, r.undoAfter(ctx, tx, tables, reservation, r.uniqueKeyConflict(c, owner))
		}
	}
	return again, nil
}

// undoAfter removes the reservation's entries and answers cause, or the
// undo's own failure. A failed undo keeps cause as text only: its failed
// statement aborts the transaction, and an answer that still matched
// ErrUniqueConflict would claim the Tx usable.
func (r *DBPersistentRecordRepository) undoAfter(ctx context.Context, tx pgx.Tx, tables model.StorageTables, reservation *uniqueKeyReservation, cause error) error {
	if err := r.undoReservation(ctx, tx, tables, reservation); err != nil {
		return fmt.Errorf("%w (after: %v)", err, cause)
	}
	return cause
}

// undoReservation deletes the entries a reservation inserted, for an
// operation that fails after reserving and must leave nothing written. The
// deleted entries stay uncommitted until the transaction ends, so another
// writer of those values waits for it, as for any entry it wrote.
func (r *DBPersistentRecordRepository) undoReservation(ctx context.Context, tx pgx.Tx, tables model.StorageTables, reservation *uniqueKeyReservation) error {
	if reservation == nil || len(reservation.inserted) == 0 {
		return nil
	}
	schemas, keys, values, rowIDs := entryColumns(reservation.inserted)
	query := fmt.Sprintf(`DELETE FROM %s AS k USING unnest($1::smallint[], $2::smallint[], $3::text[], $4::uuid[]) AS u(s, kid, v, r)
WHERE k.ltbase_schema_id = u.s AND k.key_id = u.kid AND k.key_value = u.v AND k.ltbase_row_id = u.r`,
		sanitizeIdentifier(uniquekey.KeyTable(tables.EntityMain)))
	if _, err := tx.Exec(ctx, query, schemas, keys, values, rowIDs); err != nil {
		return fmt.Errorf("remove the %d unique key entries the write reserved: %w", len(reservation.inserted), err)
	}
	reservation.inserted = nil
	return nil
}

// insertKeyEntries inserts entries in their order, skipping each value the
// key table holds, and answers the ones it inserted.
func insertKeyEntries(ctx context.Context, tx pgx.Tx, tables model.StorageTables, entries []claimedEntry) ([]claimedEntry, error) {
	schemas, keys, values, rowIDs := entryColumns(entries)
	query := fmt.Sprintf(`INSERT INTO %s (ltbase_schema_id, key_id, key_value, ltbase_row_id)
SELECT u.s, u.kid, u.v, u.r FROM unnest($1::smallint[], $2::smallint[], $3::text[], $4::uuid[]) WITH ORDINALITY AS u(s, kid, v, r, n)
ORDER BY u.n
ON CONFLICT (ltbase_schema_id, key_id, key_value) DO NOTHING
RETURNING ltbase_schema_id, key_id, key_value`, sanitizeIdentifier(uniquekey.KeyTable(tables.EntityMain)))
	rows, err := tx.Query(ctx, query, schemas, keys, values, rowIDs)
	if err != nil {
		return nil, fmt.Errorf("insert %d unique key entries: %w", len(entries), err)
	}
	defer rows.Close()
	byKey := make(map[entryKey]claimedEntry, len(entries))
	for _, c := range entries {
		byKey[c.key()] = c
	}
	var inserted []claimedEntry
	for rows.Next() {
		var k entryKey
		if err := rows.Scan(&k.schemaID, &k.keyID, &k.value); err != nil {
			return nil, fmt.Errorf("scan an inserted unique key entry: %w", err)
		}
		c, ok := byKey[k]
		if !ok {
			return nil, fmt.Errorf("the key table answered an entry of key id %d that was not inserted", k.keyID)
		}
		inserted = append(inserted, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("insert %d unique key entries: %w", len(entries), err)
	}
	return inserted, nil
}

// keyEntryOwners answers the row holding each entry's value, positionally;
// uuid.Nil where no row holds it.
func keyEntryOwners(ctx context.Context, tx pgx.Tx, tables model.StorageTables, entries []claimedEntry) ([]uuid.UUID, error) {
	schemas, keys, values, _ := entryColumns(entries)
	query := fmt.Sprintf(`SELECT u.n, k.ltbase_row_id FROM unnest($1::smallint[], $2::smallint[], $3::text[]) WITH ORDINALITY AS u(s, kid, v, n)
JOIN %s AS k ON k.ltbase_schema_id = u.s AND k.key_id = u.kid AND k.key_value = u.v`,
		sanitizeIdentifier(uniquekey.KeyTable(tables.EntityMain)))
	rows, err := tx.Query(ctx, query, schemas, keys, values)
	if err != nil {
		return nil, fmt.Errorf("read the owners of %d unique key values: %w", len(entries), err)
	}
	defer rows.Close()
	owners := make([]uuid.UUID, len(entries))
	for rows.Next() {
		var n int64
		var owner uuid.UUID
		if err := rows.Scan(&n, &owner); err != nil {
			return nil, fmt.Errorf("scan the owner of a unique key value: %w", err)
		}
		owners[n-1] = owner
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the owners of %d unique key values: %w", len(entries), err)
	}
	return owners, nil
}

// entriesNotIn answers the entries of all that inserted lacks.
func entriesNotIn(all, inserted []claimedEntry) []claimedEntry {
	done := make(map[entryKey]bool, len(inserted))
	for _, c := range inserted {
		done[c.key()] = true
	}
	var missing []claimedEntry
	for _, c := range all {
		if !done[c.key()] {
			missing = append(missing, c)
		}
	}
	return missing
}

// entryColumns lays entries out as the four key-table columns.
func entryColumns(entries []claimedEntry) (schemas, keys []int16, values []string, rowIDs []uuid.UUID) {
	schemas, keys = make([]int16, len(entries)), make([]int16, len(entries))
	values, rowIDs = make([]string, len(entries)), make([]uuid.UUID, len(entries))
	for i, c := range entries {
		schemas[i], keys[i], values[i], rowIDs[i] = c.schemaID, c.entry.keyID, c.entry.value, c.rowID
	}
	return schemas, keys, values, rowIDs
}
