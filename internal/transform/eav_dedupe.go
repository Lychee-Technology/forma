package transform

import (
	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/model"
)

// eavPrimaryKey mirrors the eav_data primary key
// (schema_id, row_id, attr_id, array_indices) declared in cmd/tools/init_db.go.
// All four columns are compared because insertEAVAttributes is also handed
// batch-assembled slices spanning several rows and schemas.
type eavPrimaryKey struct {
	schemaID     int16
	rowID        uuid.UUID
	attrID       int16
	arrayIndices string
}

func eavKeyOf(record model.EAVRecord) eavPrimaryKey {
	return eavPrimaryKey{
		schemaID:     record.SchemaID,
		rowID:        record.RowID,
		attrID:       record.AttrID,
		arrayIndices: record.ArrayIndices,
	}
}

// dedupeEAVRecords collapses records that collide on the eav_data primary key.
//
// Why collisions exist: attribute names in this codebase are dotted, so
// flattenToAttributes reaches "contact.email" both by recursing into
// {"contact":{"email":…}} and through a literal top-level "contact.email" key.
// A single request can carry both spellings — most commonly on update, where
// mergeMaps is key-literal while FromPersistentRecord re-nests stored
// attributes. Two records with one primary key make the multi-row INSERT in
// insertEAVAttributes fail with PostgreSQL 23505 (#312).
//
// Resolution is last-write-wins, matching encoding/json's own duplicate-key
// semantics. It is deterministic because flattenToAttributes sorts every map's
// keys instead of ranging in map order: for any dotted name the nested
// spelling's top-level key is a proper prefix of the literal one, so it sorts
// first and the literal key — the caller's explicit value — is emitted last.
//
// Ordering rule: the surviving value is written back at the first occurrence's
// position, so the output order of distinct keys is exactly their first-seen
// order in the input and never depends on map iteration.
func dedupeEAVRecords(records []model.EAVRecord) []model.EAVRecord {
	if len(records) < 2 {
		return records
	}

	positions := make(map[eavPrimaryKey]int, len(records))
	deduped := make([]model.EAVRecord, 0, len(records))
	for _, record := range records {
		key := eavKeyOf(record)
		if position, seen := positions[key]; seen {
			deduped[position] = record
			continue
		}
		positions[key] = len(deduped)
		deduped = append(deduped, record)
	}

	return deduped
}
