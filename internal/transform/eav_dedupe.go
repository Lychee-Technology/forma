package transform

import (
	"strconv"
	"strings"

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

// attributeIdentity is the *logical* attribute a record belongs to: the same
// primary key minus array_indices. It is the unit of replacement — a caller who
// writes a list writes the whole list, not one element of it.
type attributeIdentity struct {
	schemaID int16
	rowID    uuid.UUID
	attrID   int16
}

// taggedEAVRecord pairs a flattened record with the concrete key spelling that
// produced it. The tag is flatten-time provenance only: it never reaches
// model.EAVRecord, which is a storage struct with no column for it.
type taggedEAVRecord struct {
	record   model.EAVRecord
	spelling string
}

func eavKeyOf(record model.EAVRecord) eavPrimaryKey {
	return eavPrimaryKey{
		schemaID:     record.SchemaID,
		rowID:        record.RowID,
		attrID:       record.AttrID,
		arrayIndices: record.ArrayIndices,
	}
}

func identityOf(record model.EAVRecord) attributeIdentity {
	return attributeIdentity{
		schemaID: record.SchemaID,
		rowID:    record.RowID,
		attrID:   record.AttrID,
	}
}

// spellingOf encodes the concrete key path a record was reached through, as
// actually written in the payload.
//
// strings.Join(path, ".") cannot serve: it is exactly what collapses the two
// spellings of a dotted attribute into one name, which is the collision this
// tag exists to tell apart. ["contact","emails"] and ["contact.emails"] both
// join to "contact.emails". Length-prefixing each segment makes the encoding
// injective — "7:contact6:emails" versus "14:contact.emails" — so no segment
// content, dots or otherwise, can forge another path's tag.
func spellingOf(path []string) string {
	var builder strings.Builder
	for _, segment := range path {
		builder.WriteString(strconv.Itoa(len(segment)))
		builder.WriteByte(':')
		builder.WriteString(segment)
	}
	return builder.String()
}

// dedupeEAVRecords resolves a payload that spells one attribute more than once,
// keeping the whole attribute the last spelling produced.
//
// Why collisions exist: attribute names in this codebase are dotted, so
// flattenToAttributes reaches "contact.email" both by recursing into
// {"contact":{"email":…}} and through a literal top-level "contact.email" key.
// A single request can carry both spellings — most commonly on update, where
// mergeMaps is key-literal while FromPersistentRecord re-nests stored
// attributes. Two records with one primary key make the multi-row INSERT in
// insertEAVAttributes fail with PostgreSQL 23505 (#312).
//
// The unit of replacement is the logical attribute, not the primary key.
// Collapsing per key looks right for scalars and is wrong for collections:
// stored ["old0","old1"] plus a literal ["new0"] collides only at index 0, so
// "old1" would survive into a list the caller replaced, and a literal []
// emits only an empty-list marker (array_indices "") that collides with no
// index at all, so the clear would persist nothing. Both would return 200 with
// stale rows — quietly wrong, where the duplicate-key failure was at least
// loud. So every record a losing spelling produced for an attribute is
// discarded: all indices, and the marker.
//
// Resolution is last-write-wins, matching encoding/json's own duplicate-key
// semantics. It is deterministic because flattenToAttributes sorts every map's
// keys instead of ranging in map order: for any dotted name the nested
// spelling's top-level key is a proper prefix of the literal one, so it sorts
// first and the literal key — the caller's explicit value — is emitted last.
//
// Ordering rule: surviving records keep their relative input order, and a
// residual primary-key collision *within* one spelling is still collapsed
// last-wins at the first occurrence's position. That second pass is a
// backstop — one spelling cannot legitimately emit a key twice, because JSON
// objects have unique keys and array indices are unique per list — so it exists
// to keep the slice insertable rather than to express a policy.
func dedupeEAVRecords(tagged []taggedEAVRecord) []model.EAVRecord {
	winners := make(map[attributeIdentity]string, len(tagged))
	for _, item := range tagged {
		winners[identityOf(item.record)] = item.spelling
	}

	positions := make(map[eavPrimaryKey]int, len(tagged))
	deduped := make([]model.EAVRecord, 0, len(tagged))
	for _, item := range tagged {
		if winners[identityOf(item.record)] != item.spelling {
			continue
		}

		key := eavKeyOf(item.record)
		if position, seen := positions[key]; seen {
			deduped[position] = item.record
			continue
		}
		positions[key] = len(deduped)
		deduped = append(deduped, item.record)
	}

	return deduped
}
