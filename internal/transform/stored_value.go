package transform

import (
	"fmt"
	"strings"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// storedValue is a stored attribute value an update's merge carries without
// decoding it. The merge base holds one per stored record the read places, a
// list's empty-list marker included; only the ones the written row keeps are
// decoded (resolveStoredValues), so a stored value the read refuses, such as
// a bigint image past int64 (#590), blocks the updates that keep it and never
// the one that replaces it.
//
// It is a leaf to everything that handles the merge base: mergeMaps,
// StripComputedFields and the document walk place it, move it, or drop it
// like any scalar, and it never leaves the transform package outside that
// base.
type storedValue struct {
	record    model.EAVRecord
	valueType forma.ValueType
	attrName  string
}

// decode converts the stored value exactly as the read path does, errors
// included: FromPersistentRecord over the same row raises the same error. An
// empty-list marker decodes to the [] the read places.
func (v *storedValue) decode() (any, error) {
	if v.marker() {
		return []any{}, nil
	}
	return decodeRecordValue(v.record, v.attrName, v.valueType)
}

// marker reports whether the held record is an empty-list marker: it holds
// no value, and the rebuild places such a record only for a list.
func (v *storedValue) marker() bool {
	return !hasStoredValue(v.record)
}

// holdsNoValue reports whether a record's value, decoded or held, is none:
// the record is a list's empty-list marker or, for any other attribute,
// nothing the rebuild places.
func holdsNoValue(value any) bool {
	held, isHeld := value.(*storedValue)
	return value == nil || isHeld && held.marker()
}

// describe names what the held record stores, for an error that refuses to
// write it back.
func (v *storedValue) describe() string {
	if v.marker() {
		return fmt.Sprintf("empty-list marker of list attribute '%s'", v.attrName)
	}
	return fmt.Sprintf("%s value of attribute '%s'", v.valueType, v.attrName)
}

// replacedBy reports whether the caller's own spelling of the attribute
// replaces the stored value, rebuilt under spelling, as it replaces the
// attribute's other stored values (#312): another spelling wins, and the walk
// claims no stored value of the attribute at it, so only a caller's key
// spells it. A winning spelling the walk also claims stored values at
// replaces nothing: it is other stored records being written back.
func (v *storedValue) replacedBy(winners spellingWinners, spelling string, stored storedSpellings) bool {
	identity := identityOf(v.record)
	winner, written := winners[identity]
	if !written || winner == spelling {
		return false
	}
	_, storedThere := stored[identity][winner]
	return !storedThere
}

// nestedSpelling is the spelling of the attribute's own path, the one the
// rebuild places a stored value under unless it places it under a literal
// key (rebuildByAncestry).
func (v *storedValue) nestedSpelling() string {
	return spellingOf(strings.Split(v.attrName, "."))
}

// storedSpellings holds, per attribute, the spellings the walk claims stored
// values at.
type storedSpellings map[attributeIdentity]map[string]struct{}

func storedSpellingsOf(entries []documentEntry) storedSpellings {
	stored := make(storedSpellings)
	for _, entry := range entries {
		held, isHeld := entry.value.(*storedValue)
		if !isHeld || entry.kind != entryClaim {
			continue
		}
		identity := identityOf(held.record)
		if stored[identity] == nil {
			stored[identity] = make(map[string]struct{}, 1)
		}
		stored[identity][entry.tagged.spelling] = struct{}{}
	}
	return stored
}

// unplacedError refuses an update that keeps a stored value the write cannot
// place: the row rebuilt it at rebuiltAt, a path the schema does not define,
// so the written row would omit its record and replaceEAVAttributes would
// delete it. The stored row's shape is at fault, not the caller, so the error
// is plain, like the read path's.
func (v *storedValue) unplacedError(rebuiltAt string) error {
	return fmt.Errorf("record %s: stored %s rebuilds at '%s', which the schema does not define, "+
		"so this update cannot write it back; an update must replace the attribute or a container holding it",
		eavRecordIdentity(v.record), v.describe(), rebuiltAt)
}

// respelledError refuses an update that keeps a stored value the write would
// discard: the row rebuilds it under a different key spelling than other
// stored records of its attribute, as a row holding records written through
// both a nested and a literal dotted key does, and the write keeps one
// spelling per attribute (#312), so the written row would omit its record.
// Like unplacedError, it is the stored row's fault and the error is plain.
func (v *storedValue) respelledError() error {
	return fmt.Errorf("record %s: stored %s rebuilds under a different key spelling than the attribute's other stored records, "+
		"and a write keeps one spelling per attribute, so this update cannot write it back; an update must replace the attribute or a container holding it",
		eavRecordIdentity(v.record), v.describe())
}

// hasStoredValue reports whether a record carries a value to decode. A record
// with every value column NULL decodes to nil for every value type: it is an
// empty-list marker or nothing, on read and on the merge base alike.
func hasStoredValue(record model.EAVRecord) bool {
	return record.ValueText != nil || record.ValueNumeric != nil || record.ValueInt64 != nil
}

// decodeEAVRecord is the read path's value decode: an extraction failure is a
// consistency error, plain and operator-visible, named with the record's full
// identity (#405), since this is the hop that holds the record.
func decodeEAVRecord(record model.EAVRecord, valueType forma.ValueType) (any, error) {
	value, err := extractValueFromEAVRecord(record, valueType)
	if err != nil {
		return nil, fmt.Errorf("record %s: %w", eavRecordIdentity(record), err)
	}
	return value, nil
}
