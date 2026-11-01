package federated

import (
	"fmt"
	"sort"

	"github.com/google/uuid"

	"github.com/lychee-technology/forma/internal/model"
)

// eavKey is an EAV attribute's identity within a record: the schema and
// attribute it belongs to and its position in an array. The order of a
// record's EAV list is incidental (each engine aggregates it in its own
// order), so the comparison pairs attributes by this key, never by position.
type eavKey struct {
	schemaID     int16
	rowID        uuid.UUID
	attrID       int16
	arrayIndices string
}

// name renders the key for a mismatch on the record of rowID; it names the
// attribute's row only when that is another row.
func (k eavKey) name(rowID uuid.UUID) string {
	name := fmt.Sprintf("eav:%d/%d[%s]", k.schemaID, k.attrID, k.arrayIndices)
	if k.rowID != rowID {
		name += "@" + k.rowID.String()
	}
	return name
}

// eavValue is the semantic value an EAV attribute carries: its text and its
// numeric reading (eavNumber), which keeps the exact value the int64 sidecar
// or the read query's token holds. It leaves out the remaining read-path
// metadata (BoundColumn), so that one value read through two paths compares
// equal.
type eavValue struct {
	text   *string
	number eavNumber
	// ambiguous marks, in a report, the side whose float64 image cannot tell
	// the other side's exact value from its neighbours (eavAmbiguous).
	ambiguous bool
}

func (v eavValue) String() string {
	text := "<nil>"
	if v.text != nil {
		text = fmt.Sprintf("%q", *v.text)
	}
	s := fmt.Sprintf("{text:%s numeric:%s}", text, v.number)
	if v.ambiguous {
		s += " (ambiguous: a lossy float64 image the other side's exact value rounds to)"
	}
	return s
}

func newEAVValue(attr model.EAVRecord) eavValue {
	return eavValue{text: attr.ValueText, number: newEAVNumber(attr)}
}

// compare compares two values semantically: texts by content, numbers by
// compareNumbers.
func (v eavValue) compare(o eavValue) eavVerdict {
	if (v.text == nil) != (o.text == nil) || (v.text != nil && *v.text != *o.text) {
		return eavDiffer
	}
	return compareNumbers(v.number, o.number)
}

// markAmbiguous returns the pair to report for an ambiguous comparison, the
// side that holds only an image marked.
func markAmbiguous(fed, pg eavValue) (eavValue, eavValue) {
	fed.ambiguous = fed.number.exact == nil
	pg.ambiguous = pg.number.exact == nil
	return fed, pg
}

// indexEAV groups a record's EAV attributes by their key. An attribute that
// does not name its row is the record's own.
func indexEAV(rowID uuid.UUID, attrs []model.EAVRecord) map[eavKey][]eavValue {
	byKey := make(map[eavKey][]eavValue, len(attrs))
	for _, attr := range attrs {
		key := eavKey{schemaID: attr.SchemaID, rowID: attr.RowID, attrID: attr.AttrID, arrayIndices: attr.ArrayIndices}
		if key.rowID == uuid.Nil {
			key.rowID = rowID
		}
		byKey[key] = append(byKey[key], newEAVValue(attr))
	}
	return byKey
}

// compareEAV reports, in key order, every EAV attribute on which two records
// of the same row differ: one only one side carries, one either side repeats,
// one whose value differs, or one whose exact value the other side's float64
// image cannot confirm (eavAmbiguous, reported with that side marked).
func compareEAV(rowID uuid.UUID, fed, pg []model.EAVRecord, mismatch func(name string, fedVal, pgVal any)) {
	fedByKey, pgByKey := indexEAV(rowID, fed), indexEAV(rowID, pg)
	keys := make([]eavKey, 0, len(fedByKey)+len(pgByKey))
	for k := range fedByKey {
		keys = append(keys, k)
	}
	for k := range pgByKey {
		if _, ok := fedByKey[k]; !ok {
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return lessEAVKey(keys[i], keys[j]) })
	for _, k := range keys {
		fedVals, pgVals := fedByKey[k], pgByKey[k]
		if len(fedVals) == 1 && len(pgVals) == 1 {
			switch fedVals[0].compare(pgVals[0]) {
			case eavSame:
				continue
			case eavAmbiguous:
				fedVal, pgVal := markAmbiguous(fedVals[0], pgVals[0])
				mismatch(k.name(rowID), fedVal, pgVal)
				continue
			}
		}
		mismatch(k.name(rowID), eavReportValue(fedVals), eavReportValue(pgVals))
	}
}

func lessEAVKey(a, b eavKey) bool {
	if a.schemaID != b.schemaID {
		return a.schemaID < b.schemaID
	}
	if a.rowID != b.rowID {
		return a.rowID.String() < b.rowID.String()
	}
	if a.attrID != b.attrID {
		return a.attrID < b.attrID
	}
	return a.arrayIndices < b.arrayIndices
}

// eavReportValue renders one side of an EAV mismatch: absent, the single
// value, or every value of a repeated key.
func eavReportValue(vals []eavValue) any {
	switch len(vals) {
	case 0:
		return absentValue{}
	case 1:
		return vals[0]
	}
	return vals
}
