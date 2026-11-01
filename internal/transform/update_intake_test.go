package transform

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
)

func updateIntakeCache() forma.SchemaAttributeCache {
	bound := func(vt forma.ValueType, id int16, col forma.MainColumn, enc forma.MainColumnEncoding) forma.AttributeMetadata {
		return forma.AttributeMetadata{AttributeID: id, ValueType: vt,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: col, Encoding: enc}}
	}
	return forma.SchemaAttributeCache{
		"name":       {AttributeID: 1, ValueType: forma.ValueTypeText},
		"label":      bound(forma.ValueTypeText, 2, forma.MainColumnText01, ""),
		"tags":       {AttributeID: 3, ValueType: forma.ValueTypeList},
		"counts":     {AttributeID: 4, ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeInteger},
		"items.note": {AttributeID: 5, ValueType: forma.ValueTypeText},
		"items.qty":  {AttributeID: 6, ValueType: forma.ValueTypeSmallInt},
		"age":        {AttributeID: 7, ValueType: forma.ValueTypeInteger},
		"rank":       bound(forma.ValueTypeInteger, 8, forma.MainColumnSmallint01, ""),
		"big":        {AttributeID: 9, ValueType: forma.ValueTypeBigInt},
		"ratio":      {AttributeID: 10, ValueType: forma.ValueTypeNumeric},
		"ref":        {AttributeID: 11, ValueType: forma.ValueTypeUUID},
		"born":       {AttributeID: 12, ValueType: forma.ValueTypeDate},
		"due_iso":    bound(forma.ValueTypeDateTime, 13, forma.MainColumnText02, forma.MainColumnEncodingISO8601),
		"active":     {AttributeID: 14, ValueType: forma.ValueTypeBool},
		"created":    bound(forma.ValueTypeText, 15, "ltbase_created_at", ""),
		"createdAt":  bound(forma.ValueTypeBigInt, 16, "ltbase_created_at", ""),
		// A list declaring its (absent) array ancestors, where an [] inside
		// the list's own value is the walk's refusal (emptyList).
		"flags": {AttributeID: 17, ValueType: forma.ValueTypeList, ArrayAncestors: []string{}},
	}
}

type intakeCase struct {
	name string
	doc  map[string]any
	// want is the published refusal; "" means intake accepts the value.
	want string
}

// refusedValue is the published prefix of a conversion refusal.
func refusedValue(attr string, id int) string {
	return fmt.Sprintf("invalid value for attribute '%s' (attrID=%d): ", attr, id)
}

func refusedUpdateValues() []intakeCase {
	return []intakeCase{
		{"text, NUL", map[string]any{"name": "a\x00b"}, refusedValue("name", 1) + nulRule},
		{"bound text, invalid UTF-8", map[string]any{"label": "a\xffb"}, refusedValue("label", 2) + badUTF8Rule},
		{"text list element, NUL", map[string]any{"tags": []any{"ok", "a\x00b"}}, refusedValue("tags", 3) + nulRule},
		{"integer past its width", map[string]any{"age": json.Number("2147483648")}, refusedValue("age", 7)},
		{"integer past its bound column", map[string]any{"rank": 40000}, refusedValue("rank", 8)},
		{"integer, fractional", map[string]any{"age": 1.5}, refusedValue("age", 7)},
		{"bigint, fractional past 2^52", map[string]any{"big": json.Number("9007199254740991.5")}, refusedValue("big", 9)},
		{"bigint past its EAV image", map[string]any{"big": json.Number("9223372036854775807")}, refusedValue("big", 9)},
		{"bigint past int64", map[string]any{"big": json.Number("9223372036854775808")}, refusedValue("big", 9)},
		{"numeric, not a number", map[string]any{"ratio": "abc"}, refusedValue("ratio", 10)},
		{"integer list element, fractional", map[string]any{"counts": []any{1, 2.5}}, refusedValue("counts", 4)},
		{"array-of-objects element, integer past smallint", map[string]any{"items": []any{map[string]any{"qty": 1}, map[string]any{"qty": 70000}}}, refusedValue("items.qty", 6)},
		{"literal dotted key", map[string]any{"items.qty": 1.25}, refusedValue("items.qty", 6)},
		{"uuid, malformed", map[string]any{"ref": "not-a-uuid"}, refusedValue("ref", 11)},
		{"date, unparseable", map[string]any{"born": "yesterday"}, refusedValue("born", 12)},
		{"date past the EAV image range", map[string]any{"born": "9007199254740993"}, refusedValue("born", 12)},
		{"datetime past the iso8601 column", map[string]any{"due_iso": time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)}, refusedValue("due_iso", 13)},
		{"bool, not 0 or 1", map[string]any{"active": 2}, refusedValue("active", 14)},
		{"scalar for a list", map[string]any{"tags": "ordinary"}, refusedValue("tags", 3)},
		{"nested array in a list", map[string]any{"tags": []any{[]any{"a"}}}, refusedValue("tags", 3)},
		// A system column alias is dropped, but the write converts it to
		// its declared type first.
		{"system column alias, fractional", map[string]any{"createdAt": 1.5}, refusedValue("createdAt", 16)},
	}
}

func acceptedUpdateValues() []intakeCase {
	return []intakeCase{
		{"one of each", map[string]any{
			"name": "中", "label": "ok", "tags": []any{"a", "b"}, "counts": []any{1, json.Number("2")},
			"items": []any{map[string]any{"note": "n", "qty": 3}},
			"age":   json.Number("2147483647"), "rank": 32767, "big": json.Number("9007199254740992"),
			"ratio": 1.5, "ref": uuid.NewString(), "born": "2026-11-01", "due_iso": "2026-11-01T06:00:00Z",
			"active": true,
		}, ""},
		{"the empty-list marker", map[string]any{"tags": []any{}, "counts": []any{}}, ""},
		{"system column alias, unstorable text: dropped, not judged", map[string]any{"created": "a\x00b"}, ""},
	}
}

// Positions the walk refuses rather than claims: the write refuses each at
// its own stage, after the row lock, and intake leaves them there.
func walkRefusedUpdates() []intakeCase {
	return []intakeCase{
		{"undefined attribute", map[string]any{"nope": "a\x00b"}, ""},
		{"null", map[string]any{"age": nil}, ""},
		{"null list element", map[string]any{"counts": []any{1, nil}}, ""},
		{"[] nested in a list", map[string]any{"flags": []any{[]any{}}}, ""},
		{"past the nesting cap", cyclicDocument(), ""},
	}
}

// CheckUpdateValues converts every value of a partial document the walk
// claims, as the write converts it, and leaves the walk's own refusals to the
// write (#772).
func TestCheckUpdateValues(t *testing.T) {
	cache := updateIntakeCache()
	cases := append(append(refusedUpdateValues(), acceptedUpdateValues()...), walkRefusedUpdates()...)
	for _, tc := range cases {
		_, err := CheckUpdateValues(tc.doc, cache)
		if tc.want == "" {
			assert.NoError(t, err, tc.name)
			continue
		}
		require.ErrorIs(t, err, forma.ErrInvalidInput, tc.name)
		message, published := forma.ResolvePublicMessage(err)
		require.True(t, published, tc.name)
		assert.Contains(t, message, tc.want, tc.name)
	}
}

// writeVerdict is the full write's outcome on doc: the walk, then the
// conversion, as ToAttributes runs them (the required policy aside).
func writeVerdict(t *testing.T, doc map[string]any, cache forma.SchemaAttributeCache) error {
	t.Helper()
	entries, err := walkDocument(1, uuid.New(), doc, cache)
	if err != nil {
		return err
	}
	_, err = flattenToAttributes(entries)
	return err
}

// Every refusal CheckUpdateValues makes is the one the full write makes on
// the same value, so judging Updates first changes when the update is
// refused, not what it is refused with; and every document the write accepts
// passes intake, so no accepted update changes outcome.
func TestCheckUpdateValues_MatchesTheWrite(t *testing.T) {
	cache := updateIntakeCache()
	for _, tc := range refusedUpdateValues() {
		writeErr := writeVerdict(t, tc.doc, cache)
		_, intakeErr := CheckUpdateValues(tc.doc, cache)
		require.Error(t, writeErr, tc.name)
		require.Error(t, intakeErr, tc.name)
		assert.Equal(t, writeErr.Error(), intakeErr.Error(), tc.name)
	}
	for _, tc := range acceptedUpdateValues() {
		require.NoError(t, writeVerdict(t, tc.doc, cache), tc.name)
		checked, err := CheckUpdateValues(tc.doc, cache)
		require.NoError(t, err, tc.name)
		require.NoError(t, writeVerdict(t, checked, cache), "%s: the write accepts what intake answers", tc.name)
	}
}

// Of several refused values, intake reports the one the write's conversion
// meets first: the walk's sorted key order.
func TestCheckUpdateValues_ReportsTheWritesFirstRefusal(t *testing.T) {
	cache := updateIntakeCache()
	doc := map[string]any{"ref": "bad", "age": 1.5, "items": []any{map[string]any{"qty": 0.5}}}
	writeErr := writeVerdict(t, doc, cache)
	_, intakeErr := CheckUpdateValues(doc, cache)
	require.Error(t, intakeErr)
	assert.Equal(t, writeErr.Error(), intakeErr.Error())
	assert.Contains(t, intakeErr.Error(), "'age'")
}

// Every position CheckUpdateValues leaves alone, the write refuses for its
// own reason, so leaving it changes no outcome: the write still refuses it.
func TestCheckUpdateValues_LeavesWalkRefusalsToTheWrite(t *testing.T) {
	cache := updateIntakeCache()
	for _, tc := range walkRefusedUpdates() {
		checked, err := CheckUpdateValues(tc.doc, cache)
		require.NoError(t, err, tc.name)
		require.ErrorIs(t, writeVerdict(t, checked, cache), forma.ErrInvalidInput, tc.name)
	}
}

// Only a text position carries an image: every other converted value is
// kept as sent, so schema validation and the write see the caller's value.
func TestCheckUpdateValues_KeepsConvertedValuesAsSent(t *testing.T) {
	doc := acceptedUpdateValues()[0].doc
	checked, err := CheckUpdateValues(doc, updateIntakeCache())
	require.NoError(t, err)
	assert.Equal(t, doc, checked)
}
