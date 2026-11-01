package transform

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

const (
	nulRule     = "text cannot contain a NUL character (U+0000)"
	badUTF8Rule = "text must be valid UTF-8"
)

// #679: the write funnel refuses text PostgreSQL text cannot hold, wherever
// it is stored, and publishes the refusal naming the attribute.
func TestPopulateTypedValue_UnstorableText(t *testing.T) {
	eavText := forma.AttributeMetadata{AttributeID: 4, ValueType: forma.ValueTypeText}
	boundText := boundMeta(forma.ValueTypeText, forma.MainColumnText01, forma.MainColumnEncodingDefault)
	textList := forma.AttributeMetadata{AttributeID: 5, ValueType: forma.ValueTypeList}
	cases := []struct {
		name    string
		meta    forma.AttributeMetadata
		indices string
		value   any
		wantErr string // the published message; "" means accepted
	}{
		{"EAV text, NUL", eavText, "", "a\x00b", "invalid value for attribute 'note' (attrID=4): " + nulRule},
		{"EAV text, invalid UTF-8", eavText, "", "a\xffb", "invalid value for attribute 'note' (attrID=4): " + badUTF8Rule},
		{"EAV text, invalid UTF-8 in []byte", eavText, "", []byte("a\xffb"), "invalid value for attribute 'note' (attrID=4): " + badUTF8Rule},
		{"bound text, NUL", boundText, "", "a\x00b", "invalid value for attribute 'note' (attrID=9): " + nulRule},
		{"bound text, invalid UTF-8", boundText, "", "\xe4\xb8", "invalid value for attribute 'note' (attrID=9): " + badUTF8Rule},
		{"text list element, NUL", textList, "1", "a\x00b", "invalid value for attribute 'note' (attrID=5): " + nulRule},
		{"EAV text, multi-byte", eavText, "", "é中\U0001F600�", ""},
		{"bound text, control character", boundText, "", "a\u0001b", ""},
		{"uuid: the canonical spelling is stored", forma.AttributeMetadata{AttributeID: 6, ValueType: forma.ValueTypeUUID}, "", uuid.NewString(), ""},
		// A value bound to a system column is dropped, never stored, so it
		// is not judged (storeInMainColumn).
		{"system column alias, dropped", boundMeta(forma.ValueTypeText, "ltbase_created_at", forma.MainColumnEncodingDefault), "", "a\x00b", ""},
	}
	for _, tc := range cases {
		attr := model.EAVRecord{ArrayIndices: tc.indices}
		_, err := populateTypedValue(&attr, "note", tc.value, tc.meta)
		if tc.wantErr == "" {
			assert.NoError(t, err, tc.name)
			continue
		}
		require.ErrorIs(t, err, forma.ErrInvalidInput, tc.name)
		message, published := forma.ResolvePublicMessage(err)
		require.True(t, published, tc.name)
		assert.Equal(t, tc.wantErr, message, tc.name)
	}
}

// CheckTextValues judges the text claims of a partial document as the write
// converts them, and leaves every other position to the write.
func TestCheckTextValues(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"name":       {AttributeID: 1, ValueType: forma.ValueTypeText},
		"label":      {AttributeID: 2, ValueType: forma.ValueTypeText, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText01}},
		"tags":       {AttributeID: 3, ValueType: forma.ValueTypeList},
		"counts":     {AttributeID: 4, ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeInteger},
		"items.note": {AttributeID: 5, ValueType: forma.ValueTypeText},
		"age":        {AttributeID: 6, ValueType: forma.ValueTypeInteger},
		"ref":        {AttributeID: 7, ValueType: forma.ValueTypeUUID},
		"created":    {AttributeID: 8, ValueType: forma.ValueTypeText, ColumnBinding: &forma.MainColumnBinding{ColumnName: "ltbase_created_at"}},
	}
	cases := []struct {
		name    string
		doc     map[string]any
		wantErr string
	}{
		{"storable text", map[string]any{"name": "中", "label": "ok", "tags": []any{"a", "b"}}, ""},
		{"EAV scalar", map[string]any{"name": "a\x00b"}, "invalid value for attribute 'name' (attrID=1): " + nulRule},
		{"bound scalar", map[string]any{"label": "a\xffb"}, "invalid value for attribute 'label' (attrID=2): " + badUTF8Rule},
		{"list element", map[string]any{"tags": []any{"ok", "a\x00b"}}, "invalid value for attribute 'tags' (attrID=3): " + nulRule},
		{"array-of-objects element", map[string]any{"items": []any{map[string]any{"note": "a\x00b"}}}, "invalid value for attribute 'items.note' (attrID=5): " + nulRule},
		{"literal dotted key", map[string]any{"items.note": "a\x00b"}, "invalid value for attribute 'items.note' (attrID=5): " + nulRule},
		// Left to the write, which refuses each for its own reason.
		{"not text: integer", map[string]any{"age": "1\x00"}, ""},
		{"not text: integer list", map[string]any{"counts": []any{"1\x00"}}, ""},
		{"not text: uuid", map[string]any{"ref": "a\x00b"}, ""},
		{"undefined attribute", map[string]any{"nope": "a\x00b"}, ""},
		{"null", map[string]any{"name": nil}, ""},
		{"system column alias", map[string]any{"created": "a\x00b"}, ""},
		// A list position the write refuses for its shape is no stored
		// text: the shape refusal keeps its stage, even over invalid text.
		{"scalar for a list", map[string]any{"tags": "ordinary"}, ""},
		{"scalar for a list, NUL", map[string]any{"tags": "a\x00b"}, ""},
		{"nested array in a list", map[string]any{"tags": []any{[]any{"a\x00b"}}}, ""},
		{"past the nesting cap", cyclicDocument(), ""},
	}
	for _, tc := range cases {
		err := CheckTextValues(tc.doc, cache)
		if tc.wantErr == "" {
			assert.NoError(t, err, tc.name)
			continue
		}
		require.ErrorIs(t, err, forma.ErrInvalidInput, tc.name)
		message, published := forma.ResolvePublicMessage(err)
		require.True(t, published, tc.name)
		assert.Equal(t, tc.wantErr, message, tc.name)
	}
}

// Every refusal CheckTextValues makes is the one the full write makes on
// the same value, so judging Updates first changes when the update is
// refused, not what it is refused with.
func TestCheckTextValues_MatchesTheWrite(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"name": {AttributeID: 1, ValueType: forma.ValueTypeText},
		"tags": {AttributeID: 3, ValueType: forma.ValueTypeList},
	}
	for _, doc := range []map[string]any{
		{"name": "a\x00b"},
		{"tags": []any{"a\xffb"}},
	} {
		entries, err := walkDocument(1, uuid.New(), doc, cache)
		require.NoError(t, err)
		_, writeErr := flattenToAttributes(entries)
		intakeErr := CheckTextValues(doc, cache)
		require.Error(t, writeErr)
		require.Error(t, intakeErr)
		assert.Equal(t, writeErr.Error(), intakeErr.Error())
	}
}

// Every position CheckTextValues leaves alone, the write refuses for its own
// reason, so leaving it changes no outcome: the write still refuses it.
func TestCheckTextValues_LeavesOtherRefusalsToTheWrite(t *testing.T) {
	cache := forma.SchemaAttributeCache{"tags": {AttributeID: 3, ValueType: forma.ValueTypeList}}
	for _, doc := range []map[string]any{
		{"tags": "ordinary"},
		{"tags": "a\x00b"},
		{"tags": []any{[]any{"a\x00b"}}},
	} {
		require.NoError(t, CheckTextValues(doc, cache), doc)
		entries, err := walkDocument(1, uuid.New(), doc, cache)
		require.NoError(t, err)
		_, writeErr := flattenToAttributes(entries)
		require.ErrorIs(t, writeErr, forma.ErrInvalidInput, doc)
		assert.NotContains(t, writeErr.Error(), "NUL", "the write refuses the shape, not the text")
	}
}

// cyclicDocument is a document past the nesting cap, which the write refuses.
func cyclicDocument() map[string]any {
	doc := map[string]any{"name": "a\x00b"}
	doc["self"] = doc
	return doc
}
