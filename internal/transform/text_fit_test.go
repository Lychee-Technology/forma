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

// cyclicDocument is a document past the nesting cap, which the write refuses.
func cyclicDocument() map[string]any {
	doc := map[string]any{"name": "a\x00b"}
	doc["self"] = doc
	return doc
}
