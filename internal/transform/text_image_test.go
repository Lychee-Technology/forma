package transform

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
)

// changingText is a fmt.Stringer whose String() answers the next of its
// images on each call, the last one from then on (#773).
type changingText struct {
	images []string
	calls  *int
}

func newChangingText(images ...string) changingText {
	return changingText{images: images, calls: new(int)}
}

func (c changingText) String() string {
	i := min(*c.calls, len(c.images)-1)
	*c.calls++
	return c.images[i]
}

// failingJSON is a value json.Marshal refuses.
type failingJSON struct{}

func (failingJSON) String() string               { return "text" }
func (failingJSON) MarshalJSON() ([]byte, error) { return nil, errors.New("no JSON form") }

func textImageCache() forma.SchemaAttributeCache {
	return forma.SchemaAttributeCache{
		"name":       {AttributeID: 1, ValueType: forma.ValueTypeText},
		"age":        {AttributeID: 2, ValueType: forma.ValueTypeInteger},
		"tags":       {AttributeID: 3, ValueType: forma.ValueTypeList},
		"items.note": {AttributeID: 5, ValueType: forma.ValueTypeText},
	}
}

// writtenText answers the text the write stores for doc, per attribute ID.
func writtenText(t *testing.T, doc map[string]any, cache forma.SchemaAttributeCache) map[int16][]string {
	t.Helper()
	entries, err := walkDocument(1, uuid.New(), doc, cache)
	require.NoError(t, err)
	records, err := flattenToAttributes(entries)
	require.NoError(t, err)
	written := make(map[int16][]string)
	for _, tagged := range records {
		if tagged.record.ValueText != nil {
			written[tagged.record.AttrID] = append(written[tagged.record.AttrID], *tagged.record.ValueText)
		}
	}
	return written
}

// The issue's case: a Stringer that answers valid text once and NUL after.
// Intake judges the first image and the write stores it, at a scalar, a list
// element and an element of an array of objects, without calling String()
// again.
func TestCheckTextValues_TheWriteStoresTheCheckedImage(t *testing.T) {
	cache := textImageCache()
	name, tag, note := newChangingText("first", "a\x00b"), newChangingText("tag", "a\x00b"), newChangingText("note", "a\x00b")
	doc := map[string]any{
		"name":  name,
		"tags":  []any{"plain", tag},
		"items": []any{map[string]any{"note": note}},
	}
	checked, err := CheckTextValues(doc, cache)
	require.NoError(t, err)
	assert.Equal(t, map[int16][]string{1: {"first"}, 3: {"plain", "tag"}, 5: {"note"}}, writtenText(t, checked, cache))
	assert.Equal(t, 1, *name.calls, "String() is called once, at intake")
	assert.Equal(t, 1, *tag.calls)
	assert.Equal(t, 1, *note.calls)

	// Judging the copy again, as Update does for a Tx route's rows, keeps
	// the image and calls nothing.
	again, err := CheckTextValues(checked, cache)
	require.NoError(t, err)
	assert.Equal(t, checked, again)
	assert.Equal(t, 1, *name.calls)
}

// A Stringer whose output changes between valid strings is stored as the
// image intake checked.
func TestCheckTextValues_ChangingValidTextStoresTheFirstImage(t *testing.T) {
	cache := textImageCache()
	value := newChangingText("one", "two", "three")
	checked, err := CheckTextValues(map[string]any{"name": value}, cache)
	require.NoError(t, err)
	assert.Equal(t, map[int16][]string{1: {"one"}}, writtenText(t, checked, cache))
}

// The caller's document is never written to: the answer is a copy, and the
// caller's values stay where they were.
func TestCheckTextValues_LeavesTheCallersDocument(t *testing.T) {
	value := newChangingText("first")
	nested := map[string]any{"note": value}
	items := []any{nested}
	doc := map[string]any{"name": value, "items": items}
	checked, err := CheckTextValues(doc, textImageCache())
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"name": value, "items": []any{map[string]any{"note": value}}}, doc)
	assert.Equal(t, value, nested["note"])
	assert.Equal(t, nested, items[0])
	assert.IsType(t, textImage{}, checked["name"])
	assert.IsType(t, textImage{}, checked["items"].([]any)[0].(map[string]any)["note"])
}

// Validation judges the value's JSON form (schemavalidate.Validate), which
// the carrier keeps: a time.Time at a text attribute still marshals to RFC
// 3339, while the write stores its text image, time.String(), as before.
func TestCheckTextValues_KeepsTheJSONFormValidationJudges(t *testing.T) {
	cache := textImageCache()
	at := time.Date(2026, 11, 1, 5, 49, 22, 0, time.UTC)
	doc := map[string]any{"name": at, "items": []any{map[string]any{"note": newChangingText("x")}}}
	checked, err := CheckTextValues(doc, cache)
	require.NoError(t, err)
	assert.IsType(t, textImage{}, checked["name"])

	want, err := json.Marshal(doc)
	require.NoError(t, err)
	got, err := json.Marshal(checked)
	require.NoError(t, err)
	assert.JSONEq(t, string(want), string(got))
	assert.Equal(t, map[int16][]string{1: {at.String()}, 5: {"x"}}, writtenText(t, checked, cache))
}

// What carries no method-call image is kept as sent: the write takes the same
// image again. So is a value json.Marshal refuses, whose refusal names its
// type, and every position the write does not store as text.
func TestCheckTextValues_KeepsWhatNeedsNoCarrier(t *testing.T) {
	cache := textImageCache()
	stringer := newChangingText("1")
	doc := map[string]any{
		"name":  json.Number("12.5"),
		"tags":  []any{"s", []byte("b"), 7, int64(8), 1.5, true, failingJSON{}},
		"age":   stringer,
		"nope":  stringer,
		"items": []any{nil, map[string]any{"note": nil}},
	}
	checked, err := CheckTextValues(doc, cache)
	require.NoError(t, err)
	assert.Equal(t, doc, checked)
	assert.Zero(t, *stringer.calls, "a position not stored as text is not judged")

	for _, tc := range []map[string]any{nil, {"items": []any(nil)}, {"items": map[string]any(nil)}} {
		checked, err := CheckTextValues(tc, cache)
		require.NoError(t, err)
		assert.Equal(t, tc, checked, "nil stays nil")
	}
}

// A document past the nesting cap is answered as it is: the merged write
// refuses it at its own stage.
func TestCheckTextValues_AnswersADocumentPastTheCapUnchanged(t *testing.T) {
	doc := cyclicDocument()
	checked, err := CheckTextValues(doc, textImageCache())
	require.NoError(t, err)
	checked["marker"] = true
	assert.Equal(t, true, doc["marker"], "the same map")
}
