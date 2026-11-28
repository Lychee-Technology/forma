package internal

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Unit tests of what an unset target is (#938): a bound value the read
// cannot parse is discarded unparsed, a registered root the document
// declares a container is refused, and a registered literal name is never
// taken for a path spelling.

// unreadableBoundImages are bound text images their encoding does not store,
// as a write around the funnel leaves them: each attribute, its column, the
// image planted there and the read path's refusal of it.
var unreadableBoundImages = []struct {
	attr, column, image, rule string
}{
	{"seen", "text_03", "not a date", "decode rfc3339 encoding: column text_03: parse ISO 8601 date: "},
	{"active", "text_04", "yes", "decode bool_text encoding: column text_04: bool_text value \"yes\" is not the \"1\"/\"0\" the encoding stores"},
}

// TestUnsetDiscardsAnUnreadableBoundImage pins that a bound column's text
// image is held unparsed in the merge base, like an eav_data value (#590):
// an update that keeps it fails with the read path's consistency error, and
// an unset of it, or a value replacing it, never parses it. The other values
// of the row are still read and checked.
func TestUnsetDiscardsAnUnreadableBoundImage(t *testing.T) {
	for _, bound := range unreadableBoundImages {
		t.Run(bound.attr, func(t *testing.T) {
			h := newUnsetHarness(t, unsetStrict)
			seed := func() uuid.UUID {
				doc := unsetBaseDoc()
				doc["seen"], doc["active"] = time.UnixMilli(1700000000000).UTC(), true
				rowID := h.seed(t, doc)
				h.record(rowID).TextItems[bound.column] = bound.image
				return rowID
			}

			rowID := seed()
			_, err := h.update(rowID, map[string]any{"note": "kept"})
			require.Error(t, err)
			require.NotErrorIs(t, err, forma.ErrInvalidInput, "a stored image the read refuses is a consistency error")
			assert.Contains(t, err.Error(), "failed to read attribute "+bound.attr+" of row "+rowID.String()+" from main column: "+bound.rule)

			_, err = h.update(rowID, map[string]any{"note": "changed"}, forma.Unset(bound.attr))
			require.NoError(t, err)
			assert.Equal(t, []string{bound.column}, h.record(rowID).ClearedColumns)
			stored := h.stored(t, rowID)
			assert.NotContains(t, stored, bound.attr)
			assert.Equal(t, "changed", stored["note"])
			other := map[string]string{"seen": "active", "active": "seen"}[bound.attr]
			assert.Contains(t, stored, other, "the other bound image is kept, read and written back")

			rowID = seed()
			replacement := map[string]any{"seen": time.UnixMilli(1704067200123).UTC(), "active": false}[bound.attr]
			_, err = h.update(rowID, map[string]any{bound.attr: replacement})
			require.NoError(t, err, "a value replacing the image never parses it either")
			assert.Equal(t, replacement, h.stored(t, rowID)[bound.attr])
		})
	}
}

// TestUnsetRefusesARegisteredContainer pins that the target's own shape is
// read from the schema document: lines is registered as a list with nothing
// beneath it, and the document declares it an array of objects, so its
// empty-list marker is no list to clear. Every mode refuses it, no validator
// included, while an ordinary list stays clearable.
func TestUnsetRefusesARegisteredContainer(t *testing.T) {
	for _, mode := range unsetModes {
		t.Run(string(mode), func(t *testing.T) {
			h := newUnsetHarness(t, mode)
			doc := unsetBaseDoc()
			doc["lines"] = []any{}
			rowID := h.seed(t, doc)
			before := h.record(rowID)

			_, err := h.update(rowID, nil, forma.Unset("lines"))
			requireUnsetRefusedBeforeRepository(t, h, err, "unset names 'lines', an array of objects in schema 'unsetdoc'")
			assert.Same(t, before, h.record(rowID))

			_, err = h.update(rowID, nil, forma.Unset("tags"))
			require.NoError(t, err)
			assert.NotContains(t, h.stored(t, rowID), "tags")
		})
	}
}

// TestUnsetTakesARegisteredLiteralName pins that the path-spelling screen
// refuses JSONPath syntax, not a character: price$ is the attribute's own
// registered name and is cleared, while JSONPath, indexed and wildcard
// spellings stay refused.
func TestUnsetTakesARegisteredLiteralName(t *testing.T) {
	h := newUnsetHarness(t, unsetStrict)
	doc := unsetBaseDoc()
	doc["price$"] = "9"
	rowID := h.create(t, doc)
	_, err := h.update(rowID, nil, forma.Unset("price$"))
	require.NoError(t, err)
	assert.Equal(t, unsetLogical(t, unsetBaseDoc()), unsetLogical(t, h.stored(t, rowID)))

	for _, spelling := range []string{"$", "$.note", "$note", "$['note']", "tags[0]", "profile.*", "*"} {
		t.Run(spelling, func(t *testing.T) {
			h := newUnsetHarness(t, unsetStrict)
			_, err := h.update(uuid.New(), nil, forma.Unset(spelling))
			requireUnsetRefusedBeforeRepository(t, h, err, "unset names '"+spelling+"', which is not an attribute's dotted name")
		})
	}
}
