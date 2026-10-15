package uniquekey

import (
	"encoding/json"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustEncode(t *testing.T, components ...Component) string {
	t.Helper()
	encoded, err := Encode(components)
	require.NoError(t, err)
	return encoded
}

func one(vt forma.ValueType, v any) Component { return Component{Type: vt, Value: v} }

// Equal typed values encode identically however they arrived: the stored
// decoding and every JSON spelling of one value (#651 rule 7).
func TestEncodeEqualTypedValuesEncodeIdentically(t *testing.T) {
	id := uuid.MustParse("0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b")
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	groups := map[string][]Component{
		"integer": {one(forma.ValueTypeInteger, 1), one(forma.ValueTypeInteger, int64(1)), one(forma.ValueTypeInteger, 1.0),
			one(forma.ValueTypeInteger, json.Number("1")), one(forma.ValueTypeInteger, "1"), one(forma.ValueTypeInteger, int16(1))},
		"numeric":       {one(forma.ValueTypeNumeric, 1.5), one(forma.ValueTypeNumeric, "1.5"), one(forma.ValueTypeNumeric, json.Number("1.50"))},
		"numeric whole": {one(forma.ValueTypeNumeric, 2), one(forma.ValueTypeNumeric, 2.0), one(forma.ValueTypeNumeric, "2")},
		"numeric zero":  {one(forma.ValueTypeNumeric, 0.0), one(forma.ValueTypeNumeric, math.Copysign(0, -1))},
		"datetime": {one(forma.ValueTypeDateTime, at), one(forma.ValueTypeDateTime, "2026-01-01T00:00:00Z"),
			one(forma.ValueTypeDateTime, "2026-01-01T09:00:00+09:00"), one(forma.ValueTypeDateTime, "2026-01-01T00:00:00.0004Z")},
		"date": {one(forma.ValueTypeDate, at), one(forma.ValueTypeDate, "2026-01-01")},
		"uuid": {one(forma.ValueTypeUUID, id), one(forma.ValueTypeUUID, strings.ToUpper(id.String())), one(forma.ValueTypeUUID, id.String())},
		"bool": {one(forma.ValueTypeBool, true), one(forma.ValueTypeBool, true)},
	}
	for name, group := range groups {
		t.Run(name, func(t *testing.T) {
			want := mustEncode(t, group[0])
			for _, c := range group[1:] {
				assert.Equal(t, want, mustEncode(t, c), "%T %v", c.Value, c.Value)
			}
		})
	}
	assert.Equal(t, "1:1", mustEncode(t, one(forma.ValueTypeInteger, "1")), "the issue's 1 and \"1\" on an integer attribute")
	assert.Equal(t, "36:"+id.String(), mustEncode(t, one(forma.ValueTypeUUID, id)))
	assert.Equal(t, "13:1767225600000", mustEncode(t, one(forma.ValueTypeDateTime, at)), "unix milliseconds")
	assert.Equal(t, "1:t", mustEncode(t, one(forma.ValueTypeBool, true)))
	assert.Equal(t, "1:f", mustEncode(t, one(forma.ValueTypeBool, false)))
}

// Text compares byte-exact: no case folding, trimming or normalization.
func TestEncodeTextIsByteExact(t *testing.T) {
	distinct := []string{"a", "A", "a ", " a", "é", "é", ""}
	seen := map[string]string{}
	for _, s := range distinct {
		encoded := mustEncode(t, one(forma.ValueTypeText, s))
		if other, dup := seen[encoded]; dup {
			t.Fatalf("%q and %q encode alike: %q", s, other, encoded)
		}
		seen[encoded] = s
	}
}

// Length prefixes keep the component boundaries: tuples whose texts
// concatenate alike never encode alike.
func TestEncodeLengthPrefixesKeepBoundaries(t *testing.T) {
	text := func(values ...string) []Component {
		components := make([]Component, len(values))
		for i, v := range values {
			components[i] = one(forma.ValueTypeText, v)
		}
		return components
	}
	pairs := [][2][]Component{
		{text("ab", "c"), text("a", "bc")},
		{text("", "a"), text("a", "")},
		{text("1:a"), text("1", "a")},
		{text("2:ab"), text("ab", "")},
		{text("a", "1:b"), text("a1:", "b")},
	}
	for _, p := range pairs {
		assert.NotEqual(t, mustEncode(t, p[0]...), mustEncode(t, p[1]...))
	}
	assert.Equal(t, "2:ab1:c", mustEncode(t, text("ab", "c")...))
}

// An encoding of 512 bytes is stored as itself; one byte more is digested.
func TestEncodeDigestSwitchesAt512Bytes(t *testing.T) {
	// One text component of length n encodes to len("n:") + n bytes.
	atLimit := mustEncode(t, one(forma.ValueTypeText, strings.Repeat("x", 508)))
	assert.Len(t, atLimit, MaxInlineValueBytes)
	assert.True(t, strings.HasPrefix(atLimit, "508:"))

	over := mustEncode(t, one(forma.ValueTypeText, strings.Repeat("x", 509)))
	assert.True(t, strings.HasPrefix(over, DigestPrefix), over)
	assert.Len(t, over, len(DigestPrefix)+64)
	assert.Equal(t, over, mustEncode(t, one(forma.ValueTypeText, strings.Repeat("x", 509))), "the digest is deterministic")
	assert.NotEqual(t, over, mustEncode(t, one(forma.ValueTypeText, strings.Repeat("x", 508)+"y")))
}

// decodeInline splits an inline encoding back into its canonical texts; it
// fails unless the length prefixes parse the whole value.
func decodeInline(t *testing.T, encoded string) []string {
	t.Helper()
	var texts []string
	for encoded != "" {
		colon := strings.IndexByte(encoded, ':')
		require.Positive(t, colon, "a component starts with its length: %q", encoded)
		n, err := strconv.Atoi(encoded[:colon])
		require.NoError(t, err)
		require.LessOrEqual(t, colon+1+n, len(encoded))
		texts = append(texts, encoded[colon+1:colon+1+n])
		encoded = encoded[colon+1+n:]
	}
	return texts
}

// A seeded corpus of mixed tuples, drawn from small alphabets rich in digits
// and colons so prefixes and contents collide often: distinct canonical
// tuples never share an encoding, and every inline encoding decodes back to
// its tuple.
func TestEncodeDistinctTuplesNeverCollide(t *testing.T) {
	rng := rand.New(rand.NewPCG(651, 593))
	alphabet := []rune("ab1:0 é")
	randomText := func() string {
		n := rng.IntN(8)
		if rng.IntN(20) == 0 {
			n = 500 + rng.IntN(40) // around the digest switch
		}
		runes := make([]rune, n)
		for i := range runes {
			runes[i] = alphabet[rng.IntN(len(alphabet))]
		}
		return string(runes)
	}
	randomComponent := func() Component {
		switch rng.IntN(5) {
		case 0:
			return one(forma.ValueTypeInteger, rng.Int64N(200)-100)
		case 1:
			return one(forma.ValueTypeNumeric, float64(rng.IntN(40)-20)/4)
		case 2:
			return one(forma.ValueTypeBool, rng.IntN(2) == 0)
		default:
			return one(forma.ValueTypeText, randomText())
		}
	}
	byEncoding := map[string]string{}
	for i := 0; i < 50000; i++ {
		components := make([]Component, 1+rng.IntN(4))
		texts := make([]string, len(components))
		for j := range components {
			components[j] = randomComponent()
			text, err := canonical(components[j].Type, components[j].Value)
			require.NoError(t, err)
			texts[j] = text
		}
		identity := strings.Join(texts, "\x00") + "\x00" + strconv.Itoa(len(texts))
		encoded := mustEncode(t, components...)
		if other, seen := byEncoding[encoded]; seen && other != identity {
			t.Fatalf("tuples %q and %q share encoding %q", other, identity, encoded)
		}
		byEncoding[encoded] = identity
		if !strings.HasPrefix(encoded, DigestPrefix) {
			require.Equal(t, texts, decodeInline(t, encoded))
		}
	}
}

func TestEncodeRefusesValuesWithoutACanonicalForm(t *testing.T) {
	cases := []Component{
		one(forma.ValueTypeText, nil),
		one(forma.ValueTypeText, 7),
		one(forma.ValueTypeInteger, 1.5),
		one(forma.ValueTypeInteger, "one"),
		one(forma.ValueTypeInteger, math.Inf(1)),
		one(forma.ValueTypeNumeric, math.NaN()),
		one(forma.ValueTypeNumeric, math.Inf(-1)),
		one(forma.ValueTypeNumeric, "many"),
		one(forma.ValueTypeDateTime, "yesterday"),
		one(forma.ValueTypeDate, 20260101),
		one(forma.ValueTypeUUID, "not-a-uuid"),
		one(forma.ValueTypeBool, "true"),
		one(forma.ValueTypeList, []any{"a"}),
	}
	for _, c := range cases {
		_, err := Encode([]Component{one(forma.ValueTypeText, "ok"), c})
		require.Error(t, err, "%s %#v", c.Type, c.Value)
		assert.Contains(t, err.Error(), "component 1 ("+string(c.Type)+")")
	}
}
