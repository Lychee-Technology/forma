package transform

import (
	"encoding/json"
)

// textImage stands in an update's document for a value whose text image
// intake judged (CheckUpdateValues), holding the two forms the write reads
// off that value, each taken once (#773): the text image toString stores, and
// the JSON form schema validation judges (schemavalidate.Validate marshals
// the merged document). Replacing the value with its text alone would change
// what validation sees: a time.Time marshals to RFC 3339, while its text
// image is time.String(), so a "format": "date-time" string attribute that
// accepts the time would refuse the text.
//
// It is a fmt.Stringer, so toString answers the held text, and a
// json.Marshaler answering the held JSON, which json.Marshal took from the
// original exactly as validation would have.
type textImage struct {
	text string
	json []byte
}

func (v textImage) String() string { return v.text }

// MarshalJSON answers the JSON form the original marshalled to at intake.
func (v textImage) MarshalJSON() ([]byte, error) { return v.json, nil }

// carryTextImage answers what an update's document holds in place of value,
// whose text image intake has judged to be text. A value whose image is a
// function of the value, a string, []byte, a basic kind or a json.Number, is
// kept: the write takes the same image again. Any other value's image comes
// from a method call, a fmt.Stringer's, an error's or a fmt.Formatter's
// (which %v calls, on the value or on what it holds), and a second call need
// not answer the same text, so the value is replaced by a textImage.
//
// A value json.Marshal refuses is kept too: the validator's refusal of it
// names the value's type (marshalRefusalError), which a textImage would
// hide. Strict validation refuses it before the write converts it; under
// report-only validation, or with no validator, the write takes its text
// image again, so for such a value the stored text can still differ from the
// checked one.
func carryTextImage(value any, text string) any {
	switch value.(type) {
	case string, []byte, json.Number, textImage, bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, uintptr,
		float32, float64, complex64, complex128:
		return value
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return value
	}
	return textImage{text: text, json: encoded}
}
