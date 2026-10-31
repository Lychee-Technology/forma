package sqlutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCheckText(t *testing.T) {
	cases := []struct {
		name, in, wantErr string
	}{
		{"empty", "", ""},
		{"ascii", "plain", ""},
		{"multi-byte and the replacement character", "é中\U0001F600�", ""},
		{"other control characters", "\u0001\t\n", ""},
		{"NUL", "a\x00b", "text cannot contain a NUL character (U+0000)"},
		{"NUL alone", "\x00", "text cannot contain a NUL character (U+0000)"},
		{"invalid byte", "a\xffb", "text must be valid UTF-8"},
		{"truncated sequence", "\xe4\xb8", "text must be valid UTF-8"},
		{"surrogate half", "\xed\xa0\x80", "text must be valid UTF-8"},
	}
	for _, tc := range cases {
		err := CheckText(tc.in)
		if tc.wantErr == "" {
			assert.NoError(t, err, tc.name)
			continue
		}
		assert.EqualError(t, err, tc.wantErr, tc.name)
	}
}
