package sqlutil

import (
	"regexp"
	"testing"

	"github.com/google/uuid"
)

// TestUUIDTextPatternIsUUIDParse pins the pattern to the read path's parse:
// on every input, a match and a successful uuid.Parse agree.
func TestUUIDTextPatternIsUUIDParse(t *testing.T) {
	const canonical = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	pattern := regexp.MustCompile(UUIDTextPattern)
	for _, s := range []string{
		canonical,
		"6BA7B810-9DAD-11D1-80B4-00C04FD430C8",
		"{" + canonical + "}",
		"x" + canonical + "y",
		"\n" + canonical + "\n",
		"urn:uuid:" + canonical,
		"URN:UUID:" + canonical,
		"6ba7b8109dad11d180b400c04fd430c8",
		"",
		"not-a-uuid",
		canonical[:35],
		canonical + " ",
		" " + canonical,
		"6ba7b810-9dad11d1-80b4-00c04fd430c8",
		"6ba7b81-09dad-11d1-80b4-00c04fd430c8",
		"g" + canonical[1:],
		"{6ba7b8109dad11d180b400c04fd430c8}",
		"urn:uuix:" + canonical,
		"é" + canonical + "}",
		"{" + canonical + "é",
		"a" + canonical + "é",
		"6ba7b810-9dad-11d1-80b4-00c04fd430cé",
	} {
		_, err := uuid.Parse(s)
		if got, want := pattern.MatchString(s), err == nil; got != want {
			t.Errorf("%q: pattern match = %t, uuid.Parse accepts = %t", s, got, want)
		}
	}
}
