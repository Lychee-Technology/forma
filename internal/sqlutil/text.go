package sqlutil

import (
	"errors"
	"strings"
	"unicode/utf8"
)

// CheckText refuses a string PostgreSQL text cannot hold: one with a NUL
// character, or bytes that are not valid UTF-8. Bound as a parameter, such a
// string fails its statement with 22021, which aborts the transaction
// around it. It is the one rule for every caller-supplied string the store
// binds as text: a condition operand (sqlgen, #674) and a written value
// (transform's storage fit, #679), so a write refuses exactly the values a
// query refuses as operands.
//
// The error describes the caller's string and names no attribute; callers
// add the attribute and publish it as invalid input.
func CheckText(s string) error {
	if strings.IndexByte(s, 0) >= 0 {
		return errors.New("text cannot contain a NUL character (U+0000)")
	}
	if !utf8.ValidString(s) {
		return errors.New("text must be valid UTF-8")
	}
	return nil
}
