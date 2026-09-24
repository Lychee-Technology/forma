// Package errorid mints the correlation id a caller quotes back to an
// operator when a response withholds an error's text.
//
// Two write surfaces publish such an id: internal/httpapi puts one on every
// redacted body and on a disclosed 4xx that withholds operator detail
// (respondErrorWithStatus, #301/#361), and the best-effort batch path puts one
// on every failed forma.OperationError (#398). Each writes the same id on the
// log line that keeps the full error, so the id is the join between what the
// caller saw and what the operator can read. It lives here rather than at
// either call site for the same reason internal/redact does: the shape is a
// contract shared by both surfaces, and a second copy would drift.
package errorid

import "github.com/google/uuid"

// New returns a fresh correlation id: a canonical UUID string, so it parses
// with uuid.Parse and greps verbatim out of a log line.
func New() string {
	return uuid.NewString()
}
