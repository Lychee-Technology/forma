package errorid

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestNewIsAParseableUUID pins the shape both write surfaces rely on: the
// httpapi tests uuid.Parse every error_id they read off a body, and an
// operator greps the log for the string verbatim, so the id must be a
// canonical UUID string and nothing looser.
func TestNewIsAParseableUUID(t *testing.T) {
	id := New()
	parsed, err := uuid.Parse(id)
	require.NoError(t, err)
	require.Equal(t, parsed.String(), id, "the id must already be in canonical form")
}

// TestNewDoesNotRepeat is the correlation property: two failures must never
// share a handle, or the join from body to log line becomes ambiguous.
func TestNewDoesNotRepeat(t *testing.T) {
	require.NotEqual(t, New(), New())
}
