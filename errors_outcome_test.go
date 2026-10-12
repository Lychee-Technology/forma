package forma

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStorageOutcomeErrorsIsMatrix pins the #645 "Required semantics" table:
// the two conflict species satisfy ErrConflict so pre-#645 callers keep
// matching, while a retryable conflict and an unknown commit deliberately do
// not. Every cell is asserted bare and behind a wrap, and each outcome matches
// only itself among the four.
func TestStorageOutcomeErrorsIsMatrix(t *testing.T) {
	outcomes := []error{ErrUniqueConflict, ErrPreconditionFailed, ErrRetryableConflict, ErrCommitOutcomeUnknown}
	tests := []struct {
		sentinel     error
		conflict     bool
		notFound     bool
		invalidInput bool
	}{
		{sentinel: ErrUniqueConflict, conflict: true},
		{sentinel: ErrPreconditionFailed, conflict: true},
		{sentinel: ErrRetryableConflict},
		{sentinel: ErrCommitOutcomeUnknown},
	}
	for _, tt := range tests {
		for _, shape := range []struct {
			name string
			err  error
		}{
			{"bare", tt.sentinel},
			{"wrapped", fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", tt.sentinel))},
		} {
			t.Run(tt.sentinel.Error()+"/"+shape.name, func(t *testing.T) {
				assert.Equal(t, tt.conflict, errors.Is(shape.err, ErrConflict), "ErrConflict")
				assert.Equal(t, tt.notFound, errors.Is(shape.err, ErrNotFound), "ErrNotFound")
				assert.Equal(t, tt.invalidInput, errors.Is(shape.err, ErrInvalidInput), "ErrInvalidInput")
				for _, other := range outcomes {
					assert.Equal(t, other == tt.sentinel, errors.Is(shape.err, other), "against %q", other)
				}
			})
		}
	}

	t.Run("the generic conflict is no species", func(t *testing.T) {
		assert.NotErrorIs(t, ErrConflict, ErrUniqueConflict)
		assert.NotErrorIs(t, ErrConflict, ErrPreconditionFailed)
	})
}

// TestIsRetryableTruthTable pins invariant 2 of #645: only a known rollback is
// a safe retry, and a chain that also says the commit may have landed is never
// one — whatever else it carries.
func TestIsRetryableTruthTable(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"retryable conflict", ErrRetryableConflict, true},
		{"retryable conflict wrapped", fmt.Errorf("write: %w", ErrRetryableConflict), true},
		{"unique conflict", UniqueConflictf("exists"), false},
		{"precondition failed", PreconditionFailedf("changed"), false},
		{"commit outcome unknown", ErrCommitOutcomeUnknown, false},
		{"unknown beside retryable", errors.Join(ErrRetryableConflict, ErrCommitOutcomeUnknown), false},
		{"generic conflict", ErrConflict, false},
		{"not found", ErrNotFound, false},
		{"invalid input", ErrInvalidInput, false},
		{"deadline", context.DeadlineExceeded, false},
		{"plain storage error", errors.New("connection refused"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsRetryable(tt.err))
		})
	}
}

// TestConflictSpeciesCarriersPublish pins that the two new carriers follow
// #313 exactly like Conflictf: the sentinel classifies, the formatted message
// is the only text that resolves for a public transport, and Error() keeps
// the "<message>: <sentinel>" operator shape.
func TestConflictSpeciesCarriersPublish(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		sentinel  error
		wantError string
	}{
		{"unique conflict", UniqueConflictf("row %d already exists", 7), ErrUniqueConflict, "row 7 already exists: unique conflict"},
		{"precondition failed", PreconditionFailedf("row %d changed", 7), ErrPreconditionFailed, "row 7 changed: precondition failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.ErrorIs(t, tt.err, tt.sentinel)
			require.ErrorIs(t, tt.err, ErrConflict)
			assert.Equal(t, tt.wantError, tt.err.Error())

			msg, ok := ResolvePublicMessage(fmt.Errorf("operator prefix: %w", tt.err))
			require.True(t, ok, "a conflict species carrier must publish through a plain wrap")
			assert.Equal(t, tt.err.(PublicError).PublicMessage(), msg)

			withDetail := WithOperatorDetail(tt.err, errors.New("Key (ltbase_row_id)=(x) already exists"))
			assert.True(t, HasOperatorDetail(withDetail), "WithOperatorDetail must decorate a species carrier")
			msg, ok = ResolvePublicMessage(withDetail)
			require.True(t, ok)
			assert.NotContains(t, msg, "ltbase_row_id")
		})
	}
}
