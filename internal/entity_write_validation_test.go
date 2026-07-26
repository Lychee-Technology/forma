package internal

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// TestNewEntityManagerAcceptsNilValidator pins that validation is opt-in at the
// wiring layer. Both e2e harnesses and every existing test construct a manager
// without a validator, and none of them may start failing.
func TestNewEntityManagerAcceptsNilValidator(t *testing.T) {
	require.NotPanics(t, func() {
		_ = NewEntityManager(nil, nil, nil, nil, forma.DefaultConfig(nil), nil)
	})
}
