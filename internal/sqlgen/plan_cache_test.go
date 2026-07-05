package sqlgen

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHashShapePartsBoundaries(t *testing.T) {
	require.NotEqual(t, HashShapeParts("ab", "c"), HashShapeParts("a", "bc"))
	require.Equal(t, HashShapeParts("a", "b"), HashShapeParts("a", "b"))
	require.NotEqual(t, HashShapeParts("a"), HashShapeParts("a", ""))
}
