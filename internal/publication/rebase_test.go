package publication

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRebaseTable pins every writer kind against every kind it can find
// committed in between, and the epoch rule above them.
func TestRebaseTable(t *testing.T) {
	from := Record{SchemaID: 100, Epoch: 1, Generation: 4}
	head := Record{SchemaID: 100, Epoch: 1, Generation: 5}
	allowed := map[Kind]map[Kind]bool{
		KindFlush:   {KindFlush: true, KindCompact: true},
		KindCompact: {KindFlush: true},
		KindInit:    {KindCompact: true},
	}
	for writer, over := range allowed {
		for _, between := range []Kind{KindFlush, KindCompact, KindInit} {
			err := Rebase(writer, from, head, []Kind{between})
			if over[between] {
				require.NoError(t, err, "%s over %s", writer, between)
			} else {
				require.ErrorIs(t, err, ErrRebaseRefused, "%s over %s", writer, between)
			}
		}
		require.NoError(t, Rebase(writer, from, head, nil), "%s over nothing", writer)
		require.ErrorIs(t, Rebase(writer, from, Record{SchemaID: 100, Epoch: 2, Generation: 4}, nil), ErrRebaseRefused, "%s across an epoch", writer)
	}
	require.ErrorIs(t, Rebase(Kind("vacuum"), from, head, nil), ErrRebaseRefused)
}
