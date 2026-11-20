package compaction

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/cdcv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// TestSettleErrorTellsANeverCommittingStagingFromAnUnresolvedOne pins R1-F6
// of #907: a staging the fence proved can never land is reported as final,
// and one still unresolved directs the operator to the generation history
// row of its number, never to the current record, which a later flush may
// have moved past a landed compaction.
func TestSettleErrorTellsANeverCommittingStagingFromAnUnresolvedOne(t *testing.T) {
	token := uuid.New()
	u := &cdcv2.UnresolvedError{Kind: publication.KindCompact, Cur: publication.Record{SchemaID: 7, Generation: 4}, Next: publication.Next{Token: token}}
	c := &Compactor{}

	err := c.settleError(u, errors.Join(cdcv2.ErrAbandoned, errors.New("fenced")))
	require.ErrorIs(t, err, cdcv2.ErrAbandoned)
	require.ErrorContains(t, err, "can never commit")
	require.ErrorContains(t, err, "generation 5 of schema 7 with token "+token.String())
	require.NotContains(t, err.Error(), "unresolved")

	err = c.settleError(u, &cdcv2.UnresolvedError{Kind: publication.KindCompact, Settle: errors.New("db out")})
	require.ErrorIs(t, err, cdcv2.ErrUnresolved)
	require.ErrorContains(t, err, "still unresolved")
	require.ErrorContains(t, err, "the current record does not tell")
	require.ErrorContains(t, err, publication.GenerationTableSuffix+" history row of schema 7 generation 5 carries that token")
	require.NotContains(t, err.Error(), "the record names that token")
}
