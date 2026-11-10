package rfc3339migrate

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemaledger"
)

// TestTargetsOfReadsTheLedgerEncoding: the targets are the entries an active
// generation records with iso8601, retired ones included, whatever the
// attributes files now declare.
func TestTargetsOfReadsTheLedgerEncoding(t *testing.T) {
	gen := schemaledger.Generation{SchemaID: 300, Number: 4, Entries: []schemaledger.Entry{
		{AttrID: 1, Name: "due", ValueType: forma.ValueTypeDateTime, Column: forma.MainColumnText02, Encoding: forma.MainColumnEncodingLegacyISO8601},
		{AttrID: 2, Name: "at", ValueType: forma.ValueTypeDateTime, Column: forma.MainColumnText03, Encoding: forma.MainColumnEncodingRFC3339},
		{AttrID: 3, Name: "old", ValueType: forma.ValueTypeDate, Column: forma.MainColumnText04, Encoding: forma.MainColumnEncodingLegacyISO8601, Retired: true},
		{AttrID: 4, Name: "n", ValueType: forma.ValueTypeBigInt, Column: forma.MainColumnBigint01, Encoding: forma.MainColumnEncodingDefault},
		{AttrID: 5, Name: "note", ValueType: forma.ValueTypeText},
	}}
	require.Equal(t, []Target{
		{SchemaName: "events", SchemaID: 300, AttrID: 1, AttrName: "due", Column: forma.MainColumnText02},
		{SchemaName: "events", SchemaID: 300, AttrID: 3, AttrName: "old", Column: forma.MainColumnText04},
	}, TargetsOf("events", gen))
}

// TestStatementsRefuseAColumnThatIsNotText: the column comes from the ledger
// and is spliced into SQL, so only a text column of entity_main renders.
func TestStatementsRefuseAColumnThatIsNotText(t *testing.T) {
	for _, col := range []forma.MainColumn{forma.MainColumnBigint01, "text_99", `text_01; DROP TABLE x`} {
		target := Target{SchemaName: "events", SchemaID: 300, AttrID: 1, AttrName: "due", Column: col}
		_, _, err := BuildCountQuery("entity_main", target)
		require.ErrorContains(t, err, "is not a text column of entity_main")
		_, _, err = BuildRewriteStatement("entity_main", target, BatchSize)
		require.Error(t, err)
		_, _, err = BuildOthersQuery("entity_main", target)
		require.Error(t, err)
	}
}

// TestRewriteStatementShape pins the rewrite: the first 19 characters plus
// .000Z, bounded by the batch, guarded by the pattern on both sides.
func TestRewriteStatementShape(t *testing.T) {
	target := Target{SchemaName: "events", SchemaID: 300, AttrID: 1, AttrName: "due", Column: forma.MainColumnText02}
	query, args, err := BuildRewriteStatement("entity_main", target, 1000)
	require.NoError(t, err)
	require.Equal(t, `UPDATE "entity_main" SET "text_02" = left("text_02", 19) || '.000Z'
WHERE ltbase_schema_id = $1 AND "text_02" ~ $2 AND ltbase_row_id IN (
  SELECT ltbase_row_id FROM "entity_main" WHERE ltbase_schema_id = $1 AND "text_02" ~ $2 ORDER BY ltbase_row_id LIMIT $3)`, query)
	require.Equal(t, []any{int16(300), `^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$`, 1000}, args)
}
