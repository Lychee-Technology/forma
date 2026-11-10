package rfc3339migrate

import (
	"testing"

	"github.com/google/uuid"

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
		_, _, err = BuildCandidatesQuery("entity_main", target, BatchSize, nil)
		require.Error(t, err)
		_, _, err = BuildRewriteStatement("entity_main", target, []uuid.UUID{uuid.Nil}, []string{"2024-01-02T03:04:05Z"})
		require.Error(t, err)
		_, _, err = BuildOthersQuery("entity_main", target)
		require.Error(t, err)
	}
}

// TestCandidatesQueryShape pins a page of the rewrite's candidates: the
// whole-second shape, in row-id order, bounded by the batch, after the row
// the previous page ended on.
func TestCandidatesQueryShape(t *testing.T) {
	target := Target{SchemaName: "events", SchemaID: 300, AttrID: 1, AttrName: "due", Column: forma.MainColumnText02}
	query, args, err := BuildCandidatesQuery("entity_main", target, 1000, nil)
	require.NoError(t, err)
	require.Equal(t, `SELECT ltbase_row_id, "text_02" FROM "entity_main"
WHERE ltbase_schema_id = $1 AND "text_02" ~ $2
ORDER BY ltbase_row_id LIMIT $3`, query)
	require.Equal(t, []any{int16(300), `^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$`, 1000}, args)

	after := uuid.MustParse("00000000-0000-0000-0000-000000000007")
	query, args, err = BuildCandidatesQuery("entity_main", target, 1000, &after)
	require.NoError(t, err)
	require.Equal(t, `SELECT ltbase_row_id, "text_02" FROM "entity_main"
WHERE ltbase_schema_id = $1 AND "text_02" ~ $2 AND ltbase_row_id > $4
ORDER BY ltbase_row_id LIMIT $3`, query)
	require.Equal(t, []any{int16(300), `^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$`, 1000, after}, args)
}

// TestRewriteStatementShape pins the rewrite: the first 19 characters plus
// .000Z, only on the listed rows that still hold the listed image.
func TestRewriteStatementShape(t *testing.T) {
	target := Target{SchemaName: "events", SchemaID: 300, AttrID: 1, AttrName: "due", Column: forma.MainColumnText02}
	ids := []uuid.UUID{uuid.MustParse("00000000-0000-0000-0000-000000000001")}
	query, args, err := BuildRewriteStatement("entity_main", target, ids, []string{"2024-01-02T03:04:05Z"})
	require.NoError(t, err)
	require.Equal(t, `UPDATE "entity_main" SET "text_02" = left("text_02", 19) || '.000Z'
WHERE ltbase_schema_id = $1 AND (ltbase_row_id, "text_02") IN (
  SELECT u.ltbase_row_id, u.image FROM unnest($2::uuid[], $3::text[]) AS u(ltbase_row_id, image))`, query)
	require.Equal(t, []any{int16(300), ids, []string{"2024-01-02T03:04:05Z"}}, args)

	_, _, err = BuildRewriteStatement("entity_main", target, ids, nil)
	require.ErrorContains(t, err, "rewrite batch of 1 row id(s) and 0 image(s)")
}

// TestRewritable: the rewrite takes a whole-second image only when the read
// parses it, so the textual rewrite yields the canonical image of its
// instant. A whole-second-shaped image naming no instant is left alone.
func TestRewritable(t *testing.T) {
	for image, want := range map[string]bool{
		"2024-01-02T03:04:05Z":      true,
		"0000-01-01T00:00:00Z":      true,
		"9999-12-31T23:59:59Z":      true,
		"2024-02-29T00:00:00Z":      true,
		"2023-02-29T00:00:00Z":      false,
		"2024-02-30T03:04:05Z":      false,
		"2024-13-02T03:04:05Z":      false,
		"2024-01-02T24:00:00Z":      false,
		"2024-01-02T03:60:05Z":      false,
		"2024-01-02T03:04:60Z":      false,
		"2024-01-02T03:04:05.000Z":  false,
		"2024-01-02T11:04:05+08:00": false,
		"not a time":                false,
	} {
		require.Equal(t, want, Rewritable(image), image)
	}
}
