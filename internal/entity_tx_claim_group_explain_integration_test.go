package internal

import (
	"context"
	"fmt"
	"regexp"
	"testing"

	"github.com/lychee-technology/forma/internal/declaredindex"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// With an index declared on (partition_key, enqueued_at) (#647), the head
// subquery of a group claim reads that index instead of the schema's whole
// range. Asserted on EXPLAIN of the claim's own candidate select over
// 10,000 analyzed rows: 1,000 partitions of 10, half of each published.
func TestClaimGroupHeadUsesTheDeclaredIndex(t *testing.T) {
	env := newOutboxPG(t)
	planned, err := declaredindex.Plan(env.tables.EntityMain, []declaredindex.SchemaDeclarations{{
		Schema: outboxSchemaName, Attributes: outboxSchemaCache(),
		Declarations: []forma.IndexDeclaration{{Name: "fifo", Attributes: []string{"partition_key", "enqueued_at"}}},
	}})
	require.NoError(t, err)
	require.Len(t, planned, 1)
	_, err = declaredindex.Ensure(env.ctx, env.pool, env.tables.EntityMain, planned)
	require.NoError(t, err)
	for batch := range 10 {
		messages := make([]map[string]any, 0, 1000)
		for i := range 1000 {
			n := batch*1000 + i
			message := partitioned("g", fmt.Sprintf("p%04d", n/10), n)
			if n%10 < 5 {
				message["status"] = "published"
			}
			messages = append(messages, message)
		}
		env.createAll(t, messages)
	}

	sql, args := env.candidateSelect(t, fifo(claimOf("g", "relay", 10)))
	var explained []map[string]any
	require.NoError(t, env.pool.QueryRow(env.ctx, "EXPLAIN (FORMAT JSON) "+sql, args...).Scan(&explained))
	require.Len(t, explained, 1)
	// PostgreSQL names the second use of an alias e_2, and so on: the head
	// subquery appears once in the anchor and once in its change_log branch.
	earlier := regexp.MustCompile(`^e(_[0-9]+)?$`)
	scans := planNodes(explained[0]["Plan"], func(node map[string]any) bool {
		alias, _ := node["Alias"].(string)
		return earlier.MatchString(alias) && node["Relation Name"] == env.tables.EntityMain
	})
	require.NotEmpty(t, scans, "the plan reads the earlier rows e")
	for _, scan := range scans {
		indexes := planNodes(scan, func(node map[string]any) bool { return node["Index Name"] != nil })
		require.NotEmpty(t, indexes, "the scan of e is a %v, not an index scan", scan["Node Type"])
		for _, index := range indexes {
			assert.Equal(t, planned[0].Name, index["Index Name"], "the scan of e reads the declared index")
		}
	}
}

// candidateSelect answers the SQL and args of req's candidate select, as the
// claim builds it inside a Tx.
func (e *outboxPG) candidateSelect(t *testing.T, req *forma.ClaimRequest) (string, []any) {
	t.Helper()
	var query *model.PersistentRecordQuery
	require.NoError(t, e.inTx(func(ctx context.Context, tx forma.Tx) error {
		plan, err := tx.(*txSession).planClaim(req)
		if err == nil {
			query = plan.candidateQuery()
		}
		return err
	}))
	metadata := schemameta.NewMetadataCache()
	require.NoError(t, metadata.RegisterSchema(outboxSchemaName, outboxSchemaID, outboxSchemaCache()))
	capture := &statementCapture{}
	_, err := NewDBPersistentRecordRepository(capture, metadata).QueryPersistentRecords(e.ctx, query)
	require.ErrorIs(t, err, errStatementCaptured)
	return capture.sql, capture.args
}

// planNodes answers the nodes of an EXPLAIN (FORMAT JSON) plan tree, root
// included, that match.
func planNodes(plan any, match func(map[string]any) bool) []map[string]any {
	node, ok := plan.(map[string]any)
	if !ok {
		return nil
	}
	var found []map[string]any
	if match(node) {
		found = append(found, node)
	}
	children, _ := node["Plans"].([]any)
	for _, child := range children {
		found = append(found, planNodes(child, match)...)
	}
	return found
}
