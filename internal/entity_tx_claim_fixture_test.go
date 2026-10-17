package internal

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// The outbox653 schema and fixture of the claim tests (#653): LTFlow's outbox
// message, with its claim condition over a column-bound status and lease and
// an EAV retry time, over temporary tables on real PostgreSQL. Post-state is
// read by direct SQL or through the pool, never through the Tx under test.

const (
	outboxSchemaName       = "outbox653"
	outboxSchemaID   int16 = 653
	// outboxLease is the lease a claim takes, LTFlow's relay lease.
	outboxLease = 30 * time.Second
	// heldLease is the lease of the contention tests' claims: long enough
	// that no lease expires while a test runs, so a row claimed twice is a
	// broken claim, never a lease that legitimately ran out.
	heldLease = time.Hour
)

func outboxSchemaCache() forma.SchemaAttributeCache {
	bind := func(col string, enc forma.MainColumnEncoding) *forma.MainColumnBinding {
		return &forma.MainColumnBinding{ColumnName: forma.MainColumn(col), Encoding: enc}
	}
	return forma.SchemaAttributeCache{
		"partition_key":   {AttributeID: 1, ValueType: forma.ValueTypeText, ColumnBinding: bind("text_01", "")},
		"status":          {AttributeID: 2, ValueType: forma.ValueTypeText, ColumnBinding: bind("text_02", "")},
		"topic":           {AttributeID: 3, ValueType: forma.ValueTypeText},
		"enqueued_at":     {AttributeID: 4, ValueType: forma.ValueTypeDateTime, ColumnBinding: bind("bigint_01", forma.MainColumnEncodingUnixMs)},
		"locked_until":    {AttributeID: 5, ValueType: forma.ValueTypeDateTime, ColumnBinding: bind("bigint_02", forma.MainColumnEncodingUnixMs)},
		"lock_generation": {AttributeID: 6, ValueType: forma.ValueTypeBigInt, ColumnBinding: bind("bigint_03", "")},
		"next_attempt_at": {AttributeID: 7, ValueType: forma.ValueTypeDateTime},
		"locked_by":       {AttributeID: 8, ValueType: forma.ValueTypeText},
		"attempts":        {AttributeID: 9, ValueType: forma.ValueTypeInteger, ColumnBinding: bind("integer_01", "")},
	}
}

type outboxPG struct{ *condPG }

func newOutboxPG(t *testing.T) *outboxPG {
	t.Helper()
	return &outboxPG{newSchemaPG(t, outboxSchemaName, outboxSchemaID, outboxSchemaCache())}
}

// outboxBase is the instant fixture messages are created from: far enough in
// the past that every next_attempt_at derived from it is due.
var outboxBase = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// pendingMessage answers a due pending message of topic, created at the i-th
// millisecond after outboxBase.
func pendingMessage(topic string, i int) map[string]any {
	created := outboxBase.Add(time.Duration(i) * time.Millisecond).Format(time.RFC3339Nano)
	return map[string]any{"partition_key": "p", "status": "pending", "topic": topic, "enqueued_at": created,
		"next_attempt_at": created, "lock_generation": int64(0), "attempts": 0}
}

// due is LTFlow's claim condition, restricted to topic: pending or failed
// and due for a retry, or publishing with an expired lease.
func due(topic string) forma.Condition {
	return &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{
		&forma.CompositeCondition{Logic: forma.LogicOr, Conditions: []forma.Condition{
			&forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{
				&forma.KvCondition{Attr: "status", Value: `in:["pending","failed"]`},
				&forma.KvCondition{Attr: "next_attempt_at", Value: "lte:$now"},
			}},
			&forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{
				&forma.KvCondition{Attr: "status", Value: "equals:publishing"},
				&forma.KvCondition{Attr: "locked_until", Value: "lte:$now"},
			}},
		}},
		&forma.KvCondition{Attr: "topic", Value: "equals:" + topic},
	}}
}

// claimOf answers LTFlow's relay claim of topic by worker: publishing, a
// lease from the Tx clock and the next generation.
func claimOf(topic, worker string, limit int) *forma.ClaimRequest {
	return claimLeasing(topic, worker, limit, outboxLease)
}

// claimLeasing is claimOf with a lease of its own.
func claimLeasing(topic, worker string, limit int, lease time.Duration) *forma.ClaimRequest {
	return &forma.ClaimRequest{
		SchemaName: outboxSchemaName,
		Condition:  due(topic),
		Sort:       []forma.OrderBy{{Attribute: "enqueued_at"}},
		Limit:      limit,
		Updates:    map[string]any{"status": "publishing", "locked_by": worker},
		UpdateOps:  []forma.UpdateOp{forma.Increment("lock_generation", 1), forma.ServerTime("locked_until", lease)},
	}
}

// createWithID creates a message under the caller's row ID (#651).
func (e *outboxPG) createWithID(t *testing.T, rowID uuid.UUID, data map[string]any) uuid.UUID {
	t.Helper()
	record, err := e.em.Create(e.ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: outboxSchemaName, RowID: rowID}, Data: data})
	require.NoError(t, err)
	require.Equal(t, rowID, record.RowID)
	return rowID
}

func (e *outboxPG) create(t *testing.T, data map[string]any) uuid.UUID {
	t.Helper()
	record, err := e.em.Create(e.ctx, &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: outboxSchemaName}, Data: data})
	require.NoError(t, err)
	return record.RowID
}

// createMessages creates n pending messages of topic in one batch and
// answers their row IDs in enqueued_at order.
func (e *outboxPG) createMessages(t *testing.T, topic string, n int) []uuid.UUID {
	t.Helper()
	messages := make([]map[string]any, n)
	for i := range messages {
		messages[i] = pendingMessage(topic, i)
	}
	return e.createAll(t, messages)
}

// createAll creates messages in one batch and answers their row IDs in
// order.
func (e *outboxPG) createAll(t *testing.T, messages []map[string]any) []uuid.UUID {
	t.Helper()
	return e.createBatch(t, messages, nil)
}

// createDescending is createAll under caller row IDs (#651) that descend as
// messages ascend, so a row's ID order is the reverse of its place in
// messages: a test whose messages are in enqueued order then tells the
// group order from the row-ID tiebreak. tag keeps one test's IDs apart.
func (e *outboxPG) createDescending(t *testing.T, tag int, messages []map[string]any) []uuid.UUID {
	t.Helper()
	rowIDs := make([]uuid.UUID, len(messages))
	for i := range messages {
		rowIDs[i] = uuid.MustParse(fmt.Sprintf("0190b6a1-0000-7000-8000-%02d%010d", tag, len(messages)-i))
	}
	return e.createBatch(t, messages, rowIDs)
}

// createBatch creates messages in one atomic batch, under rowIDs when it is
// not nil.
func (e *outboxPG) createBatch(t *testing.T, messages []map[string]any, rowIDs []uuid.UUID) []uuid.UUID {
	t.Helper()
	n := len(messages)
	ops := make([]forma.EntityOperation, n)
	for i, data := range messages {
		ops[i] = forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: outboxSchemaName},
			Type: forma.OperationCreate, Data: data}
		if rowIDs != nil {
			ops[i].RowID = rowIDs[i]
		}
	}
	result, err := e.em.BatchCreate(e.ctx, &forma.BatchOperation{Operations: ops, Atomic: true})
	require.NoError(t, err)
	require.Len(t, result.Successful, n)
	// The test tables are fresh, so autovacuum has not analyzed them; without
	// statistics the candidate select plans as nested loops and slows with
	// every claim until leases expire mid-test.
	for _, table := range []string{e.tables.EntityMain, e.tables.EAVData, e.tables.ChangeLog} {
		_, err := e.pool.Exec(e.ctx, "ANALYZE "+sanitizeIdentifier(table))
		require.NoError(t, err)
	}
	ids := make([]uuid.UUID, n)
	for i, record := range result.Successful {
		ids[i] = record.RowID
		if rowIDs != nil {
			require.Equal(t, rowIDs[i], record.RowID)
		}
	}
	return ids
}

// claim runs one Claim in its own Tx and commits it.
func (e *outboxPG) claim(req *forma.ClaimRequest) ([]*forma.DataRecord, error) {
	var records []*forma.DataRecord
	err := e.inTx(func(ctx context.Context, tx forma.Tx) error {
		result, err := tx.Claim(ctx, req)
		if err != nil {
			return err
		}
		records = result.Records
		return nil
	})
	return records, err
}

// stored is a message's claim state, read by direct SQL.
type stored struct {
	status      string
	generation  int64
	lockedUntil *int64
	lockedBy    *string
}

func (e *outboxPG) stored(t *testing.T, rowID uuid.UUID) stored {
	t.Helper()
	var s stored
	require.NoError(t, e.pool.QueryRow(e.ctx, fmt.Sprintf(`SELECT m.text_02, m.bigint_03, m.bigint_02,
		(SELECT value_text FROM %s v WHERE v.schema_id = $1 AND v.row_id = $2 AND v.attr_id = 8)
		FROM %s m WHERE m.ltbase_schema_id = $1 AND m.ltbase_row_id = $2`,
		sanitizeIdentifier(e.tables.EAVData), sanitizeIdentifier(e.tables.EntityMain)),
		outboxSchemaID, rowID).Scan(&s.status, &s.generation, &s.lockedUntil, &s.lockedBy))
	return s
}

// requireStoredAs asserts record carries the row's committed claim state.
func (e *outboxPG) requireStoredAs(t *testing.T, record *forma.DataRecord) {
	t.Helper()
	s := e.stored(t, record.RowID)
	require.Equal(t, s.status, record.Attributes["status"], "status (entity_main text_02)")
	require.EqualValues(t, s.generation, record.Attributes["lock_generation"], "lock_generation (entity_main bigint_03)")
	require.NotNil(t, s.lockedUntil)
	lockedUntil, ok := record.Attributes["locked_until"].(time.Time)
	require.True(t, ok, "locked_until is a datetime, got %T", record.Attributes["locked_until"])
	require.Equal(t, *s.lockedUntil, lockedUntil.UnixMilli(), "locked_until (entity_main bigint_02)")
	require.NotNil(t, s.lockedBy)
	require.Equal(t, *s.lockedBy, record.Attributes["locked_by"], "locked_by (EAV)")
}

// expireLease moves rowID's lease into the past with a CAS Update on its
// generation, as LTFlow's lease takeover test does, without sleeping.
func (e *outboxPG) expireLease(t *testing.T, rowID uuid.UUID, generation int64) {
	t.Helper()
	_, err := e.em.Update(e.ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: outboxSchemaName, RowID: rowID},
		Preconditions:    &forma.KvCondition{Attr: "lock_generation", Value: fmt.Sprintf("equals:%d", generation)},
		UpdateOps:        []forma.UpdateOp{forma.ServerTime("locked_until", -time.Minute)},
	})
	require.NoError(t, err)
}

// complete is LTFlow's MarkPublished: a CAS Update on the generation the
// holder claimed.
func complete(rowID uuid.UUID, generation int64) *forma.EntityOperation {
	return &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: outboxSchemaName, RowID: rowID},
		Preconditions:    &forma.KvCondition{Attr: "lock_generation", Value: fmt.Sprintf("equals:%d", generation)},
		Updates:          map[string]any{"status": "published"},
	}
}

func rowIDsOf(records []*forma.DataRecord) []uuid.UUID {
	ids := make([]uuid.UUID, len(records))
	for i, record := range records {
		ids[i] = record.RowID
	}
	return ids
}

func generationOf(t *testing.T, record *forma.DataRecord) int64 {
	t.Helper()
	g, ok := record.Attributes["lock_generation"].(int64)
	require.True(t, ok, "lock_generation is a bigint, got %T", record.Attributes["lock_generation"])
	return g
}
