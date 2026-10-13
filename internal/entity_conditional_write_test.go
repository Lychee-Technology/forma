package internal

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/transform"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Unit tests of the conditional-mutation fields (#649) over the mock
// repository: intake refusals happen before any repository call, and the
// operators apply to the locked base the repository hands the merge.

const (
	condSchemaName       = "cond649"
	condSchemaID   int16 = 649
	// condClockMs is the transaction clock the mock repository reports:
	// 2026-10-12T08:15:30.250Z.
	condClockMs int64 = 1791792930250
)

func condSchemaCache() forma.SchemaAttributeCache {
	bind := func(col string, enc forma.MainColumnEncoding) *forma.MainColumnBinding {
		return &forma.MainColumnBinding{ColumnName: forma.MainColumn(col), Encoding: enc}
	}
	return forma.SchemaAttributeCache{
		"name":           {AttributeID: 1, ValueType: forma.ValueTypeText},
		"counter":        {AttributeID: 2, ValueType: forma.ValueTypeInteger},
		"small":          {AttributeID: 3, ValueType: forma.ValueTypeSmallInt},
		"big":            {AttributeID: 4, ValueType: forma.ValueTypeBigInt},
		"version":        {AttributeID: 5, ValueType: forma.ValueTypeBigInt, ColumnBinding: bind("bigint_01", "")},
		"rank":           {AttributeID: 6, ValueType: forma.ValueTypeInteger, ColumnBinding: bind("integer_01", "")},
		"due":            {AttributeID: 7, ValueType: forma.ValueTypeDateTime},
		"day":            {AttributeID: 8, ValueType: forma.ValueTypeDate},
		"due_iso":        {AttributeID: 9, ValueType: forma.ValueTypeDateTime, ColumnBinding: bind("text_01", forma.MainColumnEncodingISO8601)},
		"due_main":       {AttributeID: 10, ValueType: forma.ValueTypeDateTime, ColumnBinding: bind("bigint_02", forma.MainColumnEncodingUnixMs)},
		"stats.attempts": {AttributeID: 11, ValueType: forma.ValueTypeInteger},
		"stats.note":     {AttributeID: 12, ValueType: forma.ValueTypeText},
		"items.qty":      {AttributeID: 13, ValueType: forma.ValueTypeInteger},
	}
}

type condFixture struct {
	ctx        context.Context
	em         forma.EntityManager
	repo       *mockPersistentRecordRepository
	tr         model.PersistentRecordTransformer
	clockReads int
}

func newCondFixture(t *testing.T) *condFixture {
	t.Helper()
	registry := &stubSchemaRegistry{schemaID: condSchemaID, schemaName: condSchemaName, cache: condSchemaCache()}
	f := &condFixture{ctx: context.Background(), repo: newMockPersistentRecordRepository(), tr: transform.NewPersistentRecordTransformer(registry)}
	f.repo.clock = func(context.Context) (int64, error) {
		f.clockReads++
		return condClockMs, nil
	}
	f.em = mustNewEntityManager(t, f.tr, f.repo, nil, registry, createTestConfig(), nil)
	return f
}

func (f *condFixture) seed(t *testing.T, data map[string]any) uuid.UUID {
	t.Helper()
	rowID := uuid.New()
	f.repo.storeRecord(buildPersistentRecord(t, f.tr, condSchemaID, rowID, data))
	return rowID
}

func (f *condFixture) stored(t *testing.T, rowID uuid.UUID) map[string]any {
	t.Helper()
	attrs, err := f.tr.FromPersistentRecord(f.ctx, f.repo.records[condSchemaID][rowID])
	require.NoError(t, err)
	return attrs
}

func condOp(rowID uuid.UUID) *forma.EntityOperation {
	return &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: condSchemaName, RowID: rowID}}
}

// requireRefusedBeforeRepository asserts err is an ErrInvalidInput naming
// want and that no write reached the repository.
func requireRefusedBeforeRepository(t *testing.T, f *condFixture, err error, want string) {
	t.Helper()
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	message, published := forma.ResolvePublicMessage(err)
	require.True(t, published, "an intake refusal publishes its message: %v", err)
	assert.Contains(t, message, want)
	assert.Zero(t, f.repo.mergeCalls, "no merge may run")
	assert.Zero(t, f.repo.deleteCalls, "no delete may run")
	assert.Empty(t, f.repo.insertedRecords, "no insert may run")
	assert.Zero(t, f.repo.batchUpdateCalls, "no batch update may run")
}

func TestConditionalFieldsAreRefusedAtIntake(t *testing.T) {
	precondition := &forma.KvCondition{Attr: "version", Value: "equals:7"}
	cases := []struct {
		name string
		run  func(f *condFixture) error
		want string
	}{
		{"preconditions on create", func(f *condFixture) error {
			op := condOp(uuid.Nil)
			op.Data, op.Preconditions = map[string]any{"name": "x"}, precondition
			_, err := f.em.Create(f.ctx, op)
			return err
		}, "create does not accept preconditions (the precondition tests 'version')"},
		{"update operators on create", func(f *condFixture) error {
			op := condOp(uuid.Nil)
			op.Data, op.UpdateOps = map[string]any{"name": "x"}, []forma.UpdateOp{forma.Increment("counter", 1)}
			_, err := f.em.Create(f.ctx, op)
			return err
		}, "create does not accept update operators (increment on 'counter')"},
		{"update operators on delete", func(f *condFixture) error {
			op := condOp(uuid.New())
			op.UpdateOps = []forma.UpdateOp{forma.ServerTime("due", 0)}
			return f.em.Delete(f.ctx, op)
		}, "delete does not accept update operators (server_time on 'due')"},
		{"attribute in updates and operators", update(map[string]any{"counter": 3}, forma.Increment("counter", 1)),
			"attribute 'counter' appears in both updates (as 'counter')"},
		{"literal dotted spelling in updates", update(map[string]any{"stats.attempts": 3}, forma.Increment("stats.attempts", 1)),
			"attribute 'stats.attempts' appears in both updates (as 'stats.attempts')"},
		{"nested spelling in updates", update(map[string]any{"stats": map[string]any{"attempts": 3}}, forma.Increment("stats.attempts", 1)),
			"attribute 'stats.attempts' appears in both updates (as 'stats.attempts')"},
		{"container replaced by updates", update(map[string]any{"stats": nil}, forma.Increment("stats.attempts", 1)),
			"attribute 'stats.attempts' appears in both updates (as 'stats')"},
		{"duplicate operator", update(nil, forma.Increment("counter", 1), forma.Increment("counter", 2)),
			"attribute 'counter' has more than one update operator"},
		{"increment on text", update(nil, forma.Increment("name", 1)),
			"increment applies to smallint, integer and bigint attributes; 'name' is text"},
		{"increment on datetime", update(nil, forma.Increment("due", 1)),
			"increment applies to smallint, integer and bigint attributes; 'due' is datetime"},
		{"server_time on bigint", update(nil, forma.ServerTime("big", 0)),
			"server_time applies to date and datetime attributes; 'big' is bigint"},
		{"unknown operator attribute", update(nil, forma.Increment("missing", 1)),
			"update operator increment names attribute 'missing', which schema 'cond649' does not define"},
		{"unknown kind", update(nil, forma.UpdateOp{Attribute: "counter", Kind: "append"}),
			"update operator on attribute 'counter' has kind 'append'"},
		{"increment with offset", update(nil, forma.UpdateOp{Attribute: "counter", Kind: forma.UpdateOpIncrement, Offset: time.Second}),
			"increment on attribute 'counter' sets Offset"},
		{"server_time with by", update(nil, forma.UpdateOp{Attribute: "due", Kind: forma.UpdateOpServerTime, By: 1}),
			"server_time on attribute 'due' sets By"},
		{"server_time finer than a millisecond", update(nil, forma.ServerTime("due", time.Microsecond)),
			"server_time offset 1µs on datetime attribute 'due' must be a whole number of milliseconds"},
		{"neither updates nor operators", update(nil), "updates are required for update operation"},
		{"precondition on an unknown attribute", conditional(&forma.KvCondition{Attr: "missing", Value: "equals:1"}),
			"precondition tests attribute 'missing', which schema 'cond649' does not define"},
		{"precondition on the version stamp", conditional(&forma.KvCondition{Attr: "ltbase_updated_at", Value: "equals:1"}),
			"precondition tests attribute 'ltbase_updated_at'"},
		{"precondition holding a nil leaf", conditional((*forma.KvCondition)(nil)), "precondition holds a nil condition"},
		{"precondition holding a nil child", conditional(&forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{nil}}),
			"precondition holds a nil condition"},
		{"precondition with unknown logic", conditional(&forma.CompositeCondition{Logic: "xor", Conditions: []forma.Condition{precondition}}),
			"precondition composite has logic 'xor'"},
		{"delete precondition on an unknown attribute", func(f *condFixture) error {
			op := condOp(uuid.New())
			op.Preconditions = &forma.KvCondition{Attr: "missing", Value: "equals:1"}
			return f.em.Delete(f.ctx, op)
		}, "precondition tests attribute 'missing'"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newCondFixture(t)
			requireRefusedBeforeRepository(t, f, tc.run(f), tc.want)
		})
	}
}

func update(updates map[string]any, ops ...forma.UpdateOp) func(f *condFixture) error {
	return func(f *condFixture) error {
		op := condOp(uuid.New())
		op.Updates, op.UpdateOps = updates, ops
		_, err := f.em.Update(f.ctx, op)
		return err
	}
}

func conditional(precondition forma.Condition) func(f *condFixture) error {
	return func(f *condFixture) error {
		op := condOp(uuid.New())
		op.Updates, op.Preconditions = map[string]any{"name": "x"}, precondition
		_, err := f.em.Update(f.ctx, op)
		return err
	}
}

func TestBatchesRefuseConditionalFieldsAtIntake(t *testing.T) {
	withPrecondition := forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: condSchemaName, RowID: uuid.New()},
		Updates:          map[string]any{"name": "x"},
		Preconditions:    &forma.KvCondition{Attr: "version", Value: "equals:7"},
	}
	withOps := forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: condSchemaName, RowID: uuid.New()},
		UpdateOps:        []forma.UpdateOp{forma.Increment("counter", 1)},
	}
	plain := forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: condSchemaName, RowID: uuid.New()},
		Data:             map[string]any{"name": "x"},
		Updates:          map[string]any{"name": "x"},
	}
	for _, atomic := range []bool{false, true} {
		for name, run := range map[string]func(forma.EntityManager, context.Context, *forma.BatchOperation) (*forma.BatchResult, error){
			"BatchCreate": forma.EntityManager.BatchCreate,
			"BatchUpdate": forma.EntityManager.BatchUpdate,
			"BatchDelete": forma.EntityManager.BatchDelete,
		} {
			t.Run(name, func(t *testing.T) {
				f := newCondFixture(t)
				_, err := run(f.em, f.ctx, &forma.BatchOperation{Atomic: atomic, Operations: []forma.EntityOperation{plain, withPrecondition}})
				requireRefusedBeforeRepository(t, f, err, "operation[1]: batches do not accept preconditions (the precondition tests 'version')")

				_, err = run(f.em, f.ctx, &forma.BatchOperation{Atomic: atomic, Operations: []forma.EntityOperation{withOps}})
				requireRefusedBeforeRepository(t, f, err, "operation[0]: batches do not accept update operators (increment on 'counter')")
			})
		}
	}
}

func TestPreconditionReachesTheRepositoryGuard(t *testing.T) {
	f := newCondFixture(t)
	rowID := f.seed(t, map[string]any{"name": "a", "version": int64(7)})
	precondition := &forma.KvCondition{Attr: "version", Value: "equals:7"}

	op := condOp(rowID)
	op.Updates, op.Preconditions = map[string]any{"name": "b"}, precondition
	_, err := f.em.Update(f.ctx, op)
	require.NoError(t, err)

	op = condOp(rowID)
	op.Updates = map[string]any{"name": "c"}
	_, err = f.em.Update(f.ctx, op)
	require.NoError(t, err)

	op = condOp(rowID)
	op.Preconditions = precondition
	require.NoError(t, f.em.Delete(f.ctx, op))

	require.Len(t, f.repo.guards, 3)
	assert.Same(t, precondition, f.repo.guards[0].Precondition, "Update hands the precondition to the repository")
	assert.Equal(t, model.RowGuard{}, f.repo.guards[1], "an unconditional update passes the zero guard")
	assert.Same(t, precondition, f.repo.guards[2].Precondition, "Delete hands the precondition to the repository")
	assert.Zero(t, f.clockReads, "no server_time operator, no clock read")
}

func TestUpdateRefusalNamesWhatTheLockedRowLacks(t *testing.T) {
	cases := []struct {
		name string
		seed map[string]any
		op   forma.UpdateOp
		want string
	}{
		{"no current value", map[string]any{"name": "a"}, forma.Increment("counter", 1),
			"increment on integer attribute 'counter' needs a current value, and the row has none"},
		{"no current value beneath an absent object", map[string]any{"name": "a"}, forma.Increment("stats.attempts", 1),
			"increment on integer attribute 'stats.attempts' needs a current value"},
		{"smallint past its maximum", map[string]any{"small": int64(32767)}, forma.Increment("small", 1),
			"increment by 1 on smallint attribute 'small' would take its value 32767 past the smallint maximum 32767"},
		{"integer past its minimum", map[string]any{"counter": int64(-2147483648)}, forma.Increment("counter", -1),
			"past the integer minimum -2147483648"},
		{"inside an array", map[string]any{"items": []any{map[string]any{"qty": int64(1)}}}, forma.Increment("items.qty", 1),
			"attribute 'items.qty' is inside the array 'items'"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newCondFixture(t)
			rowID := f.seed(t, tc.seed)
			before := f.stored(t, rowID)

			op := condOp(rowID)
			op.UpdateOps = []forma.UpdateOp{tc.op}
			_, err := f.em.Update(f.ctx, op)
			require.ErrorIs(t, err, forma.ErrInvalidInput)
			message, published := forma.ResolvePublicMessage(err)
			require.True(t, published)
			assert.Contains(t, message, tc.want)
			assert.Equal(t, before, f.stored(t, rowID), "a refused operator stores nothing")
		})
	}
}

func TestUpdateOnAMissingRowIsNotFound(t *testing.T) {
	f := newCondFixture(t)
	op := condOp(uuid.New())
	op.UpdateOps = []forma.UpdateOp{forma.Increment("counter", 1)}
	_, err := f.em.Update(f.ctx, op)
	require.ErrorIs(t, err, forma.ErrNotFound)
	require.False(t, errors.Is(err, forma.ErrPreconditionFailed))
}

// TestMergeCommentNoLongerDefersTheConflictProtocol pins the #649 regression
// item: MergePersistentRecord's comment used to say the optimistic predicate
// "stays deferred" for want of a conflict protocol. Preconditions are that
// protocol now, and the comment must say so.
func TestMergeCommentNoLongerDefersTheConflictProtocol(t *testing.T) {
	src, err := os.ReadFile("postgres_persistent_repository_merge.go")
	require.NoError(t, err)
	assert.NotContains(t, string(src), "stays deferred")
	assert.Contains(t, string(src), "Optimistic concurrency is the caller's attribute precondition (#649)")
}
