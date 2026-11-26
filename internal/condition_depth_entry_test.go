package internal

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every public method that accepts a condition refuses a cyclic or
// over-deep one (#675) as a published ErrInvalidInput, before any SQL, and
// a Tx stays usable after the refusal. The manager and the Tx run on a
// pgxmock pool that expects no statement but the Tx's BEGIN and COMMIT.

func depthRefusal(noun string) string {
	return fmt.Sprintf("%s nests composite conditions more than 1000 levels deep; the %s is cyclic or too deeply nested", noun, noun)
}

// Create and the batches refuse any precondition. They keep that refusal for
// a cyclic one, without crashing on it.
const (
	createRefusal = "create does not accept preconditions"
	batchRefusal  = "batches do not accept preconditions"
)

type managerConditionEntry struct {
	name    string
	refusal string
	call    func(ctx context.Context, em forma.EntityManager, cond forma.Condition) error
}

type txConditionEntry struct {
	name    string
	refusal string
	call    func(ctx context.Context, tx forma.Tx, cond forma.Condition) error
}

func visitOp(cond forma.Condition) *forma.EntityOperation {
	return &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "visit", RowID: uuid.New()},
		Updates:          map[string]any{"status": "visited"},
		Preconditions:    cond,
	}
}

func visitCreate(cond forma.Condition) *forma.EntityOperation {
	op := visitOp(cond)
	op.Updates, op.Data = nil, visitPayload("v")
	return op
}

func managerConditionEntries() []managerConditionEntry {
	entries := []managerConditionEntry{
		{"Query", depthRefusal("condition"), func(ctx context.Context, em forma.EntityManager, cond forma.Condition) error {
			_, err := em.Query(ctx, &forma.QueryRequest{SchemaName: "visit", Condition: cond})
			return err
		}},
		{"CrossSchemaSearch", depthRefusal("condition"), func(ctx context.Context, em forma.EntityManager, cond forma.Condition) error {
			_, err := em.CrossSchemaSearch(ctx, &forma.CrossSchemaRequest{SchemaNames: []string{"visit"}, SearchTerm: "x", Condition: cond})
			return err
		}},
		{"Update", depthRefusal("precondition"), func(ctx context.Context, em forma.EntityManager, cond forma.Condition) error {
			_, err := em.Update(ctx, visitOp(cond))
			return err
		}},
		{"Delete", depthRefusal("precondition"), func(ctx context.Context, em forma.EntityManager, cond forma.Condition) error {
			op := visitOp(cond)
			op.Updates = nil
			return em.Delete(ctx, op)
		}},
		{"Create", createRefusal, func(ctx context.Context, em forma.EntityManager, cond forma.Condition) error {
			_, err := em.Create(ctx, visitCreate(cond))
			return err
		}},
	}
	for _, atomic := range []bool{false, true} {
		for name, run := range map[string]func(forma.EntityManager, context.Context, *forma.BatchOperation) (*forma.BatchResult, error){
			"BatchCreate": forma.EntityManager.BatchCreate,
			"BatchUpdate": forma.EntityManager.BatchUpdate,
			"BatchDelete": forma.EntityManager.BatchDelete,
		} {
			entries = append(entries, managerConditionEntry{fmt.Sprintf("%s atomic=%v", name, atomic), batchRefusal,
				func(ctx context.Context, em forma.EntityManager, cond forma.Condition) error {
					_, err := run(em, ctx, &forma.BatchOperation{Atomic: atomic, Operations: []forma.EntityOperation{*visitCreate(cond)}})
					return err
				}})
		}
	}
	return entries
}

func txConditionEntries() []txConditionEntry {
	return []txConditionEntry{
		{"Tx.Query", depthRefusal("condition"), func(ctx context.Context, tx forma.Tx, cond forma.Condition) error {
			_, err := tx.Query(ctx, &forma.QueryRequest{SchemaName: "visit", Condition: cond})
			return err
		}},
		{"Tx.Update", depthRefusal("precondition"), func(ctx context.Context, tx forma.Tx, cond forma.Condition) error {
			_, err := tx.Update(ctx, visitOp(cond))
			return err
		}},
		{"Tx.Delete", depthRefusal("precondition"), func(ctx context.Context, tx forma.Tx, cond forma.Condition) error {
			op := visitOp(cond)
			op.Updates = nil
			return tx.Delete(ctx, op)
		}},
		{"Tx.Create", createRefusal, func(ctx context.Context, tx forma.Tx, cond forma.Condition) error {
			_, err := tx.Create(ctx, visitCreate(cond))
			return err
		}},
		{"Tx.AssertRow", depthRefusal("precondition"), func(ctx context.Context, tx forma.Tx, cond forma.Condition) error {
			return tx.AssertRow(ctx, "visit", uuid.New(), cond)
		}},
		{"Tx.UpdateWhere", depthRefusal("condition"), func(ctx context.Context, tx forma.Tx, cond forma.Condition) error {
			req := scheduledVisits()
			req.Condition, req.Updates = cond, map[string]any{"status": "visited"}
			_, err := tx.UpdateWhere(ctx, req)
			return err
		}},
		{"Tx.DeleteWhere", depthRefusal("condition"), func(ctx context.Context, tx forma.Tx, cond forma.Condition) error {
			req := scheduledVisits()
			req.Condition = cond
			_, err := tx.DeleteWhere(ctx, req)
			return err
		}},
		{"Tx.Claim condition", depthRefusal("condition"), func(ctx context.Context, tx forma.Tx, cond forma.Condition) error {
			req := scheduledVisitClaim()
			req.Condition = cond
			_, err := tx.Claim(ctx, req)
			return err
		}},
		{"Tx.Claim group live condition", depthRefusal("group live condition"), func(ctx context.Context, tx forma.Tx, cond forma.Condition) error {
			req := callsByUserClaim()
			req.Group.Live = cond
			_, err := tx.Claim(ctx, req)
			return err
		}},
		{"Tx.Upsert", depthRefusal("precondition"), func(ctx context.Context, tx forma.Tx, cond forma.Condition) error {
			_, err := tx.Upsert(ctx, &forma.UpsertRequest{Create: *visitCreate(nil), Target: forma.ConflictTarget{RowID: true},
				Action: forma.UpsertUpdate, Updates: map[string]any{"status": "visited"}, Preconditions: cond})
			return err
		}},
	}
}

// requirePublishedRefusal asserts err is an ErrInvalidInput whose published
// message holds want.
func requirePublishedRefusal(t *testing.T, err error, want string) {
	t.Helper()
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	message, published := forma.ResolvePublicMessage(err)
	require.True(t, published, "the refusal publishes its message: %v", err)
	assert.Contains(t, message, want)
}

// runConditionEntryPoints is the regression suite, run by
// TestConditionEntryPointsRefuseCyclicAndDeepTrees and again under a capped
// stack by TestCyclicConditionsUnderACappedStack.
func runConditionEntryPoints(t *testing.T) {
	for _, tree := range refusedConditions() {
		t.Run(tree.name, func(t *testing.T) {
			for _, entry := range managerConditionEntries() {
				t.Run(entry.name, func(t *testing.T) {
					em, mock := newMockManagerOver(t, nil, "visit")
					requirePublishedRefusal(t, entry.call(context.Background(), em, tree.cond), entry.refusal)
					require.NoError(t, mock.ExpectationsWereMet(), "a refused condition sends no SQL")
				})
			}
			t.Run("Tx", func(t *testing.T) {
				txr, mock := newMockTransactorOver(t, nil, "visit", "activity")
				mock.ExpectBeginTx(readCommittedTx)
				mock.ExpectCommit()
				err := txr.InTx(context.Background(), func(ctx context.Context, tx forma.Tx) error {
					for _, entry := range txConditionEntries() {
						t.Run(entry.name, func(t *testing.T) {
							requirePublishedRefusal(t, entry.call(ctx, tx, tree.cond), entry.refusal)
						})
					}
					return nil
				})
				require.NoError(t, err, "every refusal leaves the Tx able to commit")
				require.NoError(t, mock.ExpectationsWereMet(), "the Tx sends nothing but BEGIN and COMMIT")
			})
		})
	}
}

func TestConditionEntryPointsRefuseCyclicAndDeepTrees(t *testing.T) {
	runConditionEntryPoints(t)
}

// At an entry point the bound sits exactly where checkConditionDepth puts
// it: a precondition whose leaf is at depth 1000 reaches the repository's
// guard, one at depth 1001 is refused before any write.
func TestPreconditionDepthBoundaryAtTheEntryPoint(t *testing.T) {
	leaf := &forma.KvCondition{Attr: "version", Value: "equals:7"}
	deepest := nestedCondition(maxConditionNestingDepth, leaf)
	tooDeep := nestedCondition(maxConditionNestingDepth+1, leaf)

	f := newCondFixture(t)
	rowID := f.seed(t, map[string]any{"name": "a", "version": int64(7)})
	op := condOp(rowID)
	op.Updates, op.Preconditions = map[string]any{"name": "b"}, tooDeep
	_, err := f.em.Update(f.ctx, op)
	requireRefusedBeforeRepository(t, f, err, depthRefusal("precondition"))
	op = condOp(rowID)
	op.Preconditions = tooDeep
	requireRefusedBeforeRepository(t, f, f.em.Delete(f.ctx, op), depthRefusal("precondition"))
	require.Empty(t, f.repo.guards)

	op = condOp(rowID)
	op.Updates, op.Preconditions = map[string]any{"name": "b"}, deepest
	_, err = f.em.Update(f.ctx, op)
	require.NoError(t, err)
	op = condOp(rowID)
	op.Preconditions = deepest
	require.NoError(t, f.em.Delete(f.ctx, op))
	require.Len(t, f.repo.guards, 2)
	assert.Same(t, deepest, f.repo.guards[0].Precondition)
	assert.Same(t, deepest, f.repo.guards[1].Precondition)
}
