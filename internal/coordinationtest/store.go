// Package coordinationtest is a test-only adapter that implements
// LTFlow-shaped stores (workflow instances, tasks, idempotency records,
// links, outbox messages, history events and definitions) on Forma's public
// API alone, and the contract suite that holds the adapter to LTFlow's
// consistency contract on real PostgreSQL (#654). README.md maps each LTFlow
// operation to the Forma primitives the adapter uses.
//
// The adapter's non-test files import only forma, forma/factory, the
// standard library and github.com/google/uuid; a guard test enforces it.
package coordinationtest

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// Schema names, as testdata/schemas registers them.
const (
	InstanceSchema    = "workflow_instance"
	TaskSchema        = "workflow_task"
	IdempotencySchema = "idempotency_record"
	LinkSchema        = "workflow_link"
	OutboxSchema      = "workflow_outbox"
	HistorySchema     = "workflow_history"
	DefinitionSchema  = "workflow_definition"
)

// Store is the adapter's repository: LTFlow's repository.Repository over a
// Forma Transactor.
type Store struct {
	tr forma.Transactor
}

// NewStore answers a Store whose transactions run on tr.
func NewStore(tr forma.Transactor) *Store {
	return &Store{tr: tr}
}

// InTx runs fn in one Forma transaction. LTFlow: Repository.InTx.
//
// It calls Transactor.InTx exactly once and returns its error as it is, so
// ErrRetryableConflict and ErrCommitOutcomeUnknown reach the caller
// unchanged. It never retries: re-running fn is the policy of the
// consumer's caller (the LTFlow engine), as LTFlow's own DB.InTx runs fn
// once. Wrapping the error would add nothing a caller can act on, and the
// contract is that it arrives unchanged.
func (s *Store) InTx(ctx context.Context, fn func(UnitOfWork) error) error {
	return s.tr.InTx(ctx, func(ctx context.Context, tx forma.Tx) error {
		return fn(&unitOfWork{tx: tx})
	})
}

// UnitOfWork exposes the stores of one transaction. LTFlow:
// repository.UnitOfWork, plus Definitions, which LTFlow keeps outside it.
type UnitOfWork interface {
	Instances() InstanceStore
	Tasks() TaskStore
	Idempotency() IdempotencyStore
	Links() LinkStore
	Outbox() OutboxStore
	History() HistoryStore
	Definitions() DefinitionStore
}

type unitOfWork struct {
	tx forma.Tx
}

func (u *unitOfWork) Instances() InstanceStore      { return &instanceStore{tx: u.tx} }
func (u *unitOfWork) Tasks() TaskStore              { return &taskStore{tx: u.tx} }
func (u *unitOfWork) Idempotency() IdempotencyStore { return &idempotencyStore{tx: u.tx} }
func (u *unitOfWork) Links() LinkStore              { return &linkStore{tx: u.tx} }
func (u *unitOfWork) Outbox() OutboxStore           { return &outboxStore{tx: u.tx} }
func (u *unitOfWork) History() HistoryStore         { return &historyStore{tx: u.tx} }
func (u *unitOfWork) Definitions() DefinitionStore  { return &definitionStore{tx: u.tx} }

// equals is the condition attr equals value.
func equals(attr string, value any) forma.Condition {
	return &forma.KvCondition{Attr: attr, Value: fmt.Sprintf("equals:%v", value)}
}

// all is the conjunction of conditions.
func all(conditions ...forma.Condition) forma.Condition {
	return &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: conditions}
}

// anyOf is the disjunction of conditions.
func anyOf(conditions ...forma.Condition) forma.Condition {
	return &forma.CompositeCondition{Logic: forma.LogicOr, Conditions: conditions}
}

// rfc3339 is how the adapter writes an instant: millisecond precision, the
// precision Forma stores a datetime at.
func rfc3339(t time.Time) string {
	return t.UTC().Truncate(time.Millisecond).Format(time.RFC3339Nano)
}

// create writes a new row of schema, under rowID unless it is uuid.Nil.
func create(ctx context.Context, tx forma.Tx, schema string, rowID uuid.UUID, data map[string]any) (*forma.DataRecord, error) {
	record, err := tx.Create(ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: schema, RowID: rowID}, Data: data})
	if err != nil {
		return nil, fmt.Errorf("create %s row: %w", schema, err)
	}
	return record, nil
}

// queryAll answers every row of schema matching cond, up to 100, in sort
// order.
func queryAll(ctx context.Context, tx forma.Tx, schema string, cond forma.Condition, sort ...forma.OrderBy) ([]*forma.DataRecord, error) {
	result, err := tx.Query(ctx, &forma.QueryRequest{SchemaName: schema, Condition: cond, Sort: sort, Page: 1, ItemsPerPage: 100})
	if err != nil {
		return nil, fmt.Errorf("query %s: %w", schema, err)
	}
	return result.Data, nil
}
