package coordinationtest

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// OutboxStore manages transactional outbox messages. LTFlow:
// repository.OutboxStore and repository.OutboxTopicClaimer.
type OutboxStore interface {
	Insert(ctx context.Context, msgs []OutboxMessage) error
	ClaimBatch(ctx context.Context, claimer string, lease time.Duration, limit int) ([]OutboxMessage, error)
	ClaimBatchByTopics(ctx context.Context, claimer string, lease time.Duration, limit int, topics []string) ([]OutboxMessage, error)
	MarkPublished(ctx context.Context, id uuid.UUID, expectedGeneration int64) error
	MarkFailed(ctx context.Context, id uuid.UUID, expectedGeneration int64, errMsg string, nextAttemptAt time.Time) error
	MarkDeadLetter(ctx context.Context, id uuid.UUID, expectedGeneration int64, errMsg string) error
}

type outboxStore struct {
	tx forma.Tx
}

// Insert persists msgs with one Tx.Create each (#648), under each message's
// ID unless it is uuid.Nil, and sets every ID to its row's. LTFlow:
// OutboxStore.Insert.
func (s *outboxStore) Insert(ctx context.Context, msgs []OutboxMessage) error {
	for i := range msgs {
		m := &msgs[i]
		data := map[string]any{
			"partition_key": m.PartitionKey, "topic": m.Topic, "command_type": m.CommandType, "payload": m.Payload,
			"status": m.Status, "attempts": m.Attempts, "next_attempt_at": rfc3339(m.NextAttemptAt),
			"locked_by": m.LockedBy, "lock_generation": m.LockGeneration, "enqueued_at": rfc3339(m.EnqueuedAt),
		}
		setInstant(data, "locked_until", m.LockedUntil)
		record, err := create(ctx, s.tx, OutboxSchema, m.ID, data)
		if err != nil {
			return fmt.Errorf("insert outbox message %d of %d: %w", i+1, len(msgs), err)
		}
		m.ID = record.RowID
	}
	return nil
}

// ClaimBatch claims up to limit messages for claimer with one Tx.Claim
// (#653): see ClaimBatchByTopics, without the topic restriction. LTFlow:
// OutboxStore.ClaimBatch.
func (s *outboxStore) ClaimBatch(ctx context.Context, claimer string, lease time.Duration, limit int) ([]OutboxMessage, error) {
	return s.claim(ctx, claimer, lease, limit, claimable())
}

// ClaimBatchByTopics claims up to limit messages of topics for claimer with
// one Tx.Claim (#653). A message is claimable when it is pending or failed
// and due, or publishing with an expired lease (#646's $now), and when it
// heads its partition: no earlier message of its partition_key, by
// enqueued_at then row ID, is still unresolved (Group). The claim marks each
// message publishing, increments its lock_generation and leases it until
// the transaction's clock plus lease (#649).
//
// LTFlow passes an absolute leaseUntil from the caller's clock and tests
// the lease against the database's now(); a lease on the transaction's
// clock keeps the lease and the claim condition on one clock. LTFlow:
// OutboxTopicClaimer.ClaimBatchByTopics.
func (s *outboxStore) ClaimBatchByTopics(ctx context.Context, claimer string, lease time.Duration, limit int, topics []string) ([]OutboxMessage, error) {
	if len(topics) == 0 {
		return nil, nil
	}
	list := make([]any, len(topics))
	for i, topic := range topics {
		list[i] = topic
	}
	return s.claim(ctx, claimer, lease, limit, all(claimable(), forma.In("topic", list...)))
}

func (s *outboxStore) claim(ctx context.Context, claimer string, lease time.Duration, limit int, cond forma.Condition) ([]OutboxMessage, error) {
	result, err := s.tx.Claim(ctx, &forma.ClaimRequest{
		SchemaName: OutboxSchema,
		Condition:  cond,
		Sort:       []forma.OrderBy{{Attribute: "enqueued_at"}},
		Limit:      limit,
		Group:      &forma.ClaimGroup{Key: []string{"partition_key"}, Order: []forma.OrderBy{{Attribute: "enqueued_at"}}, Live: Unresolved()},
		Updates:    map[string]any{"status": OutboxPublishing, "locked_by": claimer},
		UpdateOps:  []forma.UpdateOp{forma.Increment("lock_generation", 1), forma.ServerTime("locked_until", lease)},
	})
	if err != nil {
		return nil, fmt.Errorf("claim up to %d outbox messages for %s: %w", limit, claimer, err)
	}
	msgs := make([]OutboxMessage, len(result.Records))
	for i, r := range result.Records {
		msgs[i] = *outboxMessageOf(r)
	}
	return msgs, nil
}

// claimable is LTFlow's claim condition: pending or failed and due, or
// publishing with an expired lease.
func claimable() forma.Condition {
	return anyOf(
		all(forma.In("status", OutboxPending, OutboxFailed), forma.Now("next_attempt_at", forma.FilterLessEq, 0)),
		all(equals("status", OutboxPublishing), forma.Now("locked_until", forma.FilterLessEq, 0)),
	)
}

// Unresolved is LTFlow's outbox liveness: a message blocks the later
// messages of its partition until it is published or dead-lettered.
func Unresolved() forma.Condition {
	return forma.NotIn("status", OutboxPublished, OutboxDeadLetter)
}

// MarkPublished marks the message published with Tx.Update and the
// precondition lock_generation equals expectedGeneration (#649). A stale
// generation is ErrPreconditionFailed and writes nothing. LTFlow:
// OutboxStore.MarkPublished.
func (s *outboxStore) MarkPublished(ctx context.Context, id uuid.UUID, expectedGeneration int64) error {
	return s.complete(ctx, id, expectedGeneration, map[string]any{"status": OutboxPublished},
		forma.ServerTime("published_at", 0))
}

// MarkFailed records a transient failure and schedules the next attempt
// with Tx.Update, the precondition lock_generation equals
// expectedGeneration and Increment(attempts) (#649). LTFlow clears
// locked_until; an update cannot clear an attribute yet (#684), so the lease
// is ended at the transaction's clock instead, and status tells a failed
// message from a held one. LTFlow: OutboxStore.MarkFailed.
func (s *outboxStore) MarkFailed(ctx context.Context, id uuid.UUID, expectedGeneration int64, errMsg string, nextAttemptAt time.Time) error {
	return s.complete(ctx, id, expectedGeneration,
		map[string]any{"status": OutboxFailed, "last_error": errMsg, "next_attempt_at": rfc3339(nextAttemptAt), "locked_by": ""},
		forma.Increment("attempts", 1), forma.ServerTime("locked_until", 0))
}

// MarkDeadLetter marks the message dead_letter with Tx.Update and the
// precondition lock_generation equals expectedGeneration (#649). LTFlow:
// OutboxStore.MarkDeadLetter.
func (s *outboxStore) MarkDeadLetter(ctx context.Context, id uuid.UUID, expectedGeneration int64, errMsg string) error {
	return s.complete(ctx, id, expectedGeneration, map[string]any{"status": OutboxDeadLetter, "last_error": errMsg})
}

func (s *outboxStore) complete(ctx context.Context, id uuid.UUID, generation int64, updates map[string]any, ops ...forma.UpdateOp) error {
	_, err := s.tx.Update(ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: OutboxSchema, RowID: id},
		Preconditions:    equals("lock_generation", generation),
		Updates:          updates,
		UpdateOps:        ops,
	})
	if err != nil {
		return fmt.Errorf("move outbox message %s to %v at generation %d: %w", id, updates["status"], generation, err)
	}
	return nil
}

func outboxMessageOf(r *forma.DataRecord) *OutboxMessage {
	return &OutboxMessage{
		ID: r.RowID, PartitionKey: text(r, "partition_key"), Topic: text(r, "topic"), CommandType: text(r, "command_type"),
		Payload: text(r, "payload"), Status: text(r, "status"), Attempts: whole(r, "attempts"),
		NextAttemptAt: instant(r, "next_attempt_at"), LockedBy: text(r, "locked_by"),
		LockedUntil: instantPtr(r, "locked_until"), LockGeneration: whole(r, "lock_generation"),
		LastError: text(r, "last_error"), EnqueuedAt: instant(r, "enqueued_at"),
	}
}
