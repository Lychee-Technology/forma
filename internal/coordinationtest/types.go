package coordinationtest

import (
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// The adapter's records: the fields of LTFlow's domain types the suite
// reads and writes. An optional instant is a pointer; a nil one is not
// written.

// Instance is a workflow instance. Its ID is the row ID. LTFlow:
// domain.Instance.
type Instance struct {
	ID                uuid.UUID
	WorkflowName      string
	State             string
	Status            string
	BusinessReference string
	Payload           string
	Version           int64
	EventCount        int64
	EligibleCleanupAt *time.Time
}

// Task statuses and purposes, as LTFlow names them.
const (
	TaskPending   = "pending"
	TaskCompleted = "completed"
	TaskCancelled = "cancelled"
	TaskExpired   = "expired"
	PurposeWorker = "worker"
	PurposeTimer  = "timer"
)

// Task is a workflow task, found by its token hash. LTFlow: domain.Task.
type Task struct {
	ID                uuid.UUID
	TokenHash         string
	InstanceID        uuid.UUID
	State             string
	Version           int64
	Status            string
	ExpectedEvent     string
	Purpose           string
	ResolvedAt        *time.Time
	EligibleCleanupAt *time.Time
}

// Event record statuses.
const (
	RecordSucceeded = "succeeded"
	RecordFailed    = "failed"
)

// EventRecord is an idempotency record, unique per (instance, key). LTFlow:
// domain.EventRecord.
type EventRecord struct {
	ID                uuid.UUID
	InstanceID        uuid.UUID
	IdempotencyKey    string
	RequestHash       string
	Event             string
	Status            string
	ResultState       string
	ResultVersion     int64
	ErrorCode         string
	EligibleCleanupAt *time.Time
}

// Link is a parent-child workflow link, unique per parent task hash.
// LTFlow: domain.WorkflowLink.
type Link struct {
	ID               uuid.UUID
	ParentTaskHash   string
	ParentInstanceID uuid.UUID
	ChildInstanceID  uuid.UUID
	Status           string
}

// Outbox message statuses.
const (
	OutboxPending    = "pending"
	OutboxPublishing = "publishing"
	OutboxPublished  = "published"
	OutboxFailed     = "failed"
	OutboxDeadLetter = "dead_letter"
)

// OutboxMessage is a transactional outbox message. EnqueuedAt is LTFlow's
// created_at, a name Forma reserves. LTFlow: domain.OutboxMessage.
type OutboxMessage struct {
	ID             uuid.UUID
	PartitionKey   string
	Topic          string
	CommandType    string
	Payload        string
	Status         string
	Attempts       int64
	NextAttemptAt  time.Time
	LockedBy       string
	LockedUntil    *time.Time
	LockGeneration int64
	LastError      string
	EnqueuedAt     time.Time
}

// HistoryEvent is a workflow history event. RecordedAt is LTFlow's
// created_at; a zero one is written as now. LTFlow: domain.HistoryEvent.
type HistoryEvent struct {
	ID         uuid.UUID
	InstanceID uuid.UUID
	Type       string
	StateFrom  string
	StateTo    string
	Event      string
	Details    string
	DedupCount int64
	RecordedAt time.Time
}

// Definition is a workflow definition. A draft has no activation sequence
// (zero). LTFlow: domain.Definition.
type Definition struct {
	ID                 uuid.UUID
	Name               string
	Version            int64
	ActivationSequence int64
	Status             string
	Body               string
}

// text answers the text attribute attr of r, or "" when it is absent.
func text(r *forma.DataRecord, attr string) string {
	s, _ := r.Attributes[attr].(string)
	return s
}

// whole answers the numeric attribute attr of r, or 0 when it is absent.
func whole(r *forma.DataRecord, attr string) int64 {
	switch v := r.Attributes[attr].(type) {
	case int:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	}
	return 0
}

// instant answers the datetime attribute attr of r, or the zero time when it
// is absent.
func instant(r *forma.DataRecord, attr string) time.Time {
	t, _ := r.Attributes[attr].(time.Time)
	return t
}

// instantPtr answers the datetime attribute attr of r, or nil when it is
// absent.
func instantPtr(r *forma.DataRecord, attr string) *time.Time {
	t, ok := r.Attributes[attr].(time.Time)
	if !ok {
		return nil
	}
	return &t
}

// id answers the text attribute attr of r parsed as a UUID, or uuid.Nil.
func id(r *forma.DataRecord, attr string) uuid.UUID {
	parsed, err := uuid.Parse(text(r, attr))
	if err != nil {
		return uuid.Nil
	}
	return parsed
}

// setInstant writes t into data as attr when it is not nil.
func setInstant(data map[string]any, attr string, t *time.Time) {
	if t != nil {
		data[attr] = rfc3339(*t)
	}
}
