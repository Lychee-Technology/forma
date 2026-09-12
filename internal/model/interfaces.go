package model

import (
	"context"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

type PersistentRecordTransformer interface {
	ToPersistentRecord(ctx context.Context, schemaID int16, rowID uuid.UUID, jsonData any) (*PersistentRecord, error)
	FromPersistentRecord(ctx context.Context, record *PersistentRecord) (map[string]any, error)
}

type StorageTables struct {
	EntityMain     string
	EAVData        string
	ChangeLog      string
	SchemaRegistry string
}

type PersistentRecordQuery struct {
	Tables          StorageTables
	SchemaID        int16
	Condition       forma.Condition
	AttributeOrders []AttributeOrder
	Limit           int
	Offset          int
}

type PersistentRecordPage struct {
	Records       []*PersistentRecord
	TotalRecords  int64
	TotalPages    int
	CurrentPage   int
	ExecutionPlan *ExecutionPlan
	Partial       *PartialScan
}

type PersistentRecordKey struct {
	SchemaID int16
	RowID    uuid.UUID
}

// PersistentRecordMerge computes the record to store from the row as it
// exists inside the write transaction. existing is nil when the row is
// absent — the caller owns that decision, because the user-facing 404 text
// belongs to the service layer, not to storage. Returning an error aborts
// the write and rolls the transaction back.
//
// The result must be the COMPLETE record to store, not a delta, because it
// is both what gets written and what the repository answers with. The EAV
// attributes it carries replace the row's whole in-scope attribute set, so a
// dropped attribute is deleted. A dropped typed column keeps its stored
// value but is missing from the answered record; DeletedAt is written
// verbatim, so dropping it clears a stored tombstone. CreatedAt is never
// written, only echoed, so a result that omits it answers a zero timestamp.
// UpdatedAt is the one field the repository stamps itself.
type PersistentRecordMerge func(ctx context.Context, existing *PersistentRecord) (*PersistentRecord, error)

type PersistentRecordWriter interface {
	InsertPersistentRecord(ctx context.Context, tables StorageTables, record *PersistentRecord) error
	// MergePersistentRecord is the guarded read-modify-write (#457): it takes
	// the per-row advisory lock create and delete take, reads the row inside
	// the write transaction, hands it to merge, and stores what merge returns
	// — all in one transaction. It answers the stored record.
	MergePersistentRecord(ctx context.Context, tables StorageTables, schemaID int16, rowID uuid.UUID, merge PersistentRecordMerge) (*PersistentRecord, error)
	DeletePersistentRecord(ctx context.Context, tables StorageTables, schemaID int16, rowID uuid.UUID) error
}

type PersistentRecordReader interface {
	GetPersistentRecord(ctx context.Context, tables StorageTables, schemaID int16, rowID uuid.UUID) (*PersistentRecord, error)
	QueryPersistentRecords(ctx context.Context, query *PersistentRecordQuery) (*PersistentRecordPage, error)
	// QueryPersistentRecordsByAttrValues fetches full records whose attribute
	// equals any of the given values via one set-based lookup (#268). It exists
	// for internal batch lookups (relation enrichment); it must never expand to
	// an OR-of-N condition per value.
	QueryPersistentRecordsByAttrValues(ctx context.Context, tables StorageTables, schemaID int16, attr string, values []string, limit int) (*PersistentRecordPage, error)
}

type FederatedQueryEngine interface {
	Query(ctx context.Context, tables StorageTables, fq *FederatedAttributeQuery, opts *FederatedQueryOptions) (*PersistentRecordPage, error)
}

type PersistentRecordRepository interface {
	PersistentRecordWriter
	PersistentRecordReader
}

type AtomicBatchPersistentRecordRepository interface {
	BatchInsertPersistentRecords(ctx context.Context, tables StorageTables, records []*PersistentRecord) error
	BatchUpdatePersistentRecords(ctx context.Context, tables StorageTables, records []*PersistentRecord) error
	BatchDeletePersistentRecords(ctx context.Context, tables StorageTables, keys []PersistentRecordKey) error
}
