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

type PersistentRecordWriter interface {
	InsertPersistentRecord(ctx context.Context, tables StorageTables, record *PersistentRecord) error
	UpdatePersistentRecord(ctx context.Context, tables StorageTables, record *PersistentRecord) error
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
