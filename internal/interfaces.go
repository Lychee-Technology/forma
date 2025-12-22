package internal

import (
	"context"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

/*
* PersistentRecord 是一个用于表示持久化存储记录的结构体，包含了`entity main`和EAV Attributes。
 */
type PersistentRecord struct {
	SchemaID        int16
	RowID           uuid.UUID
	TextItems       map[string]string // e.g., "text_01" -> "Hello"
	Int16Items      map[string]int16
	Int32Items      map[string]int32
	Int64Items      map[string]int64
	Float64Items    map[string]float64
	UUIDItems       map[string]uuid.UUID
	CreatedAt       int64
	UpdatedAt       int64
	DeletedAt       *int64
	OtherAttributes []EAVRecord // EAV attributes not in hot table
}

type Transformer interface {
	// Single object conversion
	ToAttributes(ctx context.Context, schemaID int16, rowID uuid.UUID, jsonData any) ([]EntityAttribute, error)
	FromAttributes(ctx context.Context, attributes []EntityAttribute) (map[string]any, error)

	// Batch operations
	BatchToAttributes(ctx context.Context, schemaID int16, jsonObjects []any) ([]EntityAttribute, error)
	BatchFromAttributes(ctx context.Context, attributes []EntityAttribute) ([]map[string]any, error)

	// Validation
	ValidateAgainstSchema(ctx context.Context, jsonSchema any, jsonData any) error
}

type PersistentRecordTransformer interface {
	ToPersistentRecord(ctx context.Context, schemaID int16, rowID uuid.UUID, jsonData any) (*PersistentRecord, error)
	FromPersistentRecord(ctx context.Context, record *PersistentRecord) (map[string]any, error)
}

type StorageTables struct {
	EntityMain string
	EAVData    string
	ChangeLog  string
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
	Records      []*PersistentRecord
	TotalRecords int64
	TotalPages   int
	CurrentPage  int
}

type PersistentRecordRepository interface {
	InsertPersistentRecord(ctx context.Context, tables StorageTables, record *PersistentRecord) error
	UpdatePersistentRecord(ctx context.Context, tables StorageTables, record *PersistentRecord) error
	DeletePersistentRecord(ctx context.Context, tables StorageTables, schemaID int16, rowID uuid.UUID) error
	GetPersistentRecord(ctx context.Context, tables StorageTables, schemaID int16, rowID uuid.UUID) (*PersistentRecord, error)
	QueryPersistentRecords(ctx context.Context, query *PersistentRecordQuery) (*PersistentRecordPage, error)
}
