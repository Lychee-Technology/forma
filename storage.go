package forma

import (
	"context"
)

type EntityWriter interface {
	Create(ctx context.Context, req *EntityOperation) (*DataRecord, error)
	Update(ctx context.Context, req *EntityOperation) (*DataRecord, error)
	Delete(ctx context.Context, req *EntityOperation) error
}

type EntityReader interface {
	Get(ctx context.Context, req *QueryRequest) (*DataRecord, error)
	Query(ctx context.Context, req *QueryRequest) (*QueryResult, error)
	CrossSchemaSearch(ctx context.Context, req *CrossSchemaRequest) (*QueryResult, error)
}

type EntityBatchOperator interface {
	BatchCreate(ctx context.Context, req *BatchOperation) (*BatchResult, error)
	BatchUpdate(ctx context.Context, req *BatchOperation) (*BatchResult, error)
	BatchDelete(ctx context.Context, req *BatchOperation) (*BatchResult, error)
}

type EntityBatchCreator interface {
	BatchCreate(ctx context.Context, req *BatchOperation) (*BatchResult, error)
}

// EntityManager provides comprehensive entity and query operations
type EntityManager interface {
	EntityWriter
	EntityReader
	EntityBatchOperator
}
