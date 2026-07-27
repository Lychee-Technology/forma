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

// EntityManager provides comprehensive entity and query operations.
//
// Close releases resources the manager owns — for factory-built managers
// that is the embedded DuckDB client (#302). It is safe for concurrent use:
// teardown runs exactly once and every call returns the same result, so a
// failed close stays observable to late callers. Managers constructed
// without owned resources return nil. Embedders must Close the manager when
// done with it or the DuckDB instance lives until process exit.
type EntityManager interface {
	EntityWriter
	EntityReader
	EntityBatchOperator
	Close() error
}
