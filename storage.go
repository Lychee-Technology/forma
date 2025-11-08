package forma

import (
	"context"
)

// EntityManager provides comprehensive entity and query operations
type EntityManager interface {
	// Entity CRUD operations
	Create(ctx context.Context, req *EntityOperation) (*DataRecord, error)
	Get(ctx context.Context, req *QueryRequest) (*DataRecord, error)
	Update(ctx context.Context, req *EntityOperation) (*DataRecord, error)
	Delete(ctx context.Context, req *EntityOperation) error

	// Query operations
	Query(ctx context.Context, req *QueryRequest) (*QueryResult, error)
	CrossSchemaSearch(ctx context.Context, req *CrossSchemaRequest) (*QueryResult, error)

	// Batch operations
	BatchCreate(ctx context.Context, req *BatchOperation) (*BatchResult, error)
	BatchUpdate(ctx context.Context, req *BatchOperation) (*BatchResult, error)
	BatchDelete(ctx context.Context, req *BatchOperation) (*BatchResult, error)
}
