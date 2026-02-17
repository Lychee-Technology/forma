package internal

import (
	"context"

	"github.com/lychee-technology/forma"
)

// Query queries entities with filters and pagination.
func (em *entityManager) Query(ctx context.Context, req *forma.QueryRequest) (*forma.QueryResult, error) {
	return newEntityQueryService(em).Query(ctx, req)
}

// CrossSchemaSearch searches across multiple schemas using a single optimized query.
func (em *entityManager) CrossSchemaSearch(ctx context.Context, req *forma.CrossSchemaRequest) (*forma.QueryResult, error) {
	return newEntityQueryService(em).CrossSchemaSearch(ctx, req)
}
