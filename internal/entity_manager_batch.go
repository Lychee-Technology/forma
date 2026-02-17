package internal

import (
	"context"

	"github.com/lychee-technology/forma"
)

// BatchCreate creates multiple entities.
func (em *entityManager) BatchCreate(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	return newEntityBatchService(em).BatchCreate(ctx, req)
}

// BatchUpdate updates multiple entities.
func (em *entityManager) BatchUpdate(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	return newEntityBatchService(em).BatchUpdate(ctx, req)
}

// BatchDelete deletes multiple entities.
func (em *entityManager) BatchDelete(ctx context.Context, req *forma.BatchOperation) (*forma.BatchResult, error) {
	return newEntityBatchService(em).BatchDelete(ctx, req)
}
