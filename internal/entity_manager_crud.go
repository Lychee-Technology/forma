package internal

import (
	"context"

	"github.com/lychee-technology/forma"
)

// Create creates a new entity with the provided data.
func (em *entityManager) Create(ctx context.Context, req *forma.EntityOperation) (*forma.DataRecord, error) {
	return newEntityCRUDService(em).Create(ctx, req)
}

// Get retrieves an entity by schema name and row ID.
func (em *entityManager) Get(ctx context.Context, req *forma.QueryRequest) (*forma.DataRecord, error) {
	return newEntityCRUDService(em).Get(ctx, req)
}

// Update updates an existing entity.
func (em *entityManager) Update(ctx context.Context, req *forma.EntityOperation) (*forma.DataRecord, error) {
	return newEntityCRUDService(em).Update(ctx, req)
}

// Delete deletes an entity.
func (em *entityManager) Delete(ctx context.Context, req *forma.EntityOperation) error {
	return newEntityCRUDService(em).Delete(ctx, req)
}
