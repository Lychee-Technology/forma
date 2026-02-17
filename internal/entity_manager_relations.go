package internal

import (
	"context"

	"github.com/lychee-technology/forma"
)

func (em *entityManager) enrichDataRecords(ctx context.Context, schemaName string, requested []string, records ...*forma.DataRecord) error {
	return newEntityRelationService(em).enrichDataRecords(ctx, schemaName, requested, records...)
}
