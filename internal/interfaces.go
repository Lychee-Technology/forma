package internal

import (
	"context"

	"github.com/google/uuid"
	"lychee.technology/ltbase/forma"
)

type Transformer interface {
	// Single object conversion
	ToAttributes(ctx context.Context, schemaID int16, rowID uuid.UUID, jsonData any) ([]Attribute, error)
	FromAttributes(ctx context.Context, attributes []Attribute) (map[string]any, error)

	// Batch operations
	BatchToAttributes(ctx context.Context, schemaID int16, jsonObjects []any) ([]Attribute, error)
	BatchFromAttributes(ctx context.Context, attributes []Attribute) ([]map[string]any, error)

	// Validation
	ValidateAgainstSchema(ctx context.Context, jsonSchema any, jsonData any) error
}

type AttributeRepository interface {
	// Attribute operations
	InsertAttributes(ctx context.Context, attributes []Attribute) error
	UpdateAttributes(ctx context.Context, attributes []Attribute) error
	DeleteAttributes(ctx context.Context, schemaName string, rowIDs []uuid.UUID) error
	GetAttributes(ctx context.Context, schemaName string, rowID uuid.UUID) ([]Attribute, error)
	QueryAttributes(ctx context.Context, query *AttributeQuery) ([]Attribute, error)

	// Entity operations
	ExistsEntity(ctx context.Context, schemaName string, rowID uuid.UUID) (bool, error)
	DeleteEntity(ctx context.Context, schemaName string, rowID uuid.UUID) error
	CountEntities(ctx context.Context, schemaName string, filters []forma.Filter) (int64, error)

	// Batch operations
	BatchUpsertAttributes(ctx context.Context, attributes []Attribute) error

	// Advanced queries
	AdvancedQueryRowIDs(ctx context.Context, clause string, args []any, limit, offset int) ([]uuid.UUID, int64, error)
}
