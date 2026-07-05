package internal

import (
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/transform"
)

type AttributeConverter = transform.AttributeConverter

func NewAttributeConverter(registry forma.SchemaRegistry) *AttributeConverter {
	return transform.NewAttributeConverter(registry)
}

func NewPersistentRecordTransformer(registry forma.SchemaRegistry) model.PersistentRecordTransformer {
	return transform.NewPersistentRecordTransformer(registry)
}
