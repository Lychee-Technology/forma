package internal

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemameta"
)

type SchemaMetadata = schemameta.SchemaMetadata
type MetadataCache = schemameta.MetadataCache
type MetadataLoader = schemameta.MetadataLoader
type DBPool = schemameta.DBPool

func NewMetadataCache() *MetadataCache {
	return schemameta.NewMetadataCache()
}

func NewMetadataLoader(pool DBPool, schemaTableName, schemaDirectory string) *MetadataLoader {
	return schemameta.NewMetadataLoader(pool, schemaTableName, schemaDirectory)
}

func NewFileSchemaRegistry(pool *pgxpool.Pool, schemaTable string, schemaDir string) (forma.SchemaRegistry, error) {
	return schemameta.NewFileSchemaRegistry(pool, schemaTable, schemaDir)
}

func NewFileSchemaRegistryContext(ctx context.Context, pool *pgxpool.Pool, schemaTable string, schemaDir string) (forma.SchemaRegistry, error) {
	return schemameta.NewFileSchemaRegistryContext(ctx, pool, schemaTable, schemaDir)
}

func NewFileSchemaRegistryFromDirectory(schemaDir string) (forma.SchemaRegistry, error) {
	return schemameta.NewFileSchemaRegistryFromDirectory(schemaDir)
}

func getSchemaMetadata(registry forma.SchemaRegistry, schemaID int16) (forma.SchemaAttributeCache, map[int16]string, error) {
	return schemameta.GetSchemaMetadata(registry, schemaID)
}
