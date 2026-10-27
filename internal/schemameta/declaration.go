package schemameta

import (
	"fmt"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemaledger"
)

// ReadDeclaration reads schemaName's two files in schemaDir as the control
// plane records them: validated as the registry validates them at load, with
// every entry of the attributes file, retired ones included, and the content
// hash of the bytes read. init-db, apply-schema and baseline-schema record a
// generation from it. It also answers the full attributes file it read, for
// baseline-schema's audit. A missing attributes file is an error that wraps
// fs.ErrNotExist.
func ReadDeclaration(schemaDir, schemaName string, schemaID int16) (schemaledger.Declaration, forma.SchemaAttributeCache, error) {
	r := newFileSchemaRegistry(nil, "", schemaDir)
	artifacts, err := r.loadSchemaArtifacts(schemaName, schemaID)
	if err != nil {
		return schemaledger.Declaration{}, nil, fmt.Errorf("failed to load schema %s: %w", schemaName, err)
	}
	if err := r.admitSchema(schemaName, schemaID, artifacts.cache, artifacts.schema); err != nil {
		return schemaledger.Declaration{}, nil, fmt.Errorf("schema %s: %w", schemaName, err)
	}
	return artifacts.declaration(schemaName, schemaID), artifacts.cache, nil
}
