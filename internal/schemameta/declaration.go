package schemameta

import (
	"fmt"

	"github.com/lychee-technology/forma/internal/schemaledger"
)

// ReadDeclaration reads schemaName's two files in schemaDir as the control
// plane records them: validated as the registry validates them at load, with
// every entry of the attributes file, retired ones included, and the content
// hash of the bytes read. init-db, apply-schema and baseline-schema record a
// generation from it. A missing attributes file is an error that wraps
// fs.ErrNotExist.
func ReadDeclaration(schemaDir, schemaName string, schemaID int16) (schemaledger.Declaration, error) {
	r := newFileSchemaRegistry(nil, "", schemaDir)
	artifacts, err := r.loadSchemaArtifacts(schemaName, schemaID)
	if err != nil {
		return schemaledger.Declaration{}, fmt.Errorf("failed to load schema %s: %w", schemaName, err)
	}
	if err := r.admitSchema(schemaName, schemaID, artifacts.cache, artifacts.schema); err != nil {
		return schemaledger.Declaration{}, fmt.Errorf("schema %s: %w", schemaName, err)
	}
	return artifacts.declaration(schemaName, schemaID), nil
}
