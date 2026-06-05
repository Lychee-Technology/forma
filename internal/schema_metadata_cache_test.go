package internal

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

func TestSchemaMetadataCache_GetSchemaMetadataRejectsDuplicateAttributeIDs(t *testing.T) {
	registry := &stubSchemaRegistry{
		schemaID:   501,
		schemaName: "dup_attr_ids",
		cache: forma.SchemaAttributeCache{
			"first":  {AttributeID: 7, ValueType: forma.ValueTypeText},
			"second": {AttributeID: 7, ValueType: forma.ValueTypeText},
		},
	}

	cache := newSchemaMetadataCache(registry)
	_, _, err := cache.getSchemaMetadata(501)
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate attribute id")
}
