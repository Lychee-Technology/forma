package schemameta

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/sqlgen"
)

// underivedAncestryFixtures maps each schema of
// internal/testdata/underived_ancestry whose ancestry the registry cannot
// derive (#937) to the attribute it cannot place. Each also declares the
// array of objects items, so a derived cache would place items.sku in it.
var underivedAncestryFixtures = map[string]string{
	"ua_union":  "choice.kind",   // object and array members only inside oneOf
	"ua_nested": "matrix.cell",   // an array directly inside an array
	"ua_ref":    "contact.phone", // another file's $ref (#705)
}

// TestFileRegistryMarksTheAttributeItCannotPlace pins #937 at the registry:
// for each construct every attribute is left without ancestry, as before, and
// only the unplaced one is marked, so the parquet guard refuses the schema
// and names it. The referenced document, a flat schema of its own, is
// derived and stays exportable.
func TestFileRegistryMarksTheAttributeItCannotPlace(t *testing.T) {
	registry, err := NewFileSchemaRegistryFromDirectory("../testdata/underived_ancestry")
	require.NoError(t, err)

	for schema, unplaced := range underivedAncestryFixtures {
		_, cache, err := registry.GetSchemaAttributeCacheByName(schema)
		require.NoError(t, err, schema)
		require.Len(t, cache, 3, schema)
		for name, meta := range cache {
			require.Nil(t, meta.ArrayAncestors, "%s: %s", schema, name)
			require.Equal(t, name == unplaced, meta.ArrayAncestryUnresolved, "%s: %s", schema, name)
		}
		require.Contains(t, sqlgen.ParquetUnrepresentableReason(cache), `could not place attribute "`+unplaced+`"`, schema)
	}

	_, contact, err := registry.GetSchemaAttributeCacheByName("ua_contact")
	require.NoError(t, err)
	require.Equal(t, []string{}, contact["contact.phone"].ArrayAncestors)
	require.False(t, contact["contact.phone"].ArrayAncestryUnresolved)
	require.Empty(t, sqlgen.ParquetUnrepresentableReason(contact))
}

// TestFileRegistryCensusOfParquetEligibility records which shipped and
// fixture schemas the parquet guard holds back, so a change of eligibility
// shows up here. lead holds arrays of objects (#704); visit reaches its
// contactSnapshot through lead.json's $ref, which the parser does not
// resolve, so its ancestry is not derived (#937, until #705). Every other
// schema these directories ship is exported.
func TestFileRegistryCensusOfParquetEligibility(t *testing.T) {
	heldBack := map[string]map[string]string{
		"../../cmd/server/schemas": {
			"lead":  `attribute "propertyInterests.firstSeenAt" lies inside the array of objects "propertyInterests"`,
			"visit": `could not place attribute "contactSnapshot.annualIncome"`,
		},
	}
	for _, dir := range []string{
		"../../cmd/sample/schemas",
		"../../cmd/server/schemas",
		"../coordinationtest/testdata/schemas",
		"../e2e_harness/federated/benchmark/schemas",
		"../e2e_harness/production/schemas",
		"../testdata/unique_keys",
	} {
		registry, err := NewFileSchemaRegistryFromDirectory(dir)
		require.NoError(t, err, dir)
		for _, schema := range registry.ListSchemas() {
			_, cache, err := registry.GetSchemaAttributeCacheByName(schema)
			require.NoError(t, err, "%s: %s", dir, schema)
			reason := sqlgen.ParquetUnrepresentableReason(cache)
			if want, ok := heldBack[dir][schema]; ok {
				require.Contains(t, reason, want, "%s: %s", dir, schema)
				continue
			}
			require.Empty(t, reason, "%s: %s", dir, schema)
		}
	}
}
