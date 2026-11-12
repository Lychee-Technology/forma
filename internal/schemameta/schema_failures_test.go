package schemameta

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/require"
)

// Two schemas that each fail validateSchemaAttributeCache for a different
// reason, and one that is valid. Their names sort alpha < beta < gamma.
const (
	dupAttrIDAttrs  = `{"a":{"attributeID":7,"valueType":"text"},"b":{"attributeID":7,"valueType":"text"}}`
	dupBindingAttrs = `{"a":{"attributeID":1,"valueType":"text","column_binding":{"col_name":"text_01"}},` +
		`"b":{"attributeID":2,"valueType":"text","column_binding":{"col_name":"text_01"}}}`
	validAttrs = `{"status":{"attributeID":1,"valueType":"text"}}`
)

func writeAggregateFixture(t *testing.T, withSchemaFiles bool) string {
	t.Helper()
	dir := t.TempDir()
	for name, attrs := range map[string]string{"alpha": dupAttrIDAttrs, "beta": validAttrs, "gamma": dupBindingAttrs} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name+"_attributes.json"), []byte(attrs), 0o644))
		if withSchemaFiles {
			require.NoError(t, os.WriteFile(filepath.Join(dir, name+".json"), []byte(`{"type":"object"}`), 0o644))
		}
	}
	return dir
}

// requireBothFailuresInOrder pins the aggregate: a count header, then one
// diagnosis per failing schema, sorted by name, and nothing naming the valid
// schema.
func requireBothFailuresInOrder(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	msg := err.Error()
	require.Contains(t, msg, "2 schemas failed to load")
	alpha := strings.Index(msg, "schema alpha has duplicate attribute id 7")
	gamma := strings.Index(msg, "duplicate column binding")
	require.NotEqual(t, -1, alpha, "alpha's diagnosis missing: %s", msg)
	require.NotEqual(t, -1, gamma, "gamma's diagnosis missing: %s", msg)
	require.Contains(t, msg, "gamma_attributes.json")
	require.Less(t, alpha, gamma, "diagnoses must be sorted by schema name: %s", msg)
	require.NotContains(t, msg, "beta")
}

func TestDirectoryRegistryReportsEveryFailingSchema(t *testing.T) {
	registry, err := NewFileSchemaRegistryFromDirectory(writeAggregateFixture(t, true))
	requireBothFailuresInOrder(t, err)
	require.Nil(t, registry, "a registry with a failing schema must not construct, so beta is never registered")
}

// The directory loader assigns ids by sorted position whether or not a schema
// before it failed, so a diagnosis names the id its schema gets once every
// schema loads.
func TestDirectoryRegistryIDsIgnoreEarlierFailures(t *testing.T) {
	r := newFileSchemaRegistry(nil, "", writeAggregateFixture(t, true))
	require.Error(t, r.loadSchemasFromDirectory())
	require.Equal(t, map[string]int16{"beta": 101}, r.nameToID)
}

func TestLoadMetadataReportsEveryFailingSchema(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	// Unsorted rows: the report's order must not follow the table's.
	rows := pgxmock.NewRows([]string{"schema_name", "schema_id"}).
		AddRow("gamma", int16(3)).
		AddRow("beta", int16(2)).
		AddRow("alpha", int16(1))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT schema_name, schema_id FROM ` + sanitizeIdentifier("agg_reg"))).WillReturnRows(rows)

	cache, err := NewMetadataLoader(mock, "agg_reg", writeAggregateFixture(t, false)).LoadMetadata(context.Background())
	requireBothFailuresInOrder(t, err)
	require.Nil(t, cache)
	// The generation gate never runs: no ledger query was expected.
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSchemaFailures(t *testing.T) {
	var none schemaFailures
	require.NoError(t, none.err())

	only := errors.New("schema x: broken")
	var one schemaFailures
	one.add("x", only)
	require.Same(t, only, one.err(), "a lone failure keeps its message unchanged")

	errA, errB := errors.New("schema a: broken"), errors.New("schema b: broken")
	var two schemaFailures
	two.add("b", errB)
	two.add("a", errA)
	err := two.err()
	require.ErrorIs(t, err, errA)
	require.ErrorIs(t, err, errB)
	require.Equal(t, "2 schemas failed to load:\nschema a: broken\nschema b: broken", err.Error())
}
