package schemameta

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemaledger"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/require"
)

// expectActiveLedger expects the generation gate's query and answers it with
// generation 1 of each schema holding exactly the attributes file in dir, as
// if baseline-schema had recorded it: a loader test that is not about the
// gate sees it admit the declaration.
func expectActiveLedger(t *testing.T, mock pgxmock.PgxPoolIface, dir string, schemas map[string]int16) {
	t.Helper()
	rows := pgxmock.NewRows([]string{"schema_id", "active_generation", "attr_id", "attr_name",
		"value_type", "items_type", "column_name", "encoding", "retired"})
	names := make([]string, 0, len(schemas))
	for name := range schemas {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool { return schemas[names[i]] < schemas[names[j]] })
	for _, name := range names {
		for _, e := range fileEntries(t, filepath.Join(dir, name+"_attributes.json")) {
			itemsType, column, encoding := string(e.ItemsType), string(e.Column), string(e.Encoding)
			rows.AddRow(schemas[name], int64(1), &e.AttrID, &e.Name, ptr(string(e.ValueType)),
				&itemsType, &column, &encoding, ptr(e.Retired))
		}
	}
	mock.ExpectQuery(`LEFT JOIN`).WillReturnRows(rows)
}

func fileEntries(t *testing.T, path string) []schemaledger.Entry {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err, "read %s", path)
	var raw map[string]map[string]any
	require.NoError(t, json.Unmarshal(data, &raw), "parse %s", path)
	cache := forma.SchemaAttributeCache{}
	for name, attr := range raw {
		meta, err := parseAttributeMetadata(name, attr, path)
		require.NoError(t, err, "attribute %s", name)
		cache[name] = meta
	}
	return schemaledger.EntriesOf(cache)
}

func ptr[T any](v T) *T { return &v }
