package widthaudit

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/stretchr/testify/require"
)

// TestDomainTargetsSelectsStorageTheReadJudges: every EAV-only attribute is
// held to its value column (a list by its items type), and of the bound
// ones only bool_text and iso8601 can hold an image their column type
// admits and the read refuses.
func TestDomainTargetsSelectsStorageTheReadJudges(t *testing.T) {
	bound := func(name string, id int16, vt forma.ValueType, col string, enc forma.MainColumnEncoding) forma.AttributeMetadata {
		return forma.AttributeMetadata{AttributeName: name, AttributeID: id, ValueType: vt,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn(col), Encoding: enc}}
	}
	mc := schemameta.NewMetadataCache()
	require.NoError(t, mc.RegisterSchema("events", 300, forma.SchemaAttributeCache{
		"note":   {AttributeName: "note", AttributeID: 1, ValueType: forma.ValueTypeText},
		"seen":   {AttributeName: "seen", AttributeID: 2, ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeDateTime},
		"on":     bound("on", 3, forma.ValueTypeBool, "text_01", forma.MainColumnEncodingBoolText),
		"set":    bound("set", 4, forma.ValueTypeBool, "smallint_01", forma.MainColumnEncodingBoolInt),
		"due":    bound("due", 5, forma.ValueTypeDate, "text_02", forma.MainColumnEncodingISO8601),
		"at":     bound("at", 6, forma.ValueTypeDateTime, "bigint_01", forma.MainColumnEncodingUnixMs),
		"title":  bound("title", 7, forma.ValueTypeText, "text_03", forma.MainColumnEncodingDefault),
		"amount": bound("amount", 8, forma.ValueTypeNumeric, "double_01", forma.MainColumnEncodingDefault),
	}))
	want := []Target{
		{SchemaID: 300, SchemaName: "events", AttrID: 1, AttrName: "note", Declared: forma.ValueTypeText},
		{SchemaID: 300, SchemaName: "events", AttrID: 2, AttrName: "seen", Declared: forma.ValueTypeDateTime},
		{SchemaID: 300, SchemaName: "events", AttrID: 3, AttrName: "on", Declared: forma.ValueTypeBool, Column: "text_01"},
		{SchemaID: 300, SchemaName: "events", AttrID: 5, AttrName: "due", Declared: forma.ValueTypeDate, Column: "text_02"},
	}
	require.Equal(t, want, DomainTargets(mc))
}

// TestRefusedEAVValueNamesTheRefusedColumn: a value in the other column is
// the refusal, and value_text is quoted, so "12" in value_text is told
// apart from 12 in value_numeric.
func TestRefusedEAVValueNamesTheRefusedColumn(t *testing.T) {
	text, numeric := "12", "12"
	cases := []struct {
		name          string
		declared      forma.ValueType
		text, numeric *string
		want          string
	}{
		{"integer in value_text", forma.ValueTypeInteger, &text, nil, `"12"`},
		{"integer in both columns", forma.ValueTypeInteger, &text, &numeric, `"12"`},
		{"bool's own column", forma.ValueTypeBool, nil, &numeric, "12"},
		{"text in value_numeric", forma.ValueTypeText, &text, &numeric, "12"},
		{"uuid's own column", forma.ValueTypeUUID, &text, nil, `"12"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := refusedEAVValue(tc.declared, tc.text, tc.numeric)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
	_, err := refusedEAVValue(forma.ValueTypeBool, nil, nil)
	require.Error(t, err)
}
