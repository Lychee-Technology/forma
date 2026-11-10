package widthaudit

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/rfc3339"
	"github.com/lychee-technology/forma/internal/schemameta"
)

// TestDomainTargetsSelectsStorageTheReadJudges: every EAV-only attribute is
// held to its value column (a list by its items type), and of the bound
// ones only bool_text and rfc3339 can hold an image their column type
// admits and the read refuses. Every bound attribute's eav_data rows from
// before its promotion are held to the EAV rules.
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
		"due":    bound("due", 5, forma.ValueTypeDate, "text_02", forma.MainColumnEncodingRFC3339),
		"at":     bound("at", 6, forma.ValueTypeDateTime, "bigint_01", forma.MainColumnEncodingUnixMs),
		"title":  bound("title", 7, forma.ValueTypeText, "text_03", forma.MainColumnEncodingDefault),
		"amount": bound("amount", 8, forma.ValueTypeNumeric, "double_01", forma.MainColumnEncodingDefault),
	}))
	want := []Target{
		{SchemaID: 300, SchemaName: "events", AttrID: 1, AttrName: "note", Declared: forma.ValueTypeText},
		{SchemaID: 300, SchemaName: "events", AttrID: 2, AttrName: "seen", Declared: forma.ValueTypeDateTime},
		{SchemaID: 300, SchemaName: "events", AttrID: 3, AttrName: "on", Declared: forma.ValueTypeBool, Column: "text_01"},
		{SchemaID: 300, SchemaName: "events", AttrID: 3, AttrName: "on", Declared: forma.ValueTypeBool},
		{SchemaID: 300, SchemaName: "events", AttrID: 4, AttrName: "set", Declared: forma.ValueTypeBool},
		{SchemaID: 300, SchemaName: "events", AttrID: 5, AttrName: "due", Declared: forma.ValueTypeDate, Column: "text_02"},
		{SchemaID: 300, SchemaName: "events", AttrID: 5, AttrName: "due", Declared: forma.ValueTypeDate},
		{SchemaID: 300, SchemaName: "events", AttrID: 6, AttrName: "at", Declared: forma.ValueTypeDateTime},
		{SchemaID: 300, SchemaName: "events", AttrID: 7, AttrName: "title", Declared: forma.ValueTypeText},
		{SchemaID: 300, SchemaName: "events", AttrID: 8, AttrName: "amount", Declared: forma.ValueTypeNumeric},
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

// TestDomainCensusJudgesRFC3339Images (#842): an rfc3339 image is dropped
// only when it is the canonical image of its instant. One the read refuses
// is outside the declared type, the canonical shape naming no instant
// included; one the read parses but that differs from the canonical image
// is kept with its defect and the canonical image a rewrite stores, or
// none when its instant has no image. A bool_text image other than 1/0 is
// outside its type.
func TestDomainCensusJudgesRFC3339Images(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	targets := []Target{
		{SchemaID: 300, SchemaName: "events", AttrID: 3, AttrName: "on", Declared: forma.ValueTypeBool, Column: "text_01"},
		{SchemaID: 300, SchemaName: "events", AttrID: 5, AttrName: "due", Declared: forma.ValueTypeDateTime, Column: "text_02"},
	}
	tables := Tables{EntityMain: "entity_main"}
	query, args := BuildDomainColumnCensusQuery(tables, targets)
	images := []string{
		"2024-01-02T03:04:05.250Z",      // canonical
		"2024-01-02T03:04:05.000Z",      // canonical
		"2024-01-02T03:04:05Z",          // whole-second
		"2024-01-02T11:04:05.250+08:00", // offset
		"2024-01-02T03:04:05.5Z",        // short fraction
		"2024-01-02T03:04:05.2509Z",     // sub-millisecond
		"9999-12-31T23:30:00-01:00",     // no canonical image
		"2024-02-30T03:04:05.250Z",      // canonical shape, no such day
		"not a time",
	}
	rows := pgxmock.NewRows(strings.Fields("ltbase_schema_id attr_id ltbase_row_id image pending last_flushed_at"))
	ids := make([]uuid.UUID, len(images)+1)
	ids[0] = uuid.New()
	rows.AddRow(int16(300), int16(3), ids[0], "true", false, int64(0))
	for i, image := range images {
		ids[i+1] = uuid.New()
		rows.AddRow(int16(300), int16(5), ids[i+1], image, false, int64(0))
	}
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(args...).WillReturnRows(rows)

	findings, err := DomainCensus(context.Background(), mock, tables, targets)
	require.NoError(t, err)
	type judged struct {
		attr              int16
		stored, canonical string
		defect            rfc3339.Defect
	}
	got := map[uuid.UUID]judged{}
	for _, f := range findings {
		got[f.RowID] = judged{f.AttrID, f.StoredValue, f.Canonical, f.NonCanonical}
	}
	require.Equal(t, map[uuid.UUID]judged{
		ids[0]: {3, `"true"`, "", ""},
		ids[3]: {5, `"2024-01-02T03:04:05Z"`, "2024-01-02T03:04:05.000Z", rfc3339.Respelled},
		ids[4]: {5, `"2024-01-02T11:04:05.250+08:00"`, "2024-01-02T03:04:05.250Z", rfc3339.Respelled},
		ids[5]: {5, `"2024-01-02T03:04:05.5Z"`, "2024-01-02T03:04:05.500Z", rfc3339.Respelled},
		ids[6]: {5, `"2024-01-02T03:04:05.2509Z"`, "2024-01-02T03:04:05.250Z", rfc3339.SubMillisecond},
		ids[7]: {5, `"9999-12-31T23:30:00-01:00"`, "", rfc3339.NoCanonicalImage},
		ids[8]: {5, `"2024-02-30T03:04:05.250Z"`, "", ""},
		ids[9]: {5, `"not a time"`, "", ""},
	}, got)
	require.NoError(t, mock.ExpectationsWereMet())
}
