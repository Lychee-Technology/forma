package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemameta"
)

// TestValidateSchemaConsistencyReportsBoolTextImagesTheReadRefuses (#857):
// under the pgx hot leg an unflushed row whose bool_text column the OLTP
// read refuses fails every federated page that holds it, where the attach
// hot leg read it as false. The census reports each such image in a real
// entity_main and nothing else: not "1", "0" or NULL, not the same column
// of a schema that binds no bool there, and not a text attribute's column.
func TestValidateSchemaConsistencyReportsBoolTextImagesTheReadRefuses(t *testing.T) {
	tg := newInitDBTarget(t)
	if out, err := tg.run(t); err != nil {
		t.Fatalf("init-db: %v\n%s", err, out)
	}
	cache := schemameta.NewMetadataCache()
	register := func(name string, id int16, attrs forma.SchemaAttributeCache) {
		if err := cache.RegisterSchema(name, id, attrs); err != nil {
			t.Fatalf("register schema %s: %v", name, err)
		}
	}
	register("visit", 100, forma.SchemaAttributeCache{
		"active": {AttributeName: "active", AttributeID: 1, ValueType: forma.ValueTypeBool,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: "text_01", Encoding: forma.MainColumnEncodingBoolText}},
		"note": {AttributeName: "note", AttributeID: 2, ValueType: forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: "text_02"}},
	})
	register("lead", 101, forma.SchemaAttributeCache{
		"label": {AttributeName: "label", AttributeID: 1, ValueType: forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: "text_01"}},
	})

	type row struct {
		schema int16
		image  *string
		note   string
	}
	img := func(s string) *string { return &s }
	rows := []row{
		{100, img("1"), "true"}, {100, img("0"), "true"}, {100, nil, "true"},
		{100, img("true"), "1"}, {100, img(""), "0"}, {100, img(" 1"), "1"}, {100, img("01"), "0"},
		{101, img("true"), "x"},
	}
	ids := make([]uuid.UUID, len(rows))
	ctx := context.Background()
	insert := fmt.Sprintf("INSERT INTO %s (ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, text_01, text_02) VALUES ($1, $2, 1, 1, $3, $4)",
		quoteIdentifier(tg.opts.entityMain))
	for i, r := range rows {
		ids[i] = uuid.New()
		if _, err := tg.pool.Exec(ctx, insert, r.schema, ids[i], r.image, r.note); err != nil {
			t.Fatalf("seed row %d: %v", i, err)
		}
	}

	v := schemaConsistencyValidator{
		pool:     tg.pool,
		eavTable: tg.opts.eavTable,
		widths:   widthAuditOptions{changeLogTable: tg.opts.changeLog, entityMainTable: tg.opts.entityMain},
		out:      io.Discard,
	}
	issues, err := v.checkBoundTextImages(ctx, cache)
	if err != nil {
		t.Fatalf("census: %v", err)
	}
	refused := map[uuid.UUID]string{ids[3]: `"true"`, ids[4]: `""`, ids[5]: `" 1"`, ids[6]: `"01"`}
	if len(issues) != len(refused) {
		t.Fatalf("got %d issues, want %d: %+v", len(issues), len(refused), issues)
	}
	for _, issue := range issues {
		if want := "bool_text images the OLTP read refuses in " + tg.opts.entityMain; issue.category != want || issue.severity != severityError {
			t.Errorf("issue %+v, want a failure under %q", issue, want)
		}
		found := false
		for id, value := range refused {
			want := fmt.Sprintf("schema=visit schema_id=100 attr_id=1 attribute=active declared=bool row_id=%s column=text_01 value=%s", id, value)
			found = found || strings.Contains(issue.details, want)
		}
		if !found {
			t.Errorf("issue %q names no refused row", issue.details)
		}
	}
}
