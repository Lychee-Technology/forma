package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"
)

// TestIntegerWidthCensusSpansTheBindParameterLimit: the census binds a few
// parameters per target, and the extended protocol carries at most 65,535 in
// one statement. A registry needing more is still censused in full. Here
// 1,900 schemas each bind nine smallints to wider columns and store nothing
// in eav_data: 17,100 column targets and as many retained-eav_data targets,
// past the limit on both legs. The findings planted under the last schema
// show that the later batches ran.
func TestIntegerWidthCensusSpansTheBindParameterLimit(t *testing.T) {
	tg := newInitDBTarget(t)
	if out, err := tg.run(t); err != nil {
		t.Fatalf("init-db: %v\n%s", err, out)
	}
	const schemas = 1900
	cache := widerSmallintRegistry(t, schemas)

	ctx := context.Background()
	row := uuid.New()
	for _, stmt := range []string{
		fmt.Sprintf("INSERT INTO %s (schema_id, row_id, attr_id, value_numeric) VALUES (%d, '%s', 9, 40000)",
			quoteIdentifier(tg.opts.eavTable), schemas, row),
		fmt.Sprintf("INSERT INTO %s (ltbase_schema_id, ltbase_row_id, integer_01, ltbase_created_at, ltbase_updated_at) VALUES (%d, '%s', 40000, 0, 0)",
			quoteIdentifier(tg.opts.entityMain), schemas, row),
	} {
		if _, err := tg.pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	v := schemaConsistencyValidator{
		pool:     tg.pool,
		eavTable: tg.opts.eavTable,
		widths:   widthAuditOptions{changeLogTable: tg.opts.changeLog, entityMainTable: tg.opts.entityMain},
		out:      io.Discard,
	}
	issues, err := v.checkIntegerWidthExports(ctx, cache)
	if err != nil {
		t.Fatalf("census over %d schemas: %v", schemas, err)
	}
	want := []string{
		fmt.Sprintf("schema_id=%d attr_id=9 attribute=n9 declared=smallint row_id=%s value=40000", schemas, row),
		fmt.Sprintf("schema_id=%d attr_id=1 attribute=n1 declared=smallint row_id=%s column=integer_01 value=40000", schemas, row),
	}
	if len(issues) != len(want) {
		t.Fatalf("got %d issues, want %d: %+v", len(issues), len(want), issues)
	}
	for i, w := range want {
		if !strings.Contains(issues[i].details, w) {
			t.Errorf("issue %d = %q, want it to contain %q", i, issues[i].details, w)
		}
	}
}

// widerSmallintRegistry registers schemas 1..n, each binding a smallint to
// every integer_*, bigint_* and double_* column (attr_ids 1-9 in that order).
func widerSmallintRegistry(t *testing.T, n int) *schemameta.MetadataCache {
	t.Helper()
	var wider []string
	wider = append(append(append(wider, model.IntegerColumns...), model.BigintColumns...), model.DoubleColumns...)
	cache := schemameta.NewMetadataCache()
	for id := 1; id <= n; id++ {
		attrs := forma.SchemaAttributeCache{}
		for i, col := range wider {
			name := fmt.Sprintf("n%d", i+1)
			attrs[name] = forma.AttributeMetadata{AttributeName: name, AttributeID: int16(i + 1), ValueType: forma.ValueTypeSmallInt,
				ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn(col)}}
		}
		if err := cache.RegisterSchema(fmt.Sprintf("s%04d", id), int16(id), attrs); err != nil {
			t.Fatalf("register schema %d: %v", id, err)
		}
	}
	return cache
}
