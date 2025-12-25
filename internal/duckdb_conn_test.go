package internal

import (
	"testing"

	"github.com/lychee-technology/forma"
)

func TestRenderS3ParquetPath(t *testing.T) {
	tmpl := "s3://bucket/path/schema_{{.SchemaID}}/data.parquet"
	got, err := RenderS3ParquetPath(tmpl, 42)
	if err != nil {
		t.Fatalf("RenderS3ParquetPath error: %v", err)
	}
	want := "s3://bucket/path/schema_42/data.parquet"
	if got != want {
		t.Fatalf("unexpected path, got=%s want=%s", got, want)
	}
}

func TestGenerateDuckDBWhereClause_SimpleKv(t *testing.T) {
	q := &FederatedAttributeQuery{
		AttributeQuery: AttributeQuery{
			Condition: &forma.KvCondition{
				Attr:  "username",
				Value: "equals:alice",
			},
		},
	}
	where, args, err := GenerateDuckDBWhereClause(q)
	if err != nil {
		t.Fatalf("GenerateDuckDBWhereClause error: %v", err)
	}
	if where != "username = ?" {
		t.Fatalf("unexpected where clause: %s", where)
	}
	if len(args) != 1 || args[0] != "alice" {
		t.Fatalf("unexpected args: %#v", args)
	}
}

func TestNewDuckDBClient_Disabled(t *testing.T) {
	cfg := forma.DuckDBConfig{
		Enabled:       false,
		DBPath:        ":memory:",
		MemoryLimitMB: 0,
	}
	_, err := NewDuckDBClient(cfg)
	if err == nil {
		t.Fatalf("expected error when duckdb disabled, got nil")
	}
}
