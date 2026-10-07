package main

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
)

func TestNewSchemaIDs(t *testing.T) {
	tests := []struct {
		name       string
		names      []string
		registered map[string]int16
		want       map[string]int16
	}{
		{
			name:  "an empty registry numbers the schemas from 100 in order",
			names: []string{"activity", "lead", "visit"},
			want:  map[string]int16{"activity": 100, "lead": 101, "visit": 102},
		},
		{
			name:       "a re-run with the same schemas assigns nothing",
			names:      []string{"lead", "visit"},
			registered: map[string]int16{"lead": 100, "visit": 101},
			want:       map[string]int16{},
		},
		{
			name:       "a schema sorting after the registered ones gets its positional id",
			names:      []string{"lead", "visit", "zone"},
			registered: map[string]int16{"lead": 100, "visit": 101},
			want:       map[string]int16{"zone": 102},
		},
		{
			// Positional numbering gave account 100, lead's id (#643).
			name:       "a schema sorting before the registered ones gets the next free id",
			names:      []string{"account", "lead", "visit"},
			registered: map[string]int16{"lead": 100, "visit": 101},
			want:       map[string]int16{"account": 102},
		},
		{
			name:       "a gap left by a deleted registration is not refilled",
			names:      []string{"a", "b", "c"},
			registered: map[string]int16{"a": 100, "c": 102},
			want:       map[string]int16{"b": 103},
		},
		{
			name:       "a registered schema without a file still holds its id",
			names:      []string{"lead"},
			registered: map[string]int16{"retired": 104},
			want:       map[string]int16{"lead": 105},
		},
		{
			name:       "ids below 100 do not pull new ids under 100",
			names:      []string{"legacy", "lead"},
			registered: map[string]int16{"legacy": 5},
			want:       map[string]int16{"lead": 100},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newSchemaIDs(tt.names, tt.registered)
			if err != nil {
				t.Fatalf("newSchemaIDs: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("newSchemaIDs = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewSchemaIDsRefusesPastSmallint(t *testing.T) {
	registered := map[string]int16{"last": 32767}
	if _, err := newSchemaIDs([]string{"last"}, registered); err != nil {
		t.Fatalf("a registered schema needs no new id, got %v", err)
	}

	_, err := newSchemaIDs([]string{"last", "next"}, registered)
	if err == nil {
		t.Fatal("an id past 32767 would wrap the SMALLINT schema_id, want an error")
	}
	for _, want := range []string{"schema next", "32768", "SMALLINT"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not name %q", err, want)
		}
	}
}

// TestRegisterSchemasAddsNewSchemaToExistingRegistry is the re-run that
// failed before #643: account.json sorts before the registered schemas, so
// its positional id 100 collided with lead's.
func TestRegisterSchemasAddsNewSchemaToExistingRegistry(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"account.json", "lead.json", "lead_attributes.json", "visit.json", "notes.txt"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(`{}`), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	if err := os.Mkdir(filepath.Join(dir, "nested.json"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	mock, err := pgxmock.NewConn()
	if err != nil {
		t.Fatalf("create mock conn: %v", err)
	}
	t.Cleanup(func() { _ = mock.Close(context.Background()) })
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT schema_name, schema_id FROM "schema_registry_dev"`).
		WillReturnRows(pgxmock.NewRows([]string{"schema_name", "schema_id"}).
			AddRow("lead", int16(100)).
			AddRow("visit", int16(101)))
	mock.ExpectExec(`INSERT INTO "schema_registry_dev" \(schema_name, schema_id\) VALUES \(\$1, \$2\) ON CONFLICT \(schema_name\) DO NOTHING`).
		WithArgs("account", int16(102)).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	tx, err := mock.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	var regErr error
	stdout, _ := captureStdStreams(t, func() {
		regErr = registerSchemas(context.Background(), tx, "schema_registry_dev", dir)
	})
	if regErr != nil {
		t.Fatalf("registerSchemas: %v", regErr)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("registered schemas must not be re-inserted: %v", err)
	}

	wantLines := []string{
		"Registered schema, name: account, id: 102",
		"Schema already exists, schema name: lead",
		"Schema already exists, schema name: visit",
		"Registered schemas from directory, count: 3",
	}
	for _, want := range wantLines {
		if !strings.Contains(stdout, want) {
			t.Fatalf("init-db output %q lacks %q", stdout, want)
		}
	}
}

// TestListSchemaNamesOrdersByFileName pins the order that fixes a first run's
// ids: by file name, as before #643, not by the trimmed name ("-" sorts
// before ".", so lead-v2.json comes before lead.json).
func TestListSchemaNamesOrdersByFileName(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"lead.json", "lead-v2.json", "lead_attributes.json", "account.json"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(`{}`), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	got, err := listSchemaNames(dir)
	if err != nil {
		t.Fatalf("listSchemaNames: %v", err)
	}
	if want := []string{"account", "lead-v2", "lead"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("listSchemaNames = %v, want %v", got, want)
	}
}
