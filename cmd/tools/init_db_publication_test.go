package main

import (
	"slices"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/publication"
)

// wantPublicationDDL is the golden text of the publication record and
// generation tables (#895), issued after the ledger. Independent literals,
// like the rest of wantDDLStatements: never rebuild them from the builders.
func wantPublicationDDL() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS "entity_main_publication" (
		schema_id         SMALLINT NOT NULL,
		epoch             BIGINT   NOT NULL,
		state             TEXT COLLATE "C" NOT NULL,
		generation        BIGINT   NOT NULL,
		parent_generation BIGINT   NOT NULL,
		manifest_key      TEXT COLLATE "C" NOT NULL,
		manifest_sha256   TEXT COLLATE "C" NOT NULL,
		lineage           UUID,
		cas_token         UUID     NOT NULL,
		updated_at        BIGINT   NOT NULL,
		gc_horizon        BIGINT   NOT NULL,
		PRIMARY KEY (schema_id)
	)`,
		`CREATE TABLE IF NOT EXISTS "entity_main_publication_generation" (
		schema_id         SMALLINT NOT NULL,
		generation        BIGINT   NOT NULL,
		epoch             BIGINT   NOT NULL,
		kind              TEXT COLLATE "C" NOT NULL,
		parent_generation BIGINT   NOT NULL,
		manifest_key      TEXT COLLATE "C" NOT NULL,
		manifest_sha256   TEXT COLLATE "C" NOT NULL,
		lineage           UUID     NOT NULL,
		cas_token         UUID     NOT NULL,
		committed_at      BIGINT   NOT NULL,
		superseded_at     BIGINT   NOT NULL DEFAULT 0,
		retired_at        BIGINT   NOT NULL DEFAULT 0,
		PRIMARY KEY (schema_id, generation)
	)`,
	}
}

// TestPublicationDDLUsesSharedSource pins init-db's publication tables to
// publication.Statements, the source the publication suites create them
// from, on both engines, so the tables init-db creates are the ones the
// contract's statements were checked against.
func TestPublicationDDLUsesSharedSource(t *testing.T) {
	for _, engine := range []forma.DatabaseEngine{forma.EnginePostgreSQL, forma.EngineAuroraDSQL} {
		opts := testInitDBOptions()
		opts.engine = engine
		var got []string
		for _, ddl := range publicationDDL(opts) {
			got = append(got, ddl.statement)
		}
		if want := publication.Statements(engine, opts.entityMain); !slices.Equal(got, want) {
			t.Fatalf("init-db publication DDL for %s differs from publication.Statements:\n got: %q\nwant: %q", engine, got, want)
		}
	}
}

func TestPublicationDDLAnnouncements(t *testing.T) {
	opts := testInitDBOptions()
	opts.entityMain = "app.entity_main"
	ddl := publicationDDL(opts)
	want := []tableDDL{
		{announce: "Created publication record table: app.entity_main_publication\n", failure: "ensure publication record table"},
		{announce: "Created publication generation table: app.entity_main_publication_generation\n", failure: "ensure publication generation table"},
	}
	if len(ddl) != len(want) {
		t.Fatalf("publicationDDL returned %d statements, want %d", len(ddl), len(want))
	}
	for i := range want {
		if ddl[i].announce != want[i].announce || ddl[i].failure != want[i].failure {
			t.Errorf("statement %d announces %q, fails as %q; want %q, %q", i, ddl[i].announce, ddl[i].failure, want[i].announce, want[i].failure)
		}
	}
}

// TestRequireDistinctPublicationTables refuses each configured or derived
// table a publication table's derived name could also be, naming the flags
// that resolve it, and accepts the defaults (R1-F2 of #897's review).
func TestRequireDistinctPublicationTables(t *testing.T) {
	if err := requireDistinctPublicationTables(testInitDBOptions()); err != nil {
		t.Fatalf("the default table names: %v", err)
	}
	for name, tc := range map[string]struct {
		set  func(*initDBOptions)
		want []string
	}{
		"the change log is the record table": {
			set: func(o *initDBOptions) { o.changeLog = "entity_main_publication" },
			want: []string{`"entity_main_publication" is the publication record table, derived from --entity-main-table entity_main`,
				"the change log table, which --change-log-table names", "change --entity-main-table or --change-log-table"},
		},
		"the EAV table is the generation table": {
			set: func(o *initDBOptions) { o.eavTable = "entity_main_publication_generation" },
			want: []string{`"entity_main_publication_generation" is the publication generation table`,
				"the EAV table, which --eav-table names"},
		},
		"the registry's generation ledger is the generation table": {
			set:  func(o *initDBOptions) { o.schemaTable = "entity_main_publication" },
			want: []string{"the schema registry table, which --schema-table names"},
		},
		"a schema-qualified schema table": {
			set: func(o *initDBOptions) {
				o.entityMain = "app.entity_main"
				o.schemaTable = "app.entity_main_publication"
			},
			want: []string{`"app"."entity_main_publication" is the publication record table`},
		},
	} {
		t.Run(name, func(t *testing.T) {
			opts := testInitDBOptions()
			tc.set(&opts)
			err := requireDistinctPublicationTables(opts)
			if err == nil {
				t.Fatal("requireDistinctPublicationTables accepted a publication table name another table holds")
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q lacks %q", err, want)
				}
			}
		})
	}
}
