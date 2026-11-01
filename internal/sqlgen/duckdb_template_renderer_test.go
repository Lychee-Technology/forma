package sqlgen

import (
	"fmt"
	"strings"
	"testing"
	"text/template"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Tests for buildNonKeysetOrderBy
// ============================================================================

func TestBuildNonKeysetOrderBy_NilQuery_ReturnsDefault(t *testing.T) {
	got := buildNonKeysetOrderBy(nil)
	require.Equal(t, "created_at DESC, row_id ASC", got)
}

func TestBuildNonKeysetOrderBy_NoOrders_ReturnsDefault(t *testing.T) {
	q := &model.FederatedAttributeQuery{}
	got := buildNonKeysetOrderBy(q)
	require.Equal(t, "created_at DESC, row_id ASC", got)
}

func TestBuildNonKeysetOrderBy_MainColumn_ASC(t *testing.T) {
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{
			AttributeOrders: []model.AttributeOrder{
				{
					StorageLocation: forma.AttributeStorageLocationMain,
					ColumnName:      "text_01",
					AttrName:        "title",
					SortOrder:       forma.SortOrderAsc,
				},
			},
		},
	}
	got := buildNonKeysetOrderBy(q)
	require.Equal(t, "text_01 ASC, row_id ASC", got)
}

func TestBuildNonKeysetOrderBy_MainColumn_DESC(t *testing.T) {
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{
			AttributeOrders: []model.AttributeOrder{
				{
					StorageLocation: forma.AttributeStorageLocationMain,
					ColumnName:      "num_01",
					AttrName:        "price",
					SortOrder:       forma.SortOrderDesc,
				},
			},
		},
	}
	got := buildNonKeysetOrderBy(q)
	require.Equal(t, "num_01 DESC, row_id ASC", got)
}

// TestBuildNonKeysetOrderBy_EAVColumn uses the attribute's logical name (AttrName)
// as the ORDER BY column, since the unified CTE projects EAV attributes by their name.
func TestBuildNonKeysetOrderBy_EAVColumn_UsesAttrName(t *testing.T) {
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{
			AttributeOrders: []model.AttributeOrder{
				{
					StorageLocation: forma.AttributeStorageLocationEAV,
					AttrName:        "tag",
					SortOrder:       forma.SortOrderAsc,
				},
			},
		},
	}
	got := buildNonKeysetOrderBy(q)
	require.Equal(t, "tag ASC, row_id ASC", got)
}

// TestBuildNonKeysetOrderBy_EAVColumn_NoAttrName_FallsBackToDefault ensures that
// an EAV model.AttributeOrder with no AttrName set is skipped and the default is returned.
func TestBuildNonKeysetOrderBy_EAVColumn_NoAttrName_FallsBackToDefault(t *testing.T) {
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{
			AttributeOrders: []model.AttributeOrder{
				{
					StorageLocation: forma.AttributeStorageLocationEAV,
					// AttrName intentionally empty — simulates legacy/incomplete data
				},
			},
		},
	}
	got := buildNonKeysetOrderBy(q)
	require.Equal(t, "created_at DESC, row_id ASC", got)
}

// TestBuildNonKeysetOrderBy_Mixed_MainAndEAV confirms that both main-column and
// EAV sort keys are resolved and combined in order.
func TestBuildNonKeysetOrderBy_Mixed_MainAndEAV(t *testing.T) {
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{
			AttributeOrders: []model.AttributeOrder{
				{
					StorageLocation: forma.AttributeStorageLocationMain,
					ColumnName:      "text_01",
					AttrName:        "title",
					SortOrder:       forma.SortOrderAsc,
				},
				{
					StorageLocation: forma.AttributeStorageLocationEAV,
					AttrName:        "priority",
					SortOrder:       forma.SortOrderDesc,
				},
			},
		},
	}
	got := buildNonKeysetOrderBy(q)
	require.Equal(t, "text_01 ASC, priority DESC, row_id ASC", got)
}

// TestBuildNonKeysetOrderBy_AlwaysAppendsRowIDTiebreak pins the stable-sort
// contract (#183): equal user-sort keys must not leave page windows to DuckDB
// scan order. Mirrors the PG optimized template (trailing m.ltbase_row_id) and
// the production-harness oracle (row_id ASC).
func TestBuildNonKeysetOrderBy_AlwaysAppendsRowIDTiebreak(t *testing.T) {
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{
			AttributeOrders: []model.AttributeOrder{
				{
					StorageLocation: forma.AttributeStorageLocationEAV,
					AttrName:        "qty",
					SortOrder:       forma.SortOrderAsc,
				},
			},
		},
	}
	require.Equal(t, "qty ASC, row_id ASC", buildNonKeysetOrderBy(q))
}

// TestBuildDuckDBQuery_PropagatesRenderError verifies that a render-time
// (template Execute) failure inside BuildDuckDBQuery is surfaced to the caller
// rather than swallowed. It replaces the retired federated stub
// TestBuildDuckDBQuery_InvalidTemplateSyntax (#196), whose premise was stale:
// BuildDuckDBQuery receives an already-parsed *template.Template, so a *parse*
// error cannot occur at that boundary — only an Execute error can. A template
// calling ident on an invalid identifier forces exactly that Execute error,
// because RenderSQLTemplate re-binds "ident" to the validating renderer.
func TestBuildDuckDBQuery_PropagatesRenderError(t *testing.T) {
	// A placeholder ident func is registered so the template parses; the real
	// validating ident is injected by RenderSQLTemplate at render time.
	tpl := template.Must(template.New("bad").Funcs(template.FuncMap{
		"ident": func(name string) string { return name },
	}).Parse(`SELECT * FROM {{ident .BadIdent}}`))

	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 1, Limit: 10, Offset: 0},
	}

	// dual == nil forces the generic render path through RenderSQLTemplate.
	_, _, err := BuildDuckDBQuery(tpl, map[string]any{"BadIdent": "bad-name"}, q, nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid SQL identifier")
}

// ============================================================================
// Tests for the PostgreSQL attach statement's SQL-literal escaping (#301, #752)
// ============================================================================

// TestPostgresAttachSQLEscapesTheConnString pins the half of the #301
// producer fix that lives in sqlgen.
//
// The attach statement interpolates the DSN inside a single-quoted literal,
// ATTACH '<conn>' AS …, so once federated.DuckDBPostgresConnStringFromPool
// started quoting its values, the raw string carried single quotes that would
// terminate that literal early and break every federated query. Escaping also
// closes a hole that predates the quoting: a Postgres password containing a
// single quote was interpolated raw, putting deployment-controlled text into
// SQL structure.
func TestPostgresAttachSQLEscapesTheConnString(t *testing.T) {
	tests := []struct {
		name string
		conn string
		want string
	}{
		{
			name: "quoted DSN has its quotes doubled",
			conn: `host='h' port=5432 user='u' password='s3cr3t' dbname='d'`,
			want: `host=''h'' port=5432 user=''u'' password=''s3cr3t'' dbname=''d''`,
		},
		{
			name: "password containing a quote cannot break out of the literal",
			conn: `host='h' password='p\'w' dbname='d'`,
			want: `host=''h'' password=''p\''w'' dbname=''d''`,
		},
		{
			name: "unquoted legacy DSN is unchanged",
			conn: "host=h port=5432 dbname=d",
			want: "host=h port=5432 dbname=d",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PostgresAttachSQL(tt.conn)
			prefix := "ATTACH IF NOT EXISTS '"
			suffix := "' AS " + PostgresCatalogAlias(tt.conn) + " (TYPE postgres, READ_ONLY);"
			require.True(t, strings.HasPrefix(got, prefix+tt.want+suffix), "attach statement: %s", got)

			// Every single quote in the literal must be part of a doubled
			// pair, or the literal ends early.
			literal := strings.TrimSuffix(strings.TrimPrefix(strings.SplitN(got, "\n", 2)[0], prefix), suffix)
			require.Equal(t, 0, countUnpairedQuotes(literal),
				"the attach literal carries an unpaired quote and would end early: %s", literal)
		})
	}
}

// The rendered query names the attached catalog by an alias that is stable
// for one connection string and distinct across them, and never carries the
// connection string: execution plans and logs quote the rendered query, and
// before #752 every postgres_scan call in it held the password (#306).
func TestRenderedQueryNamesTheCatalogWithoutTheConnString(t *testing.T) {
	const conn = `host='h' user='u' password='do-not-render-752' dbname='d'`
	params := map[string]any{
		"DuckDBPGConnString": conn,
		"ChangeLogSchema":    "public", "ChangeLogScanTable": "change_log",
		"MainSchema": "public", "MainScanTable": "entity_main",
		"EAVSchema": "public", "EAVScanTable": "eav_data",
	}
	require.NoError(t, injectDuckDBTemplateParams(params, &model.FederatedAttributeQuery{}, nil))

	alias := PostgresCatalogAlias(conn)
	require.Regexp(t, `^forma_pg_[0-9a-f]{16}$`, alias)
	require.Equal(t, alias, PostgresCatalogAlias(conn), "the alias must be stable for one connection string")
	require.NotEqual(t, alias, PostgresCatalogAlias(conn+" port=5433"), "different connection strings must not share a catalog")
	require.Equal(t, alias+`."public"."change_log"`, params["PG_CHANGE_LOG"])
	require.Equal(t, alias+`."public"."entity_main"`, params["PG_MAIN"])
	require.Equal(t, alias+`."public"."eav_data"`, params["PG_EAV"])
	for key, v := range params {
		if key == "DuckDBPGConnString" {
			continue
		}
		require.NotContains(t, fmt.Sprint(v), "do-not-render-752", "param %s carries the password", key)
	}
}

// countUnpairedQuotes returns the number of single quotes not part of a doubled
// pair, scanning left to right.
func countUnpairedQuotes(s string) int {
	unpaired := 0
	for i := 0; i < len(s); i++ {
		if s[i] != '\'' {
			continue
		}
		if i+1 < len(s) && s[i+1] == '\'' {
			i++
			continue
		}
		unpaired++
	}
	return unpaired
}
