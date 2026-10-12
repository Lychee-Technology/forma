// Package declaredindex turns the composite indexes schemas declare under
// x-forma-indexes (#647) into physical indexes on entity_main and builds the
// missing ones without blocking writes.
//
// It is the one source of declared-index DDL: init-db and the production and
// federated e2e harnesses all plan through Plan, render through
// Index.CreateSQL and build through Ensure, so a harness database carries the
// same declared indexes an init-db database does.
package declaredindex

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// Key columns every declared index carries around the declared ones: the
// schema id leads, so schemas binding the same tuple share one index without
// reading each other's rows, and the row id trails, giving the total order
// keyset pagination and the row-id tiebreak need.
const (
	leadingColumn  = string(forma.MainColumnSchemaID)
	trailingColumn = string(forma.MainColumnRowID)
)

// nameMarker separates the table-derived prefix of a declared index name from
// its tuple digest. Other entity_main indexes end in "_idx", so no index
// init-db has ever created carries it.
const nameMarker = "_cx_"

// digestLen is the number of hex characters of the tuple digest in a name:
// 64 bits, so distinct tuples on one table do not collide in practice.
const digestLen = 16

// maxIdentifierLen is PostgreSQL's NAMEDATALEN-1. A longer name would be
// truncated by the server, and a later run would no longer find it by name.
const maxIdentifierLen = 63

// SchemaDeclarations is one schema's index declarations with the attribute
// metadata that resolves each declared attribute to its bound column.
type SchemaDeclarations struct {
	Schema       string
	Declarations []forma.IndexDeclaration
	Attributes   forma.SchemaAttributeCache
}

// Source names one declaration a physical index serves.
type Source struct {
	Schema string
	Index  string
}

// String renders the source as schema.index.
func (s Source) String() string {
	return s.Schema + "." + s.Index
}

// Index is one physical declared index on the entity_main table.
type Index struct {
	// Table is the entity_main table name as configured, possibly
	// schema-qualified.
	Table string
	// Name is derived from Table and Columns, never from a schema or
	// declaration name, so equal tuples get equal names.
	Name string
	// Columns is the full key in order: ltbase_schema_id, the bound column of
	// each declared attribute, ltbase_row_id.
	Columns []string
	// Sources are the declarations that resolve to Columns, sorted.
	Sources []Source
}

// CreateSQL renders the statement that builds the index. It is the only place
// declared-index DDL is spelled. CONCURRENTLY keeps the build from blocking
// writes, so the statement must run on its own, outside a transaction block;
// IF NOT EXISTS makes a re-run a no-op even when Ensure's catalog read raced
// another builder.
func (ix Index) CreateSQL() string {
	cols := make([]string, len(ix.Columns))
	for i, col := range ix.Columns {
		if col == leadingColumn || col == trailingColumn {
			cols[i] = col
			continue
		}
		cols[i] = pgx.Identifier{col}.Sanitize()
	}
	return fmt.Sprintf("CREATE INDEX CONCURRENTLY IF NOT EXISTS %s ON %s (%s)",
		pgx.Identifier{ix.Name}.Sanitize(), sqlutil.SanitizeIdentifier(ix.Table), strings.Join(cols, ", "))
}

// Plan resolves every declaration to its column tuple and returns one Index
// per distinct tuple, sorted by name. Declarations from any schemas that bind
// to the same tuple share one Index, because the leading ltbase_schema_id
// keeps each schema's rows in their own key range. The result depends only on
// the table name and the resolved tuples, not on input order.
func Plan(table string, schemas []SchemaDeclarations) ([]Index, error) {
	if strings.TrimSpace(table) == "" {
		return nil, errors.New("the entity_main table name is empty")
	}

	byTuple := make(map[string]*Index)
	for _, schema := range schemas {
		for _, decl := range schema.Declarations {
			cols, err := resolveColumns(schema, decl)
			if err != nil {
				return nil, fmt.Errorf("plan declared index %s.%s: %w", schema.Schema, decl.Name, err)
			}
			key := strings.Join(cols, ",")
			ix, ok := byTuple[key]
			if !ok {
				ix = &Index{Table: table, Name: Name(table, cols), Columns: cols}
				byTuple[key] = ix
			}
			ix.Sources = append(ix.Sources, Source{Schema: schema.Schema, Index: decl.Name})
		}
	}

	planned := make([]Index, 0, len(byTuple))
	for _, ix := range byTuple {
		sort.Slice(ix.Sources, func(i, j int) bool { return ix.Sources[i].String() < ix.Sources[j].String() })
		planned = append(planned, *ix)
	}
	sort.Slice(planned, func(i, j int) bool { return planned[i].Name < planned[j].Name })
	return planned, nil
}

// resolveColumns maps a declaration to its full key. A loaded registry has
// already refused unbound attributes; this check keeps Plan safe for callers
// that build SchemaDeclarations by hand.
func resolveColumns(schema SchemaDeclarations, decl forma.IndexDeclaration) ([]string, error) {
	if len(decl.Attributes) == 0 {
		return nil, errors.New("the declaration lists no attributes")
	}
	cols := make([]string, 0, len(decl.Attributes)+2)
	cols = append(cols, leadingColumn)
	for _, attr := range decl.Attributes {
		meta, ok := schema.Attributes[attr]
		if !ok || meta.ColumnBinding == nil {
			return nil, fmt.Errorf("attribute %s has no column binding; only column-bound attributes can be indexed", attr)
		}
		cols = append(cols, string(meta.ColumnBinding.ColumnName))
	}
	return append(cols, trailingColumn), nil
}

// Name derives the physical name of the declared index on table over cols:
// the table-derived prefix, "_cx_", and the first 16 hex characters of the
// SHA-256 of the column tuple. A prefix that would push the name past 63
// bytes is cut short; Ensure reports the rare clash that could cause rather
// than skipping the build.
func Name(table string, cols []string) string {
	sum := sha256.Sum256([]byte(strings.Join(cols, ",")))
	return namePrefix(table) + hex.EncodeToString(sum[:])[:digestLen]
}

// namePrefix is the table-derived part of every declared index name on table,
// marker included. It sanitizes the table the way init-db's makeIndexName
// does: a schema qualifier joins with "_" and quotes are dropped.
func namePrefix(table string) string {
	base := strings.ReplaceAll(table, ".", "_")
	base = strings.ReplaceAll(base, `"`, "")
	if limit := maxIdentifierLen - len(nameMarker) - digestLen; len(base) > limit {
		base = base[:limit]
	}
	return base + nameMarker
}

// isDeclaredName reports whether name has the shape Name gives an index on
// table, whichever tuple it covers. Ensure uses it to find declared indexes no
// current declaration plans.
func isDeclaredName(table, name string) bool {
	digest, ok := strings.CutPrefix(name, namePrefix(table))
	if !ok || len(digest) != digestLen {
		return false
	}
	_, err := hex.DecodeString(digest)
	return err == nil
}

// FromRegistry collects the declarations of every schema the registry lists,
// with each one's attribute cache, sorted by schema name. A schema without a
// schema document (only an attributes file) declares nothing, and so does a
// nil registry: init-db passes one for a schema directory with no schemas.
func FromRegistry(registry forma.SchemaRegistry) ([]SchemaDeclarations, error) {
	if registry == nil {
		return nil, nil
	}
	names := append([]string(nil), registry.ListSchemas()...)
	sort.Strings(names)

	var schemas []SchemaDeclarations
	for _, name := range names {
		_, doc, err := registry.GetSchemaByName(name)
		if errors.Is(err, forma.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("schema %s: %w", name, err)
		}
		if len(doc.Indexes) == 0 {
			continue
		}
		_, cache, err := registry.GetSchemaAttributeCacheByName(name)
		if err != nil {
			return nil, fmt.Errorf("schema %s: read attribute metadata: %w", name, err)
		}
		schemas = append(schemas, SchemaDeclarations{Schema: name, Declarations: doc.Indexes, Attributes: cache})
	}
	return schemas, nil
}
