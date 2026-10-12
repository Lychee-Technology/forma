package schemameta

import (
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/lychee-technology/forma"
)

// IndexDeclarationsKey is the top-level schema-document extension that
// declares composite indexes on column-bound attributes (#647).
const IndexDeclarationsKey = "x-forma-indexes"

// MaxIndexAttributes bounds a declaration's key. Every declared index also
// carries ltbase_schema_id and ltbase_row_id, so four attributes make a
// six-column key.
const MaxIndexAttributes = 4

// indexNamePattern is the declaration name rule. The name appears in errors
// and init-db output only: the physical index name derives from the column
// tuple, so two schemas sharing a tuple share one index whatever they call it.
var indexNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,30}$`)

// indexDeclarationKeys are the only keys a declaration takes. Anything else —
// "unique", "where" — is refused rather than ignored: those are non-goals
// (#647), and a silently dropped "unique" would leave the author believing the
// index enforces something it does not.
var indexDeclarationKeys = []string{"attributes", "name"}

// parseIndexDeclarations reads a schema document's x-forma-indexes value; nil
// when the key is absent. It enforces every rule the document alone can
// decide: the JSON shape, the name pattern and its uniqueness, 1 to
// MaxIndexAttributes attributes, and no attribute twice. Whether each
// attribute is column-bound needs the attributes file, so the registry checks
// that with validateIndexDeclarations when it loads the schema.
func parseIndexDeclarations(schemaName string, raw any) ([]forma.IndexDeclaration, error) {
	if raw == nil {
		return nil, nil
	}
	entries, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("schema %s: %s must be an array of {\"name\", \"attributes\"} objects, got %s",
			schemaName, IndexDeclarationsKey, jsonKind(raw))
	}

	decls := make([]forma.IndexDeclaration, 0, len(entries))
	seen := make(map[string]bool, len(entries))
	for i, entry := range entries {
		decl, err := parseIndexDeclaration(i, entry)
		if err != nil {
			return nil, fmt.Errorf("schema %s: %w", schemaName, err)
		}
		if seen[decl.Name] {
			return nil, fmt.Errorf("schema %s: index %s (attributes %s) is declared more than once; index names must be unique within a schema",
				schemaName, decl.Name, formatAttributes(decl.Attributes))
		}
		seen[decl.Name] = true
		decls = append(decls, decl)
	}
	return decls, nil
}

// parseIndexDeclaration reads entry i of x-forma-indexes. Its errors name the
// declaration by its name when it has a usable one, else by its position.
func parseIndexDeclaration(i int, entry any) (forma.IndexDeclaration, error) {
	obj, ok := entry.(map[string]any)
	if !ok {
		return forma.IndexDeclaration{}, fmt.Errorf("%s entry %d must be an object with \"name\" and \"attributes\", got %s",
			IndexDeclarationsKey, i, jsonKind(entry))
	}
	label := fmt.Sprintf("%s entry %d", IndexDeclarationsKey, i)
	if name, ok := obj["name"].(string); ok && name != "" {
		label = "index " + name
	}
	attrs, err := parseIndexAttributes(obj["attributes"])
	if err != nil {
		return forma.IndexDeclaration{}, fmt.Errorf("%s: %w", label, err)
	}
	if key := unsupportedDeclarationKey(obj); key != "" {
		return forma.IndexDeclaration{}, fmt.Errorf("%s (attributes %s) has unsupported key %q; a declaration takes only \"name\" and \"attributes\"",
			label, formatAttributes(attrs), key)
	}

	name, isString := obj["name"].(string)
	if obj["name"] == nil || (isString && name == "") {
		return forma.IndexDeclaration{}, fmt.Errorf("%s (attributes %s) has no name; every index needs a name matching %s",
			label, formatAttributes(attrs), indexNamePattern)
	}
	if !isString {
		return forma.IndexDeclaration{}, fmt.Errorf("%s (attributes %s) has a name that is %s, not a string matching %s",
			label, formatAttributes(attrs), jsonKind(obj["name"]), indexNamePattern)
	}
	if !indexNamePattern.MatchString(name) {
		return forma.IndexDeclaration{}, fmt.Errorf("index %q (attributes %s) has an invalid name; it must match %s",
			name, formatAttributes(attrs), indexNamePattern)
	}
	if len(attrs) == 0 || len(attrs) > MaxIndexAttributes {
		return forma.IndexDeclaration{}, fmt.Errorf("index %s lists %d attributes %s; an index takes 1 to %d attributes",
			name, len(attrs), formatAttributes(attrs), MaxIndexAttributes)
	}
	return forma.IndexDeclaration{Name: name, Attributes: attrs}, nil
}

// parseIndexAttributes reads a declaration's attribute list, refusing a
// non-string element and an attribute listed twice.
func parseIndexAttributes(raw any) ([]string, error) {
	list, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("attributes must be an array of attribute names, got %s", jsonKind(raw))
	}
	attrs := make([]string, 0, len(list))
	for j, item := range list {
		attr, ok := item.(string)
		if !ok || attr == "" {
			return nil, fmt.Errorf("attribute %d must be a non-empty attribute name, got %s", j, jsonKind(item))
		}
		if slices.Contains(attrs, attr) {
			return nil, fmt.Errorf("attribute %s is listed more than once", attr)
		}
		attrs = append(attrs, attr)
	}
	return attrs, nil
}

// unsupportedDeclarationKey returns the first key of obj, in sorted order,
// that a declaration does not take; "" when there is none.
func unsupportedDeclarationKey(obj map[string]any) string {
	keys := make([]string, 0, len(obj))
	for key := range obj {
		if !slices.Contains(indexDeclarationKeys, key) {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return ""
	}
	sort.Strings(keys)
	return keys[0]
}

// validateIndexDeclarations checks each declared attribute against the
// schema's FULL attribute cache (retired entries included, so a retired
// attribute is named as such rather than as unknown): it must be an active,
// column-bound attribute of a value type whose filters the index can serve,
// and its column must not be one of the two key columns every declared index
// already carries. Every error names the schema, the index and the attribute;
// callers wrap it with the schema file.
//
// A bool attribute is refused because #647 requires equals and in on an
// indexed attribute to be index conditions, and the entity_main SQL for a bool
// compares the column's truth value rather than the column. bool_text renders
// equals as (<col> = '1') = $n (sqlgen.BoolMainPredicate), which PostgreSQL
// folds into an index condition only for a true it can see at planning time:
// false becomes <col> <> '1', and a cached generic plan keeps the expression.
// in / not_in compare sqlgen.BoolMainTruth under both encodings, which no
// plan can seek (#671 tracks admitting bool_smallint). Every other admitted
// binding compares the bare column (TestDeclaredIndexServesEveryIndexableKind).
func validateIndexDeclarations(schemaName string, decls []forma.IndexDeclaration, cache forma.SchemaAttributeCache) error {
	for _, decl := range decls {
		for _, attr := range decl.Attributes {
			if err := validateIndexedAttribute(attr, cache); err != nil {
				return fmt.Errorf("schema %s: index %s: %w", schemaName, decl.Name, err)
			}
		}
	}
	return nil
}

func validateIndexedAttribute(attr string, cache forma.SchemaAttributeCache) error {
	meta, ok := cache[attr]
	switch {
	case !ok:
		return fmt.Errorf("attribute %s is not defined in the schema's attributes file; only column-bound attributes can be indexed", attr)
	case meta.Retired:
		return fmt.Errorf("attribute %s is retired; only active column-bound attributes can be indexed", attr)
	case meta.ColumnBinding == nil:
		return fmt.Errorf("attribute %s is stored in EAV (it has no column_binding); only column-bound attributes can be indexed", attr)
	}
	switch col := meta.ColumnBinding.ColumnName; col {
	case forma.MainColumnSchemaID, forma.MainColumnRowID:
		return fmt.Errorf("attribute %s is bound to %s, which every declared index already carries as a key column", attr, col)
	}
	if meta.ValueType == forma.ValueTypeBool {
		return fmt.Errorf("attribute %s is a bool bound to %s (encoding %s); bool filters compare the column's truth value rather than the column (in and not_in under every encoding, equals under bool_text), which an index on the column cannot reliably serve, so bool attributes cannot be indexed",
			attr, meta.ColumnBinding.ColumnName, meta.ColumnBinding.Encoding)
	}
	return nil
}

// formatAttributes renders a declaration's attribute list for an error.
func formatAttributes(attrs []string) string {
	return "[" + strings.Join(attrs, ", ") + "]"
}

// jsonKind names the JSON type of a value decoded by encoding/json.
func jsonKind(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case map[string]any:
		return "an object"
	case []any:
		return "an array"
	case string:
		return "a string"
	case float64:
		return "a number"
	case bool:
		return "a boolean"
	default:
		return fmt.Sprintf("%T", v)
	}
}
