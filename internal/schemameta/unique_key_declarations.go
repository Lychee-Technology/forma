package schemameta

import (
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"

	"github.com/lychee-technology/forma"
)

// UniqueKeyDeclarationsKey is the top-level schema-document extension that
// declares unique keys (#651).
const UniqueKeyDeclarationsKey = "x-forma-unique-keys"

// uniqueKeyDeclarationKeys are the only keys a declaration takes; anything
// else is refused rather than ignored, as for x-forma-indexes.
var uniqueKeyDeclarationKeys = []string{"attributes", "id", "name"}

// parseUniqueKeyDeclarations reads the x-forma-unique-keys value of a schema
// document; nil when the key is absent. A present key must hold an array. It
// enforces every rule the document alone decides: the JSON shape, an id from
// 1 to 32767, the name pattern, 1 to forma.MaxUniqueKeyAttributes distinct
// attributes, and no id or name twice. Whether each attribute is a scalar of
// the schema needs the attributes file, so the registry checks that with
// validateUniqueKeyDeclarations when it loads the schema.
func parseUniqueKeyDeclarations(schemaName string, doc map[string]any) ([]forma.UniqueKeyDeclaration, error) {
	raw, present := doc[UniqueKeyDeclarationsKey]
	if !present {
		return nil, nil
	}
	entries, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("schema %s: %s must be an array of {\"id\", \"name\", \"attributes\"} objects, got %s",
			schemaName, UniqueKeyDeclarationsKey, jsonKind(raw))
	}

	decls := make([]forma.UniqueKeyDeclaration, 0, len(entries))
	names := make(map[string]bool, len(entries))
	ids := make(map[int16]string, len(entries))
	for i, entry := range entries {
		decl, err := parseUniqueKeyDeclaration(i, entry)
		if err != nil {
			return nil, fmt.Errorf("schema %s: %w", schemaName, err)
		}
		if names[decl.Name] {
			return nil, fmt.Errorf("schema %s: unique key %s (attributes %s) is declared more than once; unique key names must be unique within a schema",
				schemaName, decl.Name, formatAttributes(decl.Attributes))
		}
		if other, taken := ids[decl.ID]; taken {
			return nil, fmt.Errorf("schema %s: unique key %s (attributes %s) has id %d, which unique key %s already has; ids must be unique within a schema",
				schemaName, decl.Name, formatAttributes(decl.Attributes), decl.ID, other)
		}
		names[decl.Name], ids[decl.ID] = true, decl.Name
		decls = append(decls, decl)
	}
	return decls, nil
}

// parseUniqueKeyDeclaration reads entry i of x-forma-unique-keys. Its errors
// name the declaration by its name when it has a usable one, else by its
// position.
func parseUniqueKeyDeclaration(i int, entry any) (forma.UniqueKeyDeclaration, error) {
	obj, ok := entry.(map[string]any)
	if !ok {
		return forma.UniqueKeyDeclaration{}, fmt.Errorf("%s entry %d must be an object with \"id\", \"name\" and \"attributes\", got %s",
			UniqueKeyDeclarationsKey, i, jsonKind(entry))
	}
	label := fmt.Sprintf("%s entry %d", UniqueKeyDeclarationsKey, i)
	name, isString := obj["name"].(string)
	if isString && name != "" {
		label = "unique key " + name
	}
	attrs, err := parseIndexAttributes(obj["attributes"])
	if err != nil {
		return forma.UniqueKeyDeclaration{}, fmt.Errorf("%s: %w", label, err)
	}
	if key := unsupportedKey(obj, uniqueKeyDeclarationKeys); key != "" {
		return forma.UniqueKeyDeclaration{}, fmt.Errorf("%s (attributes %s) has unsupported key %q; a declaration takes only \"id\", \"name\" and \"attributes\"",
			label, formatAttributes(attrs), key)
	}
	if !isString || !indexNamePattern.MatchString(name) {
		return forma.UniqueKeyDeclaration{}, fmt.Errorf("%s (attributes %s) needs a name matching %s, got %s",
			label, formatAttributes(attrs), indexNamePattern, describeName(obj["name"]))
	}
	id, err := parseUniqueKeyID(obj["id"])
	if err != nil {
		return forma.UniqueKeyDeclaration{}, fmt.Errorf("unique key %s (attributes %s): %w", name, formatAttributes(attrs), err)
	}
	if len(attrs) == 0 || len(attrs) > forma.MaxUniqueKeyAttributes {
		return forma.UniqueKeyDeclaration{}, fmt.Errorf("unique key %s lists %d attributes %s; a unique key takes 1 to %d attributes",
			name, len(attrs), formatAttributes(attrs), forma.MaxUniqueKeyAttributes)
	}
	return forma.UniqueKeyDeclaration{ID: id, Name: name, Attributes: attrs}, nil
}

// parseUniqueKeyID reads a declaration's id: a JSON integer from 1 to
// math.MaxInt16.
func parseUniqueKeyID(raw any) (int16, error) {
	n, ok := raw.(float64)
	if !ok {
		return 0, fmt.Errorf("id must be an integer from 1 to %d, got %s", math.MaxInt16, jsonKind(raw))
	}
	if n != math.Trunc(n) || n < 1 || n > math.MaxInt16 {
		return 0, fmt.Errorf("id must be an integer from 1 to %d, got %v", math.MaxInt16, n)
	}
	return int16(n), nil
}

func describeName(raw any) string {
	if s, ok := raw.(string); ok {
		return fmt.Sprintf("%q", s)
	}
	return jsonKind(raw)
}

// unsupportedKey returns the first key of obj, in sorted order, that is not
// in allowed; "" when there is none.
func unsupportedKey(obj map[string]any, allowed []string) string {
	keys := make([]string, 0, len(obj))
	for key := range obj {
		if !slices.Contains(allowed, key) {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return ""
	}
	sort.Strings(keys)
	return keys[0]
}

// ValidateUniqueKeyDeclarations checks each key attribute against the
// schema's attribute cache and its parsed properties: it must be an active
// scalar attribute, not a list and not inside an array, since a key compares
// one value per row. The file registry passes its FULL cache, retired entries
// included, so a retired attribute is named as such. Every error names the
// schema, the key and the attribute; callers wrap it with the schema file.
func ValidateUniqueKeyDeclarations(
	schemaName string, decls []forma.UniqueKeyDeclaration, cache forma.SchemaAttributeCache, properties map[string]*forma.PropertySchema,
) error {
	for _, decl := range decls {
		for _, attr := range decl.Attributes {
			if err := validateUniqueKeyAttribute(attr, cache, properties); err != nil {
				return fmt.Errorf("schema %s: unique key %s: %w", schemaName, decl.Name, err)
			}
		}
	}
	return nil
}

func validateUniqueKeyAttribute(attr string, cache forma.SchemaAttributeCache, properties map[string]*forma.PropertySchema) error {
	meta, ok := cache[attr]
	switch {
	case !ok:
		return fmt.Errorf("attribute %s is not defined in the schema's attributes file", attr)
	case meta.Retired:
		return fmt.Errorf("attribute %s is retired; a unique key needs active attributes", attr)
	case meta.ValueType == forma.ValueTypeList:
		return fmt.Errorf("attribute %s is a list; a unique key needs scalar attributes", attr)
	case insideArray(attr, properties):
		return fmt.Errorf("attribute %s is inside an array; a unique key needs one value per row", attr)
	case meta.ColumnBinding != nil && slices.Contains(serverMaintainedColumns, meta.ColumnBinding.ColumnName):
		return fmt.Errorf("attribute %s is bound to %s, which Forma rewrites on later writes; a unique key needs attributes the writes set",
			attr, meta.ColumnBinding.ColumnName)
	}
	return nil
}

// serverMaintainedColumns are the system columns Forma stamps on an update
// or a delete, whatever the write sets: a key over one would change without
// its entry moving. ltbase_created_at and ltbase_created_by are set once, at
// create, and stay admissible.
var serverMaintainedColumns = []forma.MainColumn{
	forma.MainColumnUpdatedAt, forma.MainColumnUpdatedBy, forma.MainColumnDeletedAt, forma.MainColumnDeletedBy,
}

// insideArray says whether the document places attr under an array. The
// attributes file names an attribute inside an array of objects by its
// dotted path alone, so only the document tells. A path the document does
// not resolve, such as one through another file's $ref, is not refused here;
// the write path refuses a key value that comes from array elements.
func insideArray(attr string, properties map[string]*forma.PropertySchema) bool {
	segments := strings.Split(attr, ".")
	for _, segment := range segments[:len(segments)-1] {
		prop := properties[segment]
		if prop == nil {
			return false
		}
		if prop.Type == "array" || prop.Items != nil {
			return true
		}
		properties = prop.Properties
	}
	return false
}
