package internal

import (
	"fmt"
	"slices"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/transform"
	"github.com/lychee-technology/forma/internal/uniquekey"
)

// uniqueKeySpec is one declared unique key (#651) as the write path enforces
// it: the declaration with each attribute's metadata resolved.
type uniqueKeySpec struct {
	id    int16
	name  string
	attrs []uniqueKeyAttribute
	// hash is uniquekey.AttributesHash of attrs: the state table records a
	// key as built for it.
	hash string
}

type uniqueKeyAttribute struct {
	name string
	meta forma.AttributeMetadata
}

// UniqueKeyCatalog holds every schema's unique keys, resolved once from the
// registry, which is immutable after construction. A nil catalog declares
// nothing.
type UniqueKeyCatalog struct {
	bySchema map[int16][]uniqueKeySpec
	// names holds every schema's name, keyed or not, for the conflict
	// errors the write path publishes.
	names map[int16]string
}

// NewUniqueKeyCatalog resolves the unique keys every schema of registry
// declares. The file registry validated them when it loaded the schema; this
// checks them again, against the active attributes, because a caller's own
// registry may not have, and a key over an attribute the write path cannot
// read would enforce nothing.
func NewUniqueKeyCatalog(registry forma.SchemaRegistry) (*UniqueKeyCatalog, error) {
	catalog := &UniqueKeyCatalog{bySchema: map[int16][]uniqueKeySpec{}, names: map[int16]string{}}
	if registry == nil {
		return catalog, nil
	}
	for _, schemaName := range registry.ListSchemas() {
		schemaID, schema, err := registry.GetSchemaByName(schemaName)
		if err != nil {
			return nil, fmt.Errorf("load schema %s for its unique keys: %w", schemaName, err)
		}
		catalog.names[schemaID] = schemaName
		if len(schema.UniqueKeys) == 0 {
			continue
		}
		_, cache, err := registry.GetSchemaAttributeCacheByName(schemaName)
		if err != nil {
			return nil, fmt.Errorf("load attributes of schema %s for its unique keys: %w", schemaName, err)
		}
		if err := schemameta.ValidateUniqueKeyDeclarations(schemaName, schema.UniqueKeys, cache, schema.Properties); err != nil {
			return nil, fmt.Errorf("validate the unique keys of schema %s: %w", schemaName, err)
		}
		specs, err := resolveUniqueKeys(schemaName, schema.UniqueKeys, cache)
		if err != nil {
			return nil, fmt.Errorf("resolve the unique keys of schema %s: %w", schemaName, err)
		}
		catalog.bySchema[schemaID] = specs
	}
	return catalog, nil
}

func resolveUniqueKeys(schemaName string, decls []forma.UniqueKeyDeclaration, cache forma.SchemaAttributeCache) ([]uniqueKeySpec, error) {
	specs := make([]uniqueKeySpec, 0, len(decls))
	for _, decl := range decls {
		if decl.ID < 1 || len(decl.Attributes) == 0 || len(decl.Attributes) > forma.MaxUniqueKeyAttributes {
			return nil, fmt.Errorf("schema %s: unique key %s needs an id from 1 to 32767 and 1 to %d attributes, got id %d and %d attributes",
				schemaName, decl.Name, forma.MaxUniqueKeyAttributes, decl.ID, len(decl.Attributes))
		}
		spec := uniqueKeySpec{id: decl.ID, name: decl.Name}
		hashed := make([]uniquekey.Attribute, 0, len(decl.Attributes))
		for _, attr := range decl.Attributes {
			meta := cache[attr] // ValidateUniqueKeyDeclarations found it
			spec.attrs = append(spec.attrs, uniqueKeyAttribute{name: attr, meta: meta})
			hashed = append(hashed, uniquekey.Attribute{Name: attr, ValueType: meta.ValueType})
		}
		spec.hash = uniquekey.AttributesHash(hashed)
		specs = append(specs, spec)
	}
	return specs, nil
}

// installUniqueKeys hands the PostgreSQL repository the unique keys of
// registry's schemas (#651), which its write cores then enforce on every
// surface the manager and its Transactor offer: the catalog WithUniqueKeys
// supplied, or one resolved here. Another repository cannot enforce them, so
// a registry that declares a key refuses it rather than leave the key
// silently unenforced. The keys live on the repository, so two managers can
// share one only when they enforce the same keys.
func (em *entityManager) installUniqueKeys(repository model.PersistentRecordRepository, registry forma.SchemaRegistry) error {
	if em.uniqueKeys == nil {
		catalog, err := NewUniqueKeyCatalog(registry)
		if err != nil {
			return fmt.Errorf("failed to resolve the schemas' unique keys: %w", err)
		}
		em.uniqueKeys = catalog
	}
	if repo, ok := repository.(*DBPersistentRecordRepository); ok {
		if repo.uniqueKeys != nil && !repo.uniqueKeys.sameKeys(em.uniqueKeys) {
			return fmt.Errorf("the repository already enforces another registry's unique keys; give each manager its own repository")
		}
		repo.uniqueKeys = em.uniqueKeys
		return nil
	}
	if keyed := em.uniqueKeys.keyedSchemas(); len(keyed) > 0 {
		return fmt.Errorf("schema %s declares unique keys, which only the PostgreSQL repository enforces; this manager's repository is %T",
			em.uniqueKeys.schemaName(keyed[0]), repository)
	}
	return nil
}

// sameKeys says whether c and other enforce the same keys: the same ids,
// names and attribute lists in every schema.
func (c *UniqueKeyCatalog) sameKeys(other *UniqueKeyCatalog) bool {
	if len(c.bySchema) != len(other.bySchema) {
		return false
	}
	for schemaID, specs := range c.bySchema {
		if !slices.EqualFunc(specs, other.bySchema[schemaID], func(a, b uniqueKeySpec) bool {
			return a.id == b.id && a.name == b.name && a.hash == b.hash
		}) {
			return false
		}
	}
	return true
}

// keysOf answers schemaID's unique keys; nil when it declares none.
func (c *UniqueKeyCatalog) keysOf(schemaID int16) []uniqueKeySpec {
	if c == nil {
		return nil
	}
	return c.bySchema[schemaID]
}

// keyNamed answers schemaID's key called name.
func (c *UniqueKeyCatalog) keyNamed(schemaID int16, name string) (uniqueKeySpec, bool) {
	i := slices.IndexFunc(c.keysOf(schemaID), func(s uniqueKeySpec) bool { return s.name == name })
	if i < 0 {
		return uniqueKeySpec{}, false
	}
	return c.bySchema[schemaID][i], true
}

// schemaName names schemaID for an error.
func (c *UniqueKeyCatalog) schemaName(schemaID int16) string {
	if c != nil {
		if name, ok := c.names[schemaID]; ok {
			return name
		}
	}
	return fmt.Sprintf("with id %d", schemaID)
}

// keyedSchemas answers the IDs of the schemas that declare a key, ascending.
func (c *UniqueKeyCatalog) keyedSchemas() []int16 {
	if c == nil {
		return nil
	}
	ids := make([]int16, 0, len(c.bySchema))
	for id := range c.bySchema {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

// uniqueKeyEntry is one key-table entry: the value of one key on one row.
type uniqueKeyEntry struct {
	keyID   int16
	keyName string
	value   string
}

// uniqueKeyEntriesOf answers the entries record holds once written: one per
// key whose attributes are all present, in declaration order. A record with
// a deleted-at stamp is not live and holds none.
func uniqueKeyEntriesOf(specs []uniqueKeySpec, record *model.PersistentRecord) ([]uniqueKeyEntry, error) {
	if len(specs) == 0 || record.DeletedAt != nil {
		return nil, nil
	}
	entries := make([]uniqueKeyEntry, 0, len(specs))
	for _, spec := range specs {
		value, present, err := uniqueKeyValueOf(spec, record)
		if err != nil {
			return nil, fmt.Errorf("compute unique key %s of row %s: %w", spec.name, record.RowID, err)
		}
		if present {
			entries = append(entries, uniqueKeyEntry{keyID: spec.id, keyName: spec.name, value: value})
		}
	}
	return entries, nil
}

// uniqueKeyValueOf answers spec's value on record; present is false when any
// of its attributes is absent or null, so the row is not subject to it.
func uniqueKeyValueOf(spec uniqueKeySpec, record *model.PersistentRecord) (string, bool, error) {
	components := make([]uniquekey.Component, 0, len(spec.attrs))
	for _, attr := range spec.attrs {
		v, err := transform.StoredAttributeValue(record, attr.meta)
		if err != nil {
			return "", false, fmt.Errorf("read attribute %s: %w", attr.name, err)
		}
		if v == nil {
			return "", false, nil
		}
		components = append(components, uniquekey.Component{Type: attr.meta.ValueType, Value: v})
	}
	value, err := uniquekey.Encode(components)
	if err != nil {
		return "", false, fmt.Errorf("encode: %w", err)
	}
	return value, true, nil
}
