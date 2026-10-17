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
	// hash is uniquekey.AttributesHash of attrs: what the key's values mean.
	// storage is uniquekey.StorageHash of attrs: where the write path reads
	// them. The state table records a key as built for both.
	hash    string
	storage string
}

type uniqueKeyAttribute struct {
	name string
	meta forma.AttributeMetadata
	// stored is where the write path reads the attribute's value.
	stored uniquekey.Storage
}

// storageOf answers where the write path reads the value meta describes: its
// bound column and encoding, an unnamed encoding spelled as the default it
// reads as, or its EAV attribute ID. Two attributes read from one place have
// equal storage.
func storageOf(meta forma.AttributeMetadata) uniquekey.Storage {
	binding := meta.ColumnBinding
	if binding == nil {
		return uniquekey.Storage{AttributeID: meta.AttributeID}
	}
	encoding := binding.Encoding
	if encoding == "" {
		encoding = forma.MainColumnEncodingDefault
	}
	return uniquekey.Storage{Column: binding.ColumnName, Encoding: encoding}
}

// UniqueKeyCatalog holds every schema's unique keys, resolved once from the
// registry. It is immutable: a repository is given one when it is built
// (WithUniqueKeyCatalog) and reads it on every write without synchronization.
// A nil catalog declares nothing.
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
		if err := refuseRelationSubtreeKeys(registry, schemaName, schema.UniqueKeys); err != nil {
			return nil, err
		}
		specs, err := resolveUniqueKeys(schemaName, schema.UniqueKeys, cache)
		if err != nil {
			return nil, fmt.Errorf("resolve the unique keys of schema %s: %w", schemaName, err)
		}
		catalog.bySchema[schemaID] = specs
	}
	return catalog, nil
}

// refuseRelationSubtreeKeys refuses a key over a relation root or anything
// beneath it. Every write strips that subtree (StripComputedFields) and every
// read derives it from the parent, so the write path never sees a value to
// reserve while reads answer one: the key would enforce nothing.
func refuseRelationSubtreeKeys(registry forma.SchemaRegistry, schemaName string, decls []forma.UniqueKeyDeclaration) error {
	relations := &RelationIndex{bySchema: map[string][]RelationDescriptor{}}
	if err := relations.loadSchemaRelations(registry, schemaName); err != nil {
		return fmt.Errorf("load the relations of schema %s for its unique keys: %w", schemaName, err)
	}
	for _, decl := range decls {
		for _, attr := range decl.Attributes {
			if root, covered := relationRootCovering(relations, schemaName, attr); covered {
				return fmt.Errorf("schema %s: unique key %s: attribute %s lies under relation root %s, which writes never store; "+
					"a unique key needs attributes the writes store", schemaName, decl.Name, attr, root)
			}
		}
	}
	return nil
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
		stored := make([]uniquekey.Storage, 0, len(decl.Attributes))
		for _, attr := range decl.Attributes {
			meta := cache[attr] // ValidateUniqueKeyDeclarations found it
			if meta.ColumnBinding != nil {
				// Copied, as the catalog is immutable and the binding is the
				// registry's.
				binding := *meta.ColumnBinding
				meta.ColumnBinding = &binding
			}
			spec.attrs = append(spec.attrs, uniqueKeyAttribute{name: attr, meta: meta, stored: storageOf(meta)})
			hashed = append(hashed, uniquekey.Attribute{Name: attr, ValueType: meta.ValueType})
			stored = append(stored, storageOf(meta))
		}
		spec.hash = uniquekey.AttributesHash(hashed)
		spec.storage = uniquekey.StorageHash(stored)
		specs = append(specs, spec)
	}
	return specs, nil
}

// checkUniqueKeys refuses a repository that does not enforce the unique keys
// of registry's schemas (#651): the catalog WithUniqueKeys supplied, or one
// resolved here. The PostgreSQL repository enforces the catalog it was built
// with, which nothing replaces afterwards, so a manager only reads it: two
// managers may share a repository when their registries resolve the same
// keys over the same storage, and any other manager is refused before it can
// write. Another repository enforces no keys, so a registry that declares one
// refuses it rather than leave the key silently unenforced.
func (em *entityManager) checkUniqueKeys(repository model.PersistentRecordRepository, registry forma.SchemaRegistry) error {
	if em.uniqueKeys == nil {
		catalog, err := NewUniqueKeyCatalog(registry)
		if err != nil {
			return fmt.Errorf("failed to resolve the schemas' unique keys: %w", err)
		}
		em.uniqueKeys = catalog
	}
	if repo, ok := repository.(*DBPersistentRecordRepository); ok {
		if difference := repo.uniqueKeys.differenceFrom(em.uniqueKeys); difference != "" {
			return fmt.Errorf("the repository enforces other unique keys than the registry declares (%s); "+
				"build the repository with WithUniqueKeyCatalog and the registry's keys", difference)
		}
		return nil
	}
	if keyed := em.uniqueKeys.keyedSchemas(); len(keyed) > 0 {
		return fmt.Errorf("schema %s declares unique keys, which only the PostgreSQL repository enforces; this manager's repository is %T",
			em.uniqueKeys.schemaName(keyed[0]), repository)
	}
	return nil
}

// differenceFrom names the first way the keys c enforces, as a repository's,
// differ from the keys other declares, as a registry's; "" when they are the
// same keys, read from the same storage. Two catalogs whose declarations
// agree but whose attributes are stored apart are different: each would
// compute its keys from columns the other's writes do not fill.
func (c *UniqueKeyCatalog) differenceFrom(other *UniqueKeyCatalog) string {
	if c == other {
		return ""
	}
	schemaIDs := append(c.keyedSchemas(), other.keyedSchemas()...)
	slices.Sort(schemaIDs)
	for _, schemaID := range slices.Compact(schemaIDs) {
		ours, theirs := c.schemaName(schemaID), other.schemaName(schemaID)
		name := theirs
		if len(other.keysOf(schemaID)) == 0 {
			name = ours
		}
		if difference := keysDifference(name, c.keysOf(schemaID), other.keysOf(schemaID)); difference != "" {
			return difference
		}
		if ours != theirs {
			return fmt.Sprintf("schema id %d is %s to the repository and %s to the registry", schemaID, ours, theirs)
		}
	}
	return ""
}

// keysDifference names the first difference between a schema's keys as the
// repository enforces them (enforced) and as the registry declares them.
func keysDifference(schema string, enforced, declared []uniqueKeySpec) string {
	for i := range max(len(enforced), len(declared)) {
		switch {
		case i >= len(enforced):
			return fmt.Sprintf("schema %s: the repository does not enforce unique key %s", schema, declared[i].name)
		case i >= len(declared):
			return fmt.Sprintf("schema %s: the repository enforces unique key %s, which the registry does not declare", schema, enforced[i].name)
		}
		a, b := enforced[i], declared[i]
		if a.id != b.id || a.name != b.name || a.hash != b.hash {
			return fmt.Sprintf("schema %s: the repository enforces unique key %s (id %d), where the registry declares unique key %s (id %d) over other attributes or types",
				schema, a.name, a.id, b.name, b.id)
		}
		for j := range a.attrs {
			if a.attrs[j].stored != b.attrs[j].stored {
				return fmt.Sprintf("schema %s: unique key %s reads attribute %s from %s, where the registry stores it in %s",
					schema, a.name, a.attrs[j].name, a.attrs[j].stored, b.attrs[j].stored)
			}
		}
	}
	return ""
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
