package internal

import (
	"slices"

	"github.com/lychee-technology/forma"
)

// SnapshotSchemaDocuments captures a registry's schema documents once and serves
// every later read of them from that capture.
//
// It exists because forma.SchemaRegistry is a public extension point that
// promises nothing about repeated reads. An implementation serving documents
// from a database, a cache, or the network may answer differently on a second
// call, so two consumers built from independent reads can be handed two
// different documents. The two consumers that matter are the startup pair —
// schemavalidate.New and LoadRelationIndex — and the disagreement they can reach
// is exactly the state the relation guard exists to refuse: a validator that
// demands a relation root paired with an index that strips it, which makes the
// entity unwritable for the process lifetime behind a preflight that passed
// (#318 review). Building both from one snapshot removes the disagreement rather
// than making it unlikely.
//
// What is captured, precisely: one ListSchemas call, and one GetSchemaByName
// call per name it reported. Each answer is stored whole — the error included —
// so a snapshot replays a registry failure exactly as the registry gave it,
// leaving each consumer's own failure handling and message unchanged.
//
// What is NOT captured, and the boundaries are stated rather than implied:
//
//   - the schema *directory*. schemavalidate.New resolves cross-file "$ref"s by
//     reading sibling files off disk (its fileLoader), which nothing here can
//     freeze. So this is one consistent view of the registry's documents, not an
//     atomic view of everything a validator is built from.
//   - a name ListSchemas did not report. Neither startup consumer asks for one —
//     both iterate ListSchemas — so there is nothing captured to answer with, and
//     such a read is passed to the wrapped registry.
//   - the attribute caches. Neither startup consumer reads them; capturing them
//     would add a read per schema at startup and a new startup failure class for
//     data no consumer of this snapshot looks at. Those accessors delegate.
//
// The result is immutable after construction, so it is safe to share and holds
// no lock. It is a startup value: it does not observe a registry that reloads,
// which is why the manager keeps the caller's registry rather than this
// (factory.buildSchemaGuards).
//
// A nil registry yields a nil forma.SchemaRegistry — an untyped nil, not a
// boxed nil pointer — so a caller's own nil check still fires.
func SnapshotSchemaDocuments(registry forma.SchemaRegistry) forma.SchemaRegistry {
	if registry == nil {
		return nil
	}

	names := registry.ListSchemas()
	snapshot := &schemaDocumentSnapshot{
		registry: registry,
		names:    slices.Clone(names),
		byName:   make(map[string]schemaDocumentRead, len(names)),
		byID:     make(map[int16]schemaDocumentRead, len(names)),
	}
	for _, name := range names {
		id, doc, err := registry.GetSchemaByName(name)
		read := schemaDocumentRead{name: name, id: id, doc: doc, err: err}
		snapshot.byName[name] = read
		// A failed read carries no usable id — id is whatever zero value the
		// registry returned alongside the error — so indexing it by id would
		// claim an answer for schema 0.
		if err == nil {
			snapshot.byID[id] = read
		}
	}
	return snapshot
}

// schemaDocumentRead is one captured registry answer, stored whole.
type schemaDocumentRead struct {
	name string
	id   int16
	doc  forma.JSONSchema
	err  error
}

// schemaDocumentSnapshot serves the captured documents and delegates everything
// it did not capture. See SnapshotSchemaDocuments for what falls in each half.
type schemaDocumentSnapshot struct {
	registry forma.SchemaRegistry
	names    []string
	byName   map[string]schemaDocumentRead
	byID     map[int16]schemaDocumentRead
}

// ListSchemas answers the captured list. Cloned per call so a consumer that
// sorts or truncates the slice it gets cannot change what the next one sees.
func (s *schemaDocumentSnapshot) ListSchemas() []string {
	return slices.Clone(s.names)
}

func (s *schemaDocumentSnapshot) GetSchemaByName(name string) (int16, forma.JSONSchema, error) {
	if read, captured := s.byName[name]; captured {
		return read.id, read.doc, read.err
	}
	return s.registry.GetSchemaByName(name)
}

func (s *schemaDocumentSnapshot) GetSchemaByID(id int16) (string, forma.JSONSchema, error) {
	if read, captured := s.byID[id]; captured {
		return read.name, read.doc, read.err
	}
	return s.registry.GetSchemaByID(id)
}

func (s *schemaDocumentSnapshot) GetSchemaAttributeCacheByName(name string) (int16, forma.SchemaAttributeCache, error) {
	return s.registry.GetSchemaAttributeCacheByName(name)
}

func (s *schemaDocumentSnapshot) GetSchemaAttributeCacheByID(id int16) (string, forma.SchemaAttributeCache, error) {
	return s.registry.GetSchemaAttributeCacheByID(id)
}
