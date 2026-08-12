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

// GetSchemaByName answers the read captured for that name — document, id and
// error alike — and delegates any other name to the wrapped registry, which is
// not a failure here: nothing was captured for a name ListSchemas never
// reported, so there is nothing to replay.
//
// The captured error is returned exactly as captured: not wrapped, not reworded.
// That is a deliberate exception to this repository's rule that an error gains
// context as it crosses a boundary, and the exception is what makes the proxy a
// proxy. A wrap would put this type into a message an operator reads as the
// registry's own, and it would hand the caller an error one level removed from
// the one the registry produced. Identity is being preserved on purpose: a
// consumer matching the registry's own error with errors.Is still matches
// through the snapshot — pinned by
// TestSnapshotSchemaDocumentsReplaysTheRegistrysFailure, which asserts ErrorIs
// against the registry's cause on every replayed read.
//
// The delegating branch adds nothing for the same reason: it is the registry's
// own call made on the caller's behalf, and each consumer wraps what it gets
// with its own context on the way out (schemavalidate.New, LoadRelationIndex).
func (s *schemaDocumentSnapshot) GetSchemaByName(name string) (int16, forma.JSONSchema, error) {
	if read, captured := s.byName[name]; captured {
		return read.id, read.doc, read.err
	}
	return s.registry.GetSchemaByName(name)
}

// GetSchemaByID answers the same captured reads keyed the other way, and
// delegates an id that was never captured — which includes every id whose read
// failed, since a failed read carries no usable id (SnapshotSchemaDocuments).
// Errors are replayed verbatim, for the reasons given on GetSchemaByName.
func (s *schemaDocumentSnapshot) GetSchemaByID(id int16) (string, forma.JSONSchema, error) {
	if read, captured := s.byID[id]; captured {
		return read.name, read.doc, read.err
	}
	return s.registry.GetSchemaByID(id)
}

// GetSchemaAttributeCacheByName answers nothing of its own: attribute caches are
// not captured, so every call is the wrapped registry's, returned as it came.
// SnapshotSchemaDocuments states why they are left out — neither startup
// consumer of this snapshot reads them.
func (s *schemaDocumentSnapshot) GetSchemaAttributeCacheByName(name string) (int16, forma.SchemaAttributeCache, error) {
	return s.registry.GetSchemaAttributeCacheByName(name)
}

// GetSchemaAttributeCacheByID is the id-keyed half of the same delegation, with
// the same reasons.
func (s *schemaDocumentSnapshot) GetSchemaAttributeCacheByID(id int16) (string, forma.SchemaAttributeCache, error) {
	return s.registry.GetSchemaAttributeCacheByID(id)
}
