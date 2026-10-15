package factory

import (
	"fmt"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
	"github.com/lychee-technology/forma/internal/schemavalidate"
)

// schemaGuards is what buildSchemaGuards produced from one snapshot of the
// registry's documents.
type schemaGuards struct {
	validator  *schemavalidate.Validator
	relations  *internal.RelationIndex
	uniqueKeys *internal.UniqueKeyCatalog
}

// buildSchemaGuards runs both fail-closed schema checks and returns what each
// one produced: the resolved validator, and the relation index. It resolves
// the unique keys (#651) from the same snapshot, so the keys the startup
// check finds built are the keys the manager enforces.
//
// They belong together because they sit at the same position and answer the same
// kind of question about the same documents. Resolving every registered schema
// fails closed so that one which cannot resolve aborts startup rather than
// silently losing validation at runtime (#314); checking relation declarations
// fails closed so that a schema requiring a relation root aborts startup rather
// than making its entity unwritable, since the root is stripped from every
// payload before the validator sees it (#318).
//
// The position is load-bearing, and this function preserves it: neither step
// opens a client, pool, or handle, so both failure returns have nothing to leak.
// Queries have already run (collectTables, LoadMetadata) — this is
// pre-read-surface, not pre-I/O. Never move a step that opens a client, pool, or
// handle above the call site.
//
// Both read the registry, not SCHEMA_DIR, so they analyse the same documents.
// Handing the directory to either would let a registry that does not serve the
// files on disk boot with an index built from one document and a validator built
// from another.
//
// Same accessors is not enough on its own, which is why they are built from a
// snapshot rather than from the registry directly. forma.SchemaRegistry promises
// nothing about repeated reads, so two independent reads can answer two
// different documents, and this pair is where that costs the most: a validator
// demanding a relation root beside an index that strips it boots cleanly and
// makes the entity unwritable (#318 review, and see SnapshotSchemaDocuments).
// One capture, both consumers.
//
// The snapshot covers the registry's *documents*. schemavalidate.New also
// resolves cross-file "$ref"s against schemaDir by reading sibling files, and
// nothing here freezes the file system — so this is one consistent view of the
// registry, not of everything the validator is built from.
//
// The snapshot deliberately stops here rather than becoming the manager's
// registry. The disagreement being closed is between two startup consumers of
// the same documents; the manager reads the registry for something else
// entirely (attribute caches, on every write) and may be given an
// implementation that reloads. Handing it a snapshot would freeze that for the
// process lifetime to fix a startup-time problem, so NewEntityManager keeps the
// caller's registry.
//
// The index is returned rather than discarded. LoadRelationIndex does the
// validating, so building it here and passing it to the manager is what makes
// "the manager strips with the declarations the guard approved" true rather than
// merely likely — see internal.WithRelationIndex.
func buildSchemaGuards(registry forma.SchemaRegistry, schemaDir string) (schemaGuards, error) {
	documents := internal.SnapshotSchemaDocuments(registry)

	schemaValidator, err := schemavalidate.New(documents, schemaDir)
	if err != nil {
		return schemaGuards{}, fmt.Errorf("failed to build schema validator: %w", err)
	}

	relationIndex, err := internal.LoadRelationIndex(documents)
	if err != nil {
		return schemaGuards{}, fmt.Errorf("failed to validate schema relations: %w", err)
	}

	uniqueKeys, err := internal.NewUniqueKeyCatalog(documents)
	if err != nil {
		return schemaGuards{}, fmt.Errorf("failed to resolve unique keys: %w", err)
	}

	return schemaGuards{validator: schemaValidator, relations: relationIndex, uniqueKeys: uniqueKeys}, nil
}

// wrapSchemaGuardError adds the call-site context to a buildSchemaGuards failure:
// the schema directory both guards were built over.
//
// An empty directory gets its own wording rather than being interpolated. It is
// a reachable state, not a defensive branch — Entity.SchemaDirectory is declared
// with no non-empty guard and defaultEntityConfig leaves it unset (forma's
// config.go), and nothing rejects it on the way here. It does not fail on its
// own either: schemavalidate.New starts with filepath.Abs(schemaDir), and
// filepath.Abs("") answers the process working directory (measured), so the
// guards run against wherever the server happens to have been started from.
// Interpolating it would render "over : …", which reads as a truncated path
// rather than as the misconfiguration it usually is.
//
// This only changes how the failure reads. It adds no new startup rejection: an
// unset directory that resolves anyway still boots, exactly as before.
func wrapSchemaGuardError(schemaDir string, err error) error {
	if schemaDir == "" {
		return fmt.Errorf("failed to build the schema guards (Entity.SchemaDirectory is unset): %w", err)
	}
	return fmt.Errorf("failed to build the schema guards over %s: %w", schemaDir, err)
}
