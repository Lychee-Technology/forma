package coordinationtest

import (
	"context"
	"fmt"

	"github.com/lychee-technology/forma"
)

// Definition statuses.
const (
	DefinitionDraft  = "draft"
	DefinitionActive = "active"
)

// DefinitionStore manages workflow definitions. LTFlow keeps its
// DefinitionStore outside the unit of work, each method in a transaction of
// its own; here it is a store of the unit of work, so a test can write
// definitions beside other records. LTFlow: postgres.DefinitionStore.
type DefinitionStore interface {
	SaveDraft(ctx context.Context, def *Definition) error
	SaveActive(ctx context.Context, def *Definition) (int64, error)
	GetByVersion(ctx context.Context, name string, version int64) (*Definition, error)
}

type definitionStore struct {
	tx forma.Tx
}

// SaveDraft saves def as a draft with Tx.Create. The unique key
// name_version (#651) makes a second definition of a name and version
// ErrUniqueConflict. LTFlow: DefinitionStore.SaveDraft.
func (s *definitionStore) SaveDraft(ctx context.Context, def *Definition) error {
	def.Status, def.ActivationSequence = DefinitionDraft, 0
	return s.save(ctx, def)
}

// SaveActive saves def as active under the name's next activation sequence,
// read with Tx.Query, and answers that sequence. The unique keys
// name_version and name_activation (#651) make a duplicate version, or a
// concurrent activation that read the same sequence, ErrUniqueConflict,
// where LTFlow serializes activations with an advisory lock. LTFlow:
// DefinitionStore.SaveActive.
func (s *definitionStore) SaveActive(ctx context.Context, def *Definition) (int64, error) {
	latest, err := queryAll(ctx, s.tx, DefinitionSchema,
		all(equals("name", def.Name), forma.IsNotNull("activation_sequence")),
		forma.OrderBy{Attribute: "activation_sequence", SortOrder: forma.SortOrderDesc})
	if err != nil {
		return 0, fmt.Errorf("read the latest activation of %s: %w", def.Name, err)
	}
	def.Status, def.ActivationSequence = DefinitionActive, 1
	if len(latest) > 0 {
		def.ActivationSequence = whole(latest[0], "activation_sequence") + 1
	}
	if err := s.save(ctx, def); err != nil {
		return 0, fmt.Errorf("activate definition %s as sequence %d: %w", def.Name, def.ActivationSequence, err)
	}
	return def.ActivationSequence, nil
}

// GetByVersion reads the definition of name and version with Tx.Query
// (#650); none is ErrNotFound. LTFlow: DefinitionStore.GetByVersion.
func (s *definitionStore) GetByVersion(ctx context.Context, name string, version int64) (*Definition, error) {
	records, err := queryAll(ctx, s.tx, DefinitionSchema, all(equals("name", name), equals("version", version)))
	if err != nil {
		return nil, fmt.Errorf("get definition %s version %d: %w", name, version, err)
	}
	if len(records) == 0 {
		return nil, forma.NotFoundf("no definition %s has version %d", name, version)
	}
	r := records[0]
	return &Definition{ID: r.RowID, Name: text(r, "name"), Version: whole(r, "version"),
		ActivationSequence: whole(r, "activation_sequence"), Status: text(r, "status"), Body: text(r, "body")}, nil
}

func (s *definitionStore) save(ctx context.Context, def *Definition) error {
	data := map[string]any{"name": def.Name, "version": def.Version, "status": def.Status, "body": def.Body}
	if def.ActivationSequence != 0 {
		data["activation_sequence"] = def.ActivationSequence
	}
	record, err := create(ctx, s.tx, DefinitionSchema, def.ID, data)
	if err != nil {
		return fmt.Errorf("save definition %s version %d: %w", def.Name, def.Version, err)
	}
	def.ID = record.RowID
	return nil
}
