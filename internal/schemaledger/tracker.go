package schemaledger

import (
	"context"
	"errors"
	"fmt"
)

// ErrGenerationChanged reports a schema whose active generation moved while
// a process ran on the one it started with.
var ErrGenerationChanged = errors.New("the active generation changed during the run")

// Tracker is implemented by a schema registry the gate admitted: it knows
// the generation each schema started on and can read the active one again.
type Tracker interface {
	// LoadedGeneration answers the generation schemaID started on.
	LoadedGeneration(schemaID int16) (int64, bool)
	// ActiveGeneration reads schemaID's active generation again.
	ActiveGeneration(ctx context.Context, schemaID int16) (int64, bool, error)
}

// RequireUnchanged answers ErrGenerationChanged when schemaID's active
// generation is no longer the one registry started on. cdc-flush and cdc-init
// call it after a batch's export and before the batch leaves _tmp: the export
// wrote stored values through the run's own declared types, and a generation
// recorded meanwhile can admit values those types cannot hold (1001.5 exports
// as SMALLINT 1002 under smallint after smallint to numeric). A registry that
// is not a Tracker (directory mode, or a caller's own) has no ledger, so
// nothing is checked.
func RequireUnchanged(ctx context.Context, registry any, schemaID int16) error {
	tracker, ok := registry.(Tracker)
	if !ok {
		return nil
	}
	loaded, ok := tracker.LoadedGeneration(schemaID)
	if !ok {
		return fmt.Errorf("schema id %d was not admitted by the generation gate at start", schemaID)
	}
	current, ok, err := tracker.ActiveGeneration(ctx, schemaID)
	if err != nil {
		return fmt.Errorf("failed to re-read the active generation of schema id %d: %w", schemaID, err)
	}
	if !ok {
		return fmt.Errorf("schema id %d started on generation %d but now has no active generation: %w",
			schemaID, loaded, ErrGenerationChanged)
	}
	if current != loaded {
		return fmt.Errorf("schema id %d started on generation %d but generation %d is now active; "+
			"restart on the new declaration: %w", schemaID, loaded, current, ErrGenerationChanged)
	}
	return nil
}
