package federatedv2

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma/internal/publication"
)

// LoadEpochPins reads, on tx, the epoch of each of schemaIDs' publication
// records: the process's epoch pins (Config.Epochs), read when it loads the
// schemas' metadata. The caller opens tx as one REPEATABLE READ snapshot, so
// the pins agree with each other.
//
// A schema without a record gets no pin, and every read of it fails closed
// (ErrNotPinned) until a process that loads its metadata again finds one: a
// record created later is not adopted, since the metadata this process holds
// may describe a schema the record no longer belongs to. A row the read
// refused as invalid is pinned at the epoch it holds: reads of it fail closed
// on the row itself (publication.ErrRecordInvalid). Any other failure, a
// missing publication table among them, is the caller's to refuse startup
// with.
func LoadEpochPins(ctx context.Context, tx pgx.Tx, entityMain string, schemaIDs []int16) (map[int16]int64, error) {
	tables := publication.TablesFor(entityMain)
	pins := make(map[int16]int64, len(schemaIDs))
	for _, id := range schemaIDs {
		snap, err := publication.Read(ctx, publication.PgxTx(tx), tables, id)
		var invalid *publication.InvalidRecordError
		switch {
		case errors.Is(err, publication.ErrNoRecord):
			continue
		case errors.As(err, &invalid):
			pins[id] = invalid.Record.Epoch
		case err != nil:
			return nil, fmt.Errorf("pin the publication epoch of schema %d: %w", id, err)
		default:
			pins[id] = snap.Epoch
		}
	}
	return pins, nil
}
