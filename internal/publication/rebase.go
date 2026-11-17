package publication

import (
	"context"
	"errors"
	"fmt"
	"slices"
)

// ErrRebaseRefused reports that a writer that lost its compare-and-set must
// not move its staged generation onto the record's new head: what committed
// in between changed content the writer's generation would silently drop or
// resurrect. The writer abandons the staged objects (garbage collection
// reclaims them) and starts over from the new head.
var ErrRebaseRefused = errors.New("rebase refused")

const kindsSinceSQL = `SELECT generation, kind FROM %s
	WHERE schema_id = $1 AND generation > $2 AND generation <= $3
	ORDER BY generation`

// KindsSince reads the kinds of the generations committed after from up to
// head, in order. Within one epoch generations are consecutive, so a gap
// means garbage collection already removed history the rebase decision
// needs, and is refused rather than read as "nothing happened".
func KindsSince(ctx context.Context, q Querier, t Tables, from, head Record) ([]Kind, error) {
	rows, err := q.QueryContext(ctx, fmt.Sprintf(kindsSinceSQL, t.Generation), from.SchemaID, from.Generation, head.Generation)
	if err != nil {
		return nil, fmt.Errorf("read schema %d generations %d..%d: %w", from.SchemaID, from.Generation+1, head.Generation, err)
	}
	defer rows.Close()
	var kinds []Kind
	want := from.Generation + 1
	for rows.Next() {
		var generation int64
		var kind string
		if err := rows.Scan(&generation, &kind); err != nil {
			return nil, fmt.Errorf("scan schema %d generation kind: %w", from.SchemaID, err)
		}
		if generation != want {
			return nil, fmt.Errorf("schema %d history skips from generation %d to %d: %w", from.SchemaID, want, generation, ErrRebaseRefused)
		}
		kinds = append(kinds, Kind(kind))
		want++
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read schema %d generation kinds: %w", from.SchemaID, err)
	}
	if want != head.Generation+1 {
		return nil, fmt.Errorf("schema %d history ends at generation %d, the head is %d: %w", from.SchemaID, want-1, head.Generation, ErrRebaseRefused)
	}
	return kinds, nil
}

// Rebase decides whether a writer of kind, which staged its generation on
// from and lost the compare-and-set to head, may restage onto head; since is
// KindsSince(from, head). A nil answer lets it: the writer rebuilds its
// manifest from head's file list (and, for a flush, its own new files) under
// a fresh token, and keeps its marks.
//
//   - Any writer: never across an epoch. The new epoch's record promises
//     nothing about the old epoch's rows.
//   - A flush appends rows it exported at exact versions, which stay correct
//     over any head of the same epoch; it keeps head's lineage.
//   - A compaction replaces the files it merged. It may rebase over flushes
//     only, which append files and remove none; a compaction or cdc-init in
//     between may have removed or replaced files it merged.
//   - A cdc-init replaces every file with an export from PostgreSQL. It may
//     rebase over compactions only, which change no content. A flush in
//     between appended rows the export may predate, which the replacement
//     would drop while their entries stay marked.
func Rebase(kind Kind, from, head Record, since []Kind) error {
	if head.Epoch != from.Epoch {
		return fmt.Errorf("schema %d moved from epoch %d to %d: %w", from.SchemaID, from.Epoch, head.Epoch, ErrRebaseRefused)
	}
	allowed := map[Kind][]Kind{
		KindFlush:   {KindFlush, KindCompact, KindInit},
		KindCompact: {KindFlush},
		KindInit:    {KindCompact},
	}[kind]
	if allowed == nil {
		return fmt.Errorf("unknown generation kind %q: %w", kind, ErrRebaseRefused)
	}
	for i, k := range since {
		if !slices.Contains(allowed, k) {
			return fmt.Errorf("a %s staged on schema %d generation %d cannot rebase over the %s that committed generation %d: %w",
				kind, from.SchemaID, from.Generation, k, from.Generation+int64(i)+1, ErrRebaseRefused)
		}
	}
	return nil
}
