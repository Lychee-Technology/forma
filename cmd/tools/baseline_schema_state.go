package main

import (
	"context"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma/internal/sqlutil"
	"github.com/lychee-technology/forma/internal/widthaudit"
)

// storedState is one schema's stored data as a baseline saw it before
// auditing it: the count and the version sum of its entity_main rows, and
// its live parquet objects. baseline-schema reads it again in the
// transaction that records the baseline, and refuses when it moved: the
// audit then describes data that is no longer there (#733).
//
// Every write path stamps the row's entity_main version. A create adds a
// row, an update raises ltbase_updated_at past its previous value
// (GREATEST over the stored version, #274), and a delete removes the row, so
// a write that commits during the audit moves the count or the sum. An
// export or a compaction moves the object set. This is a fingerprint, not a
// lock: it catches a writer or exporter still running during the audit, and
// it cannot hold off one that writes after the baseline commits. The
// upgrade procedure therefore stops the old release first.
type storedState struct {
	rows     int64
	versions string
	// objects is sorted, and nil without a lakehouse tier.
	objects []string
}

// readStoredState reads schemaID's stored state through q: the pool before
// the audit, the recording transaction after it.
func readStoredState(ctx context.Context, q widthaudit.Querier, opts baselineOptions, schemaID int16) (storedState, error) {
	// SUM over BIGINT is NUMERIC, so the sum cannot overflow; it is compared
	// as text.
	query := fmt.Sprintf("SELECT COUNT(*), COALESCE(SUM(ltbase_updated_at), 0)::text FROM %s WHERE ltbase_schema_id = $1",
		sqlutil.SanitizeIdentifier(opts.tables.EntityMain))
	rows, err := q.Query(ctx, query, schemaID)
	if err != nil {
		return storedState{}, fmt.Errorf("read the entity_main versions of schema id %d: %w", schemaID, err)
	}
	var state storedState
	if _, err := pgx.ForEachRow(rows, []any{&state.rows, &state.versions}, func() error { return nil }); err != nil {
		return storedState{}, fmt.Errorf("read the entity_main versions of schema id %d: %w", schemaID, err)
	}
	if opts.parquet == nil {
		return state, nil
	}
	objects, err := opts.parquet.objects(ctx, schemaID)
	if err != nil {
		return storedState{}, fmt.Errorf("resolve the parquet objects of schema id %d: %w", schemaID, err)
	}
	state.objects = slices.Sorted(slices.Values(objects))
	return state, nil
}

// movedDuringAudit reads each seed's stored state again on tx and names
// every schema whose state differs from the one its audit read.
func movedDuringAudit(ctx context.Context, tx pgx.Tx, opts baselineOptions, seeds []baselineSeed) ([]string, error) {
	var moved []string
	for _, seed := range seeds {
		now, err := readStoredState(ctx, tx, opts, seed.decl.SchemaID)
		if err != nil {
			return nil, fmt.Errorf("read the stored state of schema %s again: %w", seed.decl.SchemaName, err)
		}
		before := seed.state
		where := fmt.Sprintf("schema %s (id %d)", seed.decl.SchemaName, seed.decl.SchemaID)
		if now.rows != before.rows || now.versions != before.versions {
			moved = append(moved, fmt.Sprintf("%s: entity_main went from %d row(s) with version sum %s to %d row(s) with version sum %s",
				where, before.rows, before.versions, now.rows, now.versions))
		}
		if added, removed := diffObjects(before.objects, now.objects); added+removed > 0 {
			moved = append(moved, fmt.Sprintf("%s: the live parquet objects changed (%d added, %d removed)", where, added, removed))
		}
	}
	return moved, nil
}

// diffObjects counts the objects of now that before lacks, and the reverse.
func diffObjects(before, now []string) (added, removed int) {
	for _, o := range now {
		if _, found := slices.BinarySearch(before, o); !found {
			added++
		}
	}
	for _, o := range before {
		if _, found := slices.BinarySearch(now, o); !found {
			removed++
		}
	}
	return added, removed
}
