package cdcv2

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// CompactStager decides what a compaction merges and writes the merge
// outputs (docs/publication-protocol-v2.md, "What a generation replaces").
type CompactStager interface {
	// StageCompaction picks the files of req.Input to merge, merges them,
	// and writes the outputs as base objects under fresh keys of req.Layout
	// and req.Epoch (Layout.NewParquetKey), each written once. It runs
	// before the publication transaction, and what it writes stays
	// unreachable until that transaction commits. An answer that merges
	// nothing stages nothing and publishes nothing.
	//
	// The merge must keep each merged row's newest version, tombstones
	// included: a flush may rebase over the compaction, and its older copy
	// of a row deleted since would otherwise be the row's only one.
	StageCompaction(ctx context.Context, req CompactRequest) (CompactStaging, error)
}

// CompactRequest is one compaction's input: the generation the record named
// when the compaction read it, verified through the cache.
type CompactRequest struct {
	SchemaID int16
	Epoch    int64
	Layout   manifestv2.Layout
	Input    *manifestv2.Manifest
}

// CompactStaging is what a compaction staged: the keys of the input's files
// it merged, and the base files it wrote in their place.
type CompactStaging struct {
	Merged  []string
	Outputs []manifestv2.File
}

// CompactReport is what a Compact did.
type CompactReport struct {
	// Input is the record the compaction merged from, the last round's
	// when it restarted; its generation is the committed manifest's
	// input_generation. Zero when the record was never read.
	Input publication.Record
	// Committed says a compact generation committed, and Generation is the
	// record it committed. A compaction that found nothing to merge, or a
	// schema with nothing published, commits nothing.
	Committed  bool
	Generation publication.Record
	// Staging is what the committed compaction merged and wrote, or what
	// the last round staged when nothing committed.
	Staging CompactStaging
	// Restarts counts abandoned compactions after which it read the head
	// and staged again; Resends and Rebases count the writer's retries.
	Restarts, Resends, Rebases int
}

// Compact publishes one compaction of schemaID: it reads the record, hands
// the stager the current generation, and publishes the staged merge as the
// next generation with compare-and-set, keeping the record's lineage.
//
// pin is the epoch the process loaded the schema at (0 pins the epoch the
// first read finds, for every later round): a record of another epoch fails
// the compaction closed (ErrEpochChanged). A schema with nothing published
// (UNPUBLISHED) has nothing to compact and commits nothing. One that is
// AWAITING_INIT or DROPPED is refused.
//
// A compaction that loses its compare-and-set rebases over flushes only
// (publication.Rebase): its merged sources are still listed, and the
// generation is rebuilt from the head under a fresh token. Over a compaction,
// a cdc-init or another epoch it is abandoned, and Compact reads the head
// and stages again from it, at most Config.Restarts times. A staging
// abandoned or never committed is unreachable garbage; nothing is deleted
// here, and old generations and their objects stay readable until garbage
// collection retires them.
func (w *Writer) Compact(ctx context.Context, schemaID int16, pin int64, st CompactStager) (CompactReport, error) {
	var rep CompactReport
	for {
		err := w.compactRound(ctx, schemaID, pin, st, &rep)
		if pin == 0 && rep.Input.Epoch != 0 {
			pin = rep.Input.Epoch
		}
		if err == nil || !errors.Is(err, ErrAbandoned) || rep.Restarts >= w.cfg.Restarts {
			return rep, err
		}
		rep.Restarts++
	}
}

// compactRound reads the record, stages a compaction of its generation and
// publishes it.
func (w *Writer) compactRound(ctx context.Context, schemaID int16, pin int64, st CompactStager, rep *CompactReport) error {
	cur, err := w.readRecord(ctx, schemaID, pin)
	if err != nil {
		return fmt.Errorf("compact: %w", err)
	}
	rep.Input, rep.Staging = cur, CompactStaging{}
	switch cur.State {
	case publication.Unpublished:
		return nil
	case publication.AwaitingInit:
		return fmt.Errorf("compact schema %d: only a cdc-init publishes its first V2 generation: %w", schemaID, publication.ErrAwaitingInit)
	case publication.Dropped:
		return fmt.Errorf("compact schema %d at epoch %d: %w", schemaID, cur.Epoch, publication.ErrDropped)
	}
	// A merge's outputs are keyed under w's layout, and a full rewrite
	// replaces every file of the generation: under another prefix than the
	// published one it would move the inventory away from the flush writers.
	if err := w.cfg.Layout.Holds(cur.ManifestKey); err != nil {
		return fmt.Errorf("compact schema %d generation %d: %w", schemaID, cur.Generation, err)
	}
	input, err := w.manifest(ctx, cur)
	if err != nil {
		return fmt.Errorf("compact schema %d: %w", schemaID, err)
	}
	stagedAt, err := w.now(ctx, w.cfg.DB)
	if err != nil {
		return fmt.Errorf("compact schema %d: %w", schemaID, err)
	}
	if err := w.fail(ctx, PointStageObjects); err != nil {
		return fmt.Errorf("compact schema %d: %w", schemaID, err)
	}
	staging, err := st.StageCompaction(ctx, CompactRequest{SchemaID: schemaID, Epoch: cur.Epoch, Layout: w.cfg.Layout, Input: input})
	if err != nil {
		return fmt.Errorf("compact schema %d generation %d: stage the merge: %w", schemaID, cur.Generation, err)
	}
	rep.Staging = staging
	if len(staging.Merged) == 0 {
		return nil
	}
	p := &pub{
		kind: publication.KindCompact,
		cur:  cur,
		build: func(ctx context.Context, head publication.Record) (publication.Next, error) {
			return w.stageCompactManifest(ctx, head, cur.Generation, staging, stagedAt)
		},
	}
	if p.next, err = p.build(ctx, cur); err != nil {
		return fmt.Errorf("compact schema %d: %w", schemaID, err)
	}
	out, err := w.publish(ctx, p)
	rep.Resends += p.resends
	rep.Rebases += p.rebases
	if err != nil {
		return fmt.Errorf("compact: %w", err)
	}
	rep.Committed, rep.Generation = true, out
	return nil
}

// manifest answers the verified manifest of rec's current generation.
func (w *Writer) manifest(ctx context.Context, rec publication.Record) (*manifestv2.Manifest, error) {
	ref, err := manifestv2.RecordRef(rec)
	if err != nil {
		return nil, fmt.Errorf("name the manifest of generation %d: %w", rec.Generation, err)
	}
	m, err := w.cfg.Cache.Get(ctx, ref)
	if err != nil {
		return nil, fmt.Errorf("get the manifest of generation %d: %w", rec.Generation, err)
	}
	return m, nil
}

// stageCompactManifest stages the compact generation after head: head's
// files less the merged sources, plus the outputs, under a fresh token.
// input is the generation the compaction merged from; head is that
// generation, or a head the compaction rebased onto over flushes only.
func (w *Writer) stageCompactManifest(ctx context.Context, head publication.Record, input int64, staging CompactStaging, stagedAt int64) (publication.Next, error) {
	if err := w.cfg.Layout.Holds(head.ManifestKey); err != nil {
		return publication.Next{}, fmt.Errorf("rebuild the compaction over generation %d: %w", head.Generation, err)
	}
	parent, err := w.manifest(ctx, head)
	if err != nil {
		return publication.Next{}, err
	}
	h, err := manifestv2.Stage(head, publication.KindCompact, head.Lineage, uuid.New())
	if err != nil {
		return publication.Next{}, err
	}
	h.InputGeneration = input
	m, err := manifestv2.NewCompact(parent, h, staging.Merged, staging.Outputs)
	if err != nil {
		return publication.Next{}, err
	}
	return w.stageManifest(ctx, m, stagedAt)
}
