package compaction

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/cdcv2"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"go.uber.org/zap"
)

// stager is a pass's cdcv2.CompactStager. It keeps what its last call
// decided, for the pass's result: a restarted compaction stages again from
// the new head, and the result describes the round that ended the pass.
type stager struct {
	c   *Compactor
	cfg cdc.CompactionConfig

	outcome    CompactionOutcome
	dirtyRatio float64
	baseMB     int64
	deltaMB    int64
	stats      MergeStats
	newBaseKey string
}

// StageCompaction decides what of req.Input to merge, as the V1 compactor
// did, and merges it:
//
//   - promotion, when the delta tier holds at least the target base size:
//     every delta file. A V2 key names its tier, so a delta object cannot be
//     relabelled base; it is merged into a new base object instead;
//   - rewrite, when the dirty ratio (delta rows over base rows) passes the
//     threshold: every base and delta file.
//
// Promotion takes precedence. Without a Merger it merges nothing and
// reports RewritePending.
func (s *stager) StageCompaction(ctx context.Context, req cdcv2.CompactRequest) (cdcv2.CompactStaging, error) {
	*s = stager{c: s.c, cfg: s.cfg, outcome: Noop}
	base, delta := byTier(req.Input.Files)
	baseBytes, deltaBytes := sizeOf(base), sizeOf(delta)
	s.baseMB, s.deltaMB = baseBytes>>20, deltaBytes>>20
	s.dirtyRatio = dirtyRatio(base, delta)
	s.c.Metrics.EmitCompactionDirtyRatio(ctx, req.SchemaID, s.dirtyRatio)

	// Compare bytes, not truncated MB: sub-MB delta tiers must still be able
	// to promote once the byte-precise threshold is lowered.
	promote := len(delta) > 0 && deltaBytes >= s.cfg.TargetBaseSizeBytes
	rewrite := len(base) > 0 && s.dirtyRatio > float64(s.cfg.DirtyRatioPct)/100.0
	if !promote && !rewrite {
		s.c.log().Debug("compaction not needed", zap.Int16("schema_id", req.SchemaID),
			zap.Int64("generation", req.Input.Generation), zap.Int64("base_mb", s.baseMB), zap.Int64("delta_mb", s.deltaMB))
		return cdcv2.CompactStaging{}, nil
	}
	if s.c.Merger == nil {
		s.outcome = RewritePending
		s.c.Metrics.EmitCompactionRewritePending(ctx, req.SchemaID)
		s.c.log().Warn("compaction needed but the compactor has no merge wiring; publishing nothing",
			zap.Int16("schema_id", req.SchemaID), zap.Float64("dirty_ratio", s.dirtyRatio),
			zap.Int64("base_mb", s.baseMB), zap.Int64("delta_mb", s.deltaMB))
		return cdcv2.CompactStaging{}, nil
	}
	sources, outcome := delta, PromotionApplied
	if !promote {
		sources, outcome = append(append([]manifestv2.File{}, base...), delta...), RewriteApplied
	}
	staging, err := s.merge(ctx, req, sources)
	if err != nil {
		return cdcv2.CompactStaging{}, err
	}
	s.outcome = outcome
	return staging, nil
}

// merge verifies sources, merges them into a local scratch file and stages
// it under a fresh base key, written once.
func (s *stager) merge(ctx context.Context, req cdcv2.CompactRequest, sources []manifestv2.File) (cdcv2.CompactStaging, error) {
	// Fail closed before anything is merged (#347): past this point a
	// corrupt source would be folded into the new base.
	if err := s.c.verifySourceChecksums(ctx, req.SchemaID, sources, s.cfg.SkipInputChecksumVerify); err != nil {
		return cdcv2.CompactStaging{}, err
	}
	uris := make([]string, len(sources))
	merged := make([]string, len(sources))
	for i, f := range sources {
		uris[i], merged[i] = s.c.objectURI(f.Key), f.Key
	}
	dir, err := os.MkdirTemp("", "forma-compaction-")
	if err != nil {
		return cdcv2.CompactStaging{}, fmt.Errorf("create the merge scratch directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	scratch := filepath.Join(dir, "merged.parquet")
	stats, err := s.c.Merger.MergeToTmp(ctx, uris, scratch)
	if err != nil {
		return cdcv2.CompactStaging{}, fmt.Errorf("merge %d sources of schema %d generation %d: %w", len(sources), req.SchemaID, req.Input.Generation, err)
	}
	s.stats = stats
	staging := cdcv2.CompactStaging{Merged: merged}
	if stats.RowsOut == 0 {
		// Every source held no row: there is nothing to list in their place.
		return staging, nil
	}
	out, err := s.stageOutput(ctx, req, scratch, stats)
	if err != nil {
		return cdcv2.CompactStaging{}, err
	}
	s.newBaseKey = out.Key
	staging.Outputs = []manifestv2.File{out}
	return staging, nil
}

// stageOutput writes the merged file once under a fresh base key and
// answers its manifest entry.
func (s *stager) stageOutput(ctx context.Context, req cdcv2.CompactRequest, path string, stats MergeStats) (manifestv2.File, error) {
	lo, err := uuid.Parse(stats.RowIDMin)
	if err != nil {
		return manifestv2.File{}, fmt.Errorf("merged file of schema %d: least row id %q: %w", req.SchemaID, stats.RowIDMin, err)
	}
	hi, err := uuid.Parse(stats.RowIDMax)
	if err != nil {
		return manifestv2.File{}, fmt.Errorf("merged file of schema %d: greatest row id %q: %w", req.SchemaID, stats.RowIDMax, err)
	}
	key, err := req.Layout.NewParquetKey(req.SchemaID, req.Epoch, manifestv2.TierBase)
	if err != nil {
		return manifestv2.File{}, fmt.Errorf("mint the merged base key: %w", err)
	}
	f, err := os.Open(path)
	if err != nil {
		return manifestv2.File{}, fmt.Errorf("open the merged file: %w", err)
	}
	defer f.Close()
	h := sha256.New()
	size, err := io.Copy(h, f)
	if err != nil {
		return manifestv2.File{}, fmt.Errorf("hash the merged file: %w", err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return manifestv2.File{}, fmt.Errorf("rewind the merged file: %w", err)
	}
	if err := s.c.Store.Put(ctx, key, f); err != nil {
		return manifestv2.File{}, fmt.Errorf("stage the merged base object %s: %w", key, err)
	}
	return manifestv2.File{
		Tier: manifestv2.TierBase, Key: key, SizeBytes: size, SHA256: hex.EncodeToString(h.Sum(nil)),
		RowCount: stats.RowsOut, RowIDMin: lo, RowIDMax: hi, Columns: stats.Columns,
	}, nil
}

// result describes the pass from rep and what the stager last decided.
func (s *stager) result(schemaID int16, rep cdcv2.CompactReport) CompactionResult {
	r := CompactionResult{
		Outcome: s.outcome, SchemaID: schemaID, Epoch: rep.Input.Epoch, InputGeneration: rep.Input.Generation,
		DirtyRatio: s.dirtyRatio, BaseMB: s.baseMB, DeltaMB: s.deltaMB,
		Restarts: rep.Restarts, Resends: rep.Resends, Rebases: rep.Rebases,
	}
	if r.Outcome == "" {
		r.Outcome = Noop
	}
	if rep.Committed {
		r.Generation = rep.Generation.Generation
		r.FilesMerged = len(rep.Staging.Merged)
		r.RowsIn, r.RowsOut, r.NewBaseKey = s.stats.RowsIn, s.stats.RowsOut, s.newBaseKey
	}
	return r
}

func byTier(files []manifestv2.File) (base, delta []manifestv2.File) {
	for _, f := range files {
		if f.Tier == manifestv2.TierBase {
			base = append(base, f)
		} else {
			delta = append(delta, f)
		}
	}
	byRowID := func(a, b manifestv2.File) int { return strings.Compare(a.RowIDMin.String(), b.RowIDMin.String()) }
	slices.SortStableFunc(base, byRowID)
	slices.SortStableFunc(delta, byRowID)
	return base, delta
}

func sizeOf(files []manifestv2.File) int64 {
	var n int64
	for _, f := range files {
		n += f.SizeBytes
	}
	return n
}

// dirtyRatio estimates the share of base rows the deltas update: delta rows
// over base rows, 0 without a base row.
func dirtyRatio(base, delta []manifestv2.File) float64 {
	var baseRows, deltaRows int64
	for _, f := range base {
		baseRows += f.RowCount
	}
	for _, f := range delta {
		deltaRows += f.RowCount
	}
	if baseRows == 0 {
		return 0
	}
	return float64(deltaRows) / float64(baseRows)
}
