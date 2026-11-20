// Package compaction merges a schema's parquet objects and publishes the
// result as a Protocol V2 compact generation (docs/publication-protocol-v2.md,
// "What a generation replaces" and "Writers"). A pass reads the schema's
// publication record, decides from the current generation's manifest what to
// merge, merges it into a new base object under a fresh write-once key, and
// publishes the next generation through internal/cdcv2, with compare-and-set
// against the exact generation it read. It deletes nothing: the merged sources
// and the replaced generation stay readable for in-flight queries until
// garbage collection retires them (#891).
package compaction

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/cdcv2"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/telemetry"
	"go.uber.org/zap"
)

// CompactionOutcome describes the result of a compaction pass for a single schema.
type CompactionOutcome string

const (
	// Noop is reported when there is nothing to compact: the schema has no
	// published generation, or its generation calls for no merge.
	Noop CompactionOutcome = "noop"
	// PromotionApplied is reported when the delta tier reached the target
	// base size and its files were merged into one new base object.
	PromotionApplied CompactionOutcome = "promotion_applied"
	// RewritePending is reported when a merge is called for but no Merger is
	// wired, so the pass leaves everything untouched.
	RewritePending CompactionOutcome = "rewrite_pending"
	// RewriteApplied is reported when the dirty-ratio rewrite merged every
	// base and delta file into one new base object.
	RewriteApplied CompactionOutcome = "rewrite_applied"
)

// CompactionResult carries the outcome and metadata from a compaction pass.
type CompactionResult struct {
	Outcome  CompactionOutcome
	SchemaID int16
	// Epoch and InputGeneration name the generation the pass read and
	// merged from. Generation is the compact generation it committed, 0
	// when it committed none.
	Epoch           int64
	InputGeneration int64
	Generation      int64
	DirtyRatio      float64
	BaseMB          int64
	DeltaMB         int64

	// Merge metadata, populated when the pass committed a merge.
	FilesMerged int    // source files folded into the new base
	RowsIn      int64  // rows read across all source files
	RowsOut     int64  // newest versions per row, tombstones included, written to the new base
	NewBaseKey  string // key of the merged base object; "" when the merge wrote no row

	// Restarts, Resends and Rebases count the publication's retries
	// (cdcv2.CompactReport).
	Restarts, Resends, Rebases int
}

// settleTimeout bounds the one in-process attempt to settle a compaction
// whose COMMIT went without an answer. It runs on a fresh context: the pass's
// own may be what ended.
const settleTimeout = 2 * time.Minute

// Compactor performs Base/Delta maintenance of one schema.
type Compactor struct {
	Logger *zap.Logger
	Config cdc.CompactionConfig
	// Writer publishes the compact generation; the merge outputs are keyed
	// under its layout.
	Writer *cdcv2.Writer
	// Store writes the merge outputs once. It is the store Writer stages
	// manifests in.
	Store manifestv2.Store
	// Merger merges the sources; nil makes a pass that calls for a merge
	// report RewritePending.
	Merger Merger
	// Bucket and SourceURI address a key for the merge engine: SourceURI
	// when set, s3://<Bucket>/<key> otherwise (federatedv2.S3URI's form).
	Bucket    string
	SourceURI func(key string) string
	// ObjectReader hashes merge sources before the merge, against the
	// digest their manifest entry lists (#347). Nil disables it.
	ObjectReader cdc.S3GetClient
	// Epoch pins the schema's lifecycle epoch (0: the epoch the pass first
	// reads). A record of another epoch fails the pass closed.
	Epoch int64

	// Metrics receives the compaction_* and parquet_checksum_mismatch_total
	// metrics (#423); build it with telemetry.NewSink from the operator's
	// forma.MetricEmitter. Nil emits nothing.
	Metrics *telemetry.Sink
}

// RunOnce runs one compaction pass of Config.SchemaID and returns its result.
// A compaction that loses its compare-and-set rebases over flushes, or reads
// the head and merges again, at most as often as Writer's Restarts allow
// (NewWriter sets them from Config.MaxRetries).
func (c *Compactor) RunOnce(ctx context.Context) (CompactionResult, error) {
	if c.Writer == nil {
		return CompactionResult{}, errors.New("compaction needs a V2 publication writer")
	}
	if c.Config.SchemaID == 0 {
		return CompactionResult{}, errors.New("schema_id required for compaction")
	}
	if c.Merger != nil && c.Store == nil {
		return CompactionResult{}, errors.New("compaction with a merger needs the object store its outputs are staged in")
	}
	schemaID := c.Config.SchemaID
	st := &stager{c: c, cfg: c.Config.WithDefaults()}
	rep, err := c.Writer.Compact(ctx, schemaID, c.Epoch, st)
	rep, err = c.settle(ctx, rep, err)
	result := st.result(schemaID, rep)
	if err != nil {
		return result, fmt.Errorf("compact schema %d: %w", schemaID, err)
	}
	if !rep.Committed {
		if result.Outcome != RewritePending {
			result.Outcome = Noop
		}
		return result, nil
	}
	if result.Outcome == RewriteApplied {
		// The series counts dirty-ratio rewrites only, never a promotion
		// (docs/telemetry.md).
		c.Metrics.EmitCompactionRewriteApplied(ctx, schemaID)
	}
	c.log().Info("compaction published",
		zap.Int16("schema_id", schemaID),
		zap.String("outcome", string(result.Outcome)),
		zap.Int64("epoch", result.Epoch),
		zap.Int64("input_generation", result.InputGeneration),
		zap.Int64("generation", result.Generation),
		zap.Int("files_merged", result.FilesMerged),
		zap.Int64("rows_in", result.RowsIn),
		zap.Int64("rows_out", result.RowsOut),
		zap.String("new_base_key", result.NewBaseKey))
	return result, nil
}

// settle settles, once and on a fresh context, a compaction the writer
// stopped with ErrUnresolved: its COMMIT went without an answer and the
// writer could not tell whether it landed. Either way nothing is lost: a
// compaction that never commits leaves its staged objects unreferenced, and
// the next pass merges from the head.
func (c *Compactor) settle(ctx context.Context, rep cdcv2.CompactReport, err error) (cdcv2.CompactReport, error) {
	var u *cdcv2.UnresolvedError
	if !errors.As(err, &u) {
		return rep, err
	}
	c.log().Warn("compaction publication outcome unknown; settling it once", zap.Int16("schema_id", u.Cur.SchemaID), zap.Error(err))
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
	defer cancel()
	gen, serr := c.Writer.Settle(sctx, u)
	if serr == nil {
		rep.Committed, rep.Generation = true, gen
		return rep, nil
	}
	return rep, c.settleError(u, serr)
}

// settleError describes a compaction that settling did not find committed.
// A fence that proved it can never land is final. Otherwise the COMMIT may
// still have landed, and the current record cannot tell: a flush may have
// replaced the compaction's generation since, and its token is then on the
// record. Only the generation history keeps the compaction's token at its
// number, which is what publication.Resolve reads.
func (c *Compactor) settleError(u *cdcv2.UnresolvedError, serr error) error {
	gen := u.Cur.Generation + 1
	if errors.Is(serr, cdcv2.ErrAbandoned) {
		return fmt.Errorf("the compaction staged generation %d of schema %d with token %s, and settling it proved it can never "+
			"commit: nothing it staged is listed, and the next pass merges from the head: %w", gen, u.Cur.SchemaID, u.Next.Token, serr)
	}
	return fmt.Errorf("the compaction staged generation %d of schema %d with token %s, and whether its COMMIT landed is still "+
		"unresolved, and the current record does not tell: a flush committed after a landed compaction puts its own token there. "+
		"It landed exactly when the %s history row of schema %d generation %d carries that token (what publication.Resolve reads). "+
		"Either way nothing is lost: its staged objects must stay until garbage collection, and the next pass merges from the head: %w",
		gen, u.Cur.SchemaID, u.Next.Token, "<entity main table>"+publication.GenerationTableSuffix, u.Cur.SchemaID, gen, serr)
}

// objectURI addresses key for the merge engine.
func (c *Compactor) objectURI(key string) string {
	if c.SourceURI != nil {
		return c.SourceURI(key)
	}
	return "s3://" + c.Bucket + "/" + key
}

func (c *Compactor) log() *zap.Logger {
	if c.Logger == nil {
		return zap.NewNop()
	}
	return c.Logger
}
