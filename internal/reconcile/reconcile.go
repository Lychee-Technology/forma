package reconcile

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/lychee-technology/forma/internal/compaction"
	"github.com/lychee-technology/forma/internal/manifest"
)

// ManifestStore loads and saves one schema's manifest with optimistic
// concurrency. Load must have LoadOrCreate semantics: a schema without a
// manifest reconciles as empty instead of erroring.
type ManifestStore interface {
	Load(ctx context.Context, schemaID int16) (*manifest.Manifest, string, error)
	Save(ctx context.Context, schemaID int16, m *manifest.Manifest, etag string) (string, error)
}

// StatsReader inspects one parquet file's contents: FileStats recomputes
// manifest metadata; UncoveredRows returns the rows whose newest version no
// listed file supersedes, with a tombstone flag (the repair guard's
// version-aware coverage probe) — with no listed keys it enumerates every
// distinct row id in the file (#292 init promotion). FileColumns probes the
// footer for the #256 column stamp. All take bucket-relative keys. Only
// consulted under --repair.
type StatsReader interface {
	FileStats(ctx context.Context, key string) (compaction.MergeStats, error)
	UncoveredRows(ctx context.Context, key string, listedKeys []string) ([]compaction.UncoveredRow, error)
	FileColumns(ctx context.Context, key string) (map[string]string, error)
}

// LiveRowChecker reports Postgres entity-store liveness. MissingLiveRows
// returns which of the given row ids are NOT live (a row absent from every
// manifest-listed parquet AND missing from Postgres was deleted — its
// tombstone won a compaction merge and was dropped, so re-appending would
// resurrect it; see classifyDeltaOrphan in repair.go). LiveRowCount returns
// the number of live rows for a schema — the right-hand side of init
// promotion's coverage identity (#292): the orphan set may replace the base
// tier only if it provably covers every one of these rows.
type LiveRowChecker interface {
	MissingLiveRows(ctx context.Context, schemaID int16, rowIDs []string) ([]string, error)
	LiveRowCount(ctx context.Context, schemaID int16) (int64, error)
}

// Locker serializes reconciliation against the live flusher per schema.
// unlock is non-nil exactly when locked is true.
type Locker interface {
	TryLock(ctx context.Context, schemaID int16) (locked bool, unlock func(), err error)
}

// SchemaEnumerator yields the schema IDs to reconcile.
type SchemaEnumerator interface {
	SchemaIDs(ctx context.Context) ([]int16, error)
}

// GCStateStore persists per-schema first-unlisted sighting timestamps
// (key -> unix ms) with optimistic concurrency. GC deletes an orphan only
// after it has been observed unlisted for longer than the grace period —
// LastModified alone cannot express "unlisted duration" (#188 follow-up:
// an old source freshly spliced out by the compactor would otherwise be
// deleted inside the in-flight-reader window).
type GCStateStore interface {
	Load(ctx context.Context, schemaID int16) (map[string]int64, string, error)
	Save(ctx context.Context, schemaID int16, state map[string]int64, etag string) (string, error)
}

// Options selects the reconcile actions. The zero value is a read-only
// report.
type Options struct {
	Repair         bool          // append delta-shaped orphans back to the manifest
	GC             bool          // delete base-shaped and _tmp orphans past the grace period
	GCGrace        time.Duration // minimum object age before GC may delete it
	MaxETagRetries int           // manifest save retries on optimistic-concurrency conflict
}

// Reconciler diffs S3 parquet objects against per-schema manifests and
// optionally repairs (append delta orphans) or garbage-collects (delete
// compaction leftovers). See the package comment for the recovery model.
type Reconciler struct {
	Lister     ObjectLister
	Deleter    ObjectDeleter
	Manifests  ManifestStore
	Stats      StatsReader    // may be nil unless Opts.Repair
	LiveRows   LiveRowChecker // may be nil unless Opts.Repair
	Locker     Locker
	Schemas    SchemaEnumerator
	GCStates   GCStateStore // may be nil unless Opts.GC
	Now        func() time.Time
	Bucket     string
	DataPrefix string
	Logger     *zap.Logger
	Opts       Options
}

// Run reconciles every enumerated schema. Per-schema failures are recorded
// in the report and do not abort the run; the returned error is reserved
// for failures that prevent reconciling anything at all.
func (r *Reconciler) Run(ctx context.Context) (Report, error) {
	ids, err := r.Schemas.SchemaIDs(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("enumerate schemas: %w", err)
	}
	report := Report{Schemas: make([]SchemaReport, 0, len(ids))}
	for _, schemaID := range ids {
		report.Schemas = append(report.Schemas, r.reconcileSchema(ctx, schemaID))
	}
	return report, nil
}

func (r *Reconciler) reconcileSchema(ctx context.Context, schemaID int16) SchemaReport {
	s := SchemaReport{SchemaID: schemaID}

	locked, unlock, err := r.Locker.TryLock(ctx, schemaID)
	if err != nil {
		s.Err = fmt.Errorf("acquire schema %d advisory lock: %w", schemaID, err)
		return s
	}
	if !locked {
		s.Skipped = true
		r.Logger.Info("schema lock not acquired, skipping", zap.Int16("schema_id", schemaID))
		return s
	}
	defer unlock()

	// Load the manifest BEFORE listing: a file created after the manifest
	// snapshot then still appears in the listing, so a racing compactor's
	// freshly committed base entry can never be reported dangling.
	m, etag, err := r.Manifests.Load(ctx, schemaID)
	if err != nil {
		s.Err = fmt.Errorf("load schema %d manifest: %w", schemaID, err)
		return s
	}

	prefix := schemaDataPrefix(r.DataPrefix, schemaID)
	objects, err := r.Lister.ListObjects(ctx, prefix)
	if err != nil {
		s.Err = fmt.Errorf("list schema %d objects under %s: %w", schemaID, prefix, err)
		return s
	}

	d := diffSchema(r.Bucket, r.DataPrefix, schemaID, objects, m)
	s.DeltaOrphans = objectKeyList(d.deltaOrphans)
	s.BaseOrphans = append(objectKeyList(d.baseInitOrphans), objectKeyList(d.baseMergedOrphans)...)
	s.TmpOrphans = objectKeyList(d.tmpOrphans)
	s.Unknown = objectKeyList(d.unknown)
	s.Unverifiable = d.unverifiable
	s.Dangling, err = r.confirmDangling(ctx, schemaID, d.dangling)
	if err != nil {
		// An unconfirmable candidate is a tool failure, not a data-drift
		// verdict: the caller maps s.Err to exit 1, never 2.
		s.Err = fmt.Errorf("confirm schema %d dangling candidates: %w", schemaID, err)
		return s
	}

	var deltaLeftovers []ObjectInfo
	promotedInit := false
	if r.Opts.Repair && len(d.baseInitOrphans) > 0 {
		promo, pm, petag, err := r.promoteInitOrphans(ctx, schemaID, m, etag, d.baseInitOrphans)
		if err != nil {
			s.Err = err
			return s
		}
		m, etag = pm, petag
		s.PromotedBase = promo.promoted
		s.InitPromotionRefusal = promo.refusal
		promotedInit = len(promo.promoted) > 0
	}
	if r.Opts.Repair && len(d.deltaOrphans) > 0 {
		outcome, err := r.repairSchema(ctx, schemaID, m, etag, d.deltaOrphans)
		if err != nil {
			s.Err = err
			return s
		}
		s.Repaired = outcome.repaired
		s.DeltaLeftovers = objectKeyList(outcome.leftovers)
		deltaLeftovers = outcome.leftovers
	}
	if r.Opts.GC {
		// Init-shaped base orphans are GC candidates since #290 — unless this
		// run just promoted them (#292): a promoted set is now manifest-listed
		// inventory, and gcSchema's state prune drops their sighting entries
		// so a later unlisting restarts the grace clock. A refused set keeps
		// the #290 behavior: recovery for a failed publish is re-running
		// cdc-init or a later --repair pass that can prove coverage.
		// Delta leftovers require the repair analysis, so they are only
		// deletable under --repair --gc.
		var candidates []ObjectInfo
		if !promotedInit {
			candidates = append(candidates, d.baseInitOrphans...)
		}
		candidates = append(candidates, d.baseMergedOrphans...)
		candidates = append(candidates, d.tmpOrphans...)
		candidates = append(candidates, deltaLeftovers...)
		// Run even with zero candidates: sighting-state entries for keys
		// that stopped being orphans must be pruned so a later unlisting
		// restarts their grace clock.
		s.Deleted, err = r.gcSchema(ctx, schemaID, candidates)
		if err != nil {
			s.Err = err
			return s
		}
	}
	return s
}

// confirmDangling re-verifies dangling candidates against a fresh manifest
// load and a per-key existence probe. The lock excludes the flusher but not
// the compactor or cdc-init, so a splice landing mid-run could otherwise
// surface a properly handled (or freshly created) object as dangling. A
// failed reload or probe leaves the candidate UNCONFIRMED and is returned
// as an error — a storage outage must surface as a tool failure (exit 1),
// never as a confirmed data-drift report (exit 2).
func (r *Reconciler) confirmDangling(ctx context.Context, schemaID int16, dangling []string) ([]string, error) {
	if len(dangling) == 0 {
		return nil, nil
	}
	m2, _, err := r.Manifests.Load(ctx, schemaID)
	if err != nil {
		return nil, fmt.Errorf("reload manifest: %w", err)
	}
	still := make(map[string]bool, len(m2.Files))
	for _, f := range m2.Files {
		if key, ok := normalizeKey(r.Bucket, f.Path); ok {
			still[key] = true
		}
	}

	var confirmed []string
	for _, key := range dangling {
		if !still[key] {
			continue // spliced out concurrently; nothing dangling anymore
		}
		objs, err := r.Lister.ListObjects(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("re-probe candidate %s: %w", key, err)
		}
		exists := false
		for _, o := range objs {
			if o.Key == key {
				exists = true
				break
			}
		}
		if !exists {
			confirmed = append(confirmed, key)
		}
	}
	return confirmed, nil
}

func objectKeyList(objs []ObjectInfo) []string {
	if len(objs) == 0 {
		return nil
	}
	keys := make([]string, 0, len(objs))
	for _, o := range objs {
		keys = append(keys, o.Key)
	}
	return keys
}
