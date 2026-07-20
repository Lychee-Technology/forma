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
// version-aware coverage probe). Both take bucket-relative keys. Only
// consulted under --repair.
type StatsReader interface {
	FileStats(ctx context.Context, key string) (compaction.MergeStats, error)
	UncoveredRows(ctx context.Context, key string, listedKeys []string) ([]compaction.UncoveredRow, error)
}

// LiveRowChecker reports which of the given row ids are NOT live in the
// Postgres entity store. A row absent from every manifest-listed parquet
// AND missing from Postgres was deleted — its tombstone won a compaction
// merge and was dropped, so re-appending the file would resurrect it.
type LiveRowChecker interface {
	MissingLiveRows(ctx context.Context, schemaID int16, rowIDs []string) ([]string, error)
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
		// Init-shaped base orphans are never GC candidates: an in-flight
		// cdc-init promotes them long before publishing the manifest and
		// holds no advisory lock. Delta leftovers require the repair
		// analysis, so they are only deletable under --repair --gc.
		candidates := append(append([]ObjectInfo(nil), d.baseMergedOrphans...), d.tmpOrphans...)
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
