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

// StatsReader recomputes one parquet file's manifest metadata from its
// contents. Only consulted under --repair.
type StatsReader interface {
	FileStats(ctx context.Context, uri string) (compaction.MergeStats, error)
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
	Stats      StatsReader // may be nil unless Opts.Repair
	Locker     Locker
	Schemas    SchemaEnumerator
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

	prefix := schemaDataPrefix(r.DataPrefix, schemaID)
	objects, err := r.Lister.ListObjects(ctx, prefix)
	if err != nil {
		s.Err = fmt.Errorf("list schema %d objects under %s: %w", schemaID, prefix, err)
		return s
	}

	m, etag, err := r.Manifests.Load(ctx, schemaID)
	if err != nil {
		s.Err = fmt.Errorf("load schema %d manifest: %w", schemaID, err)
		return s
	}

	d := diffSchema(r.Bucket, r.DataPrefix, schemaID, objects, m)
	s.DeltaOrphans = objectKeyList(d.deltaOrphans)
	s.BaseOrphans = objectKeyList(d.baseOrphans)
	s.TmpOrphans = objectKeyList(d.tmpOrphans)
	s.Unknown = objectKeyList(d.unknown)
	s.Dangling = d.dangling
	s.Unverifiable = d.unverifiable

	if r.Opts.Repair && len(d.deltaOrphans) > 0 {
		s.Repaired, err = r.repairSchema(ctx, schemaID, m, etag, d.deltaOrphans)
		if err != nil {
			s.Err = err
			return s
		}
	}
	if r.Opts.GC {
		candidates := append(append([]ObjectInfo(nil), d.baseOrphans...), d.tmpOrphans...)
		if len(candidates) > 0 {
			s.Deleted = r.gcSchema(ctx, schemaID, candidates)
		}
	}
	return s
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
