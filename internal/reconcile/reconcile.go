// Package reconcile implements Protocol V2 manifest-reconcile
// (docs/publication-protocol-v2.md, "Garbage collection and reader lifetime";
// docs/manifest-reconcile.md): a read-only, lock-free diagnosis of each
// schema's publication state and, under --gc, ordered garbage collection of
// expired generations and horizon-fenced orphans.
package reconcile

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	"go.uber.org/zap"

	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// ErrRetentionUnsafe reports options under which garbage collection could
// delete an object a reader still scans: a retention grace shorter than the
// reader's pass deadline plus the clock margin, or no deadline at all.
var ErrRetentionUnsafe = errors.New("retention does not cover the reader's pass deadline")

// Options are the protocol's time parameters and what a run does.
type Options struct {
	// GC runs garbage collection. Without it a run is a read-only diagnosis
	// that reports what a collection would delete and mutates nothing.
	GC bool
	// VerifyChecksums hashes every distinct object a retained generation lists
	// and compares it with the digest and size of every retained entry naming it.
	VerifyChecksums bool
	// RetentionGrace is G: how long after a collector retired a superseded
	// generation (or a lost-record recovery) its objects stay protected.
	RetentionGrace time.Duration
	// ReaderDeadline is R_max, the federated reader's pass deadline.
	ReaderDeadline time.Duration
	// ClockMargin is C: drift allowed between readers, database clock and S3.
	ClockMargin time.Duration
	// OrphanAge is A: how far behind the database clock the collector raises
	// the record's horizon.
	OrphanAge time.Duration
}

// Validate refuses options under which a reader could lose an object it scans:
// G must be at least R_max + C, summed without signed-integer overflow.
func (o Options) Validate() error {
	switch {
	case o.ReaderDeadline <= 0:
		return fmt.Errorf("reader pass deadline %s must be positive: %w", o.ReaderDeadline, ErrRetentionUnsafe)
	case o.ClockMargin < 0:
		return fmt.Errorf("clock margin %s must not be negative", o.ClockMargin)
	case o.OrphanAge <= 0:
		return fmt.Errorf("orphan age %s must be positive: a horizon at the database clock would refuse every publication in flight", o.OrphanAge)
	}
	floor, ok := AddDurations(o.ReaderDeadline, o.ClockMargin)
	if !ok {
		return fmt.Errorf("the reader pass deadline %s plus the clock margin %s overflows a duration: %w",
			o.ReaderDeadline, o.ClockMargin, ErrRetentionUnsafe)
	}
	if o.RetentionGrace < floor {
		return fmt.Errorf("retention grace %s is below the reader pass deadline %s plus the clock margin %s: %w",
			o.RetentionGrace, o.ReaderDeadline, o.ClockMargin, ErrRetentionUnsafe)
	}
	return nil
}

// AddDurations answers a + b for non-negative a and b, and false when the sum
// overflows time.Duration.
func AddDurations(a, b time.Duration) (time.Duration, bool) {
	if a < 0 || b < 0 || a > math.MaxInt64-b {
		return 0, false
	}
	return a + b, true
}

// SchemaEnumerator answers the schemas a run visits.
type SchemaEnumerator interface {
	SchemaIDs(ctx context.Context) ([]int16, error)
}

// Reconciler runs manifest-reconcile over every schema its enumerator answers.
type Reconciler struct {
	DB      *sql.DB
	Tables  publication.Tables
	Layout  manifestv2.Layout
	Objects ObjectStore
	// Locker is the writer schema lock used under --gc for liveness only.
	// Diagnosis never takes a lock (on Aurora DSQL a lock writes a lease row).
	Locker  cdc.SchemaLocker
	Schemas SchemaEnumerator
	Logger  *zap.Logger
	Opts    Options
	// Storage is the target this run lists and deletes in, checked against
	// Objects (when Objects implements Destination) and the deployment's
	// PostgreSQL storage binding before any collection deletes anything.
	Storage publication.StorageTarget

	storageFault *Finding
}

// Run visits every schema: a collection under the schema's lock, a diagnosis
// without it. Per-schema failures are recorded in SchemaReport.Err so the
// remaining schemas still run.
func (r *Reconciler) Run(ctx context.Context) (Report, error) {
	if err := r.Opts.Validate(); err != nil {
		return Report{}, fmt.Errorf("manifest-reconcile options: %w", err)
	}
	if r.Opts.GC && r.Locker == nil {
		return Report{}, errors.New("manifest-reconcile --gc needs the schema lock cdc-flush and cdc-init take")
	}
	fault, err := r.checkStorage(ctx)
	if err != nil {
		return Report{}, err
	}
	r.storageFault = fault
	ids, err := r.Schemas.SchemaIDs(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("enumerate schemas: %w", err)
	}
	var report Report
	for _, id := range ids {
		if !r.Opts.GC {
			report.Schemas = append(report.Schemas, r.diagnose(ctx, id))
			continue
		}
		report.Schemas = append(report.Schemas, r.underLock(ctx, id))
	}
	return report, nil
}

func (r *Reconciler) diagnose(ctx context.Context, id int16) SchemaReport {
	rep := r.schema(ctx, id)
	r.log(rep)
	return rep
}

func (r *Reconciler) underLock(ctx context.Context, id int16) SchemaReport {
	held, release, ok, err := r.Locker.TryLock(ctx, id)
	if err != nil {
		return SchemaReport{SchemaID: id, Err: fmt.Errorf("take schema %d's lock: %w", id, err)}
	}
	if !ok {
		return SchemaReport{SchemaID: id, Skipped: true}
	}
	defer release()
	rep := r.schema(held, id)
	if lost := cdc.WithLockLoss(held, rep.Err); lost != nil {
		rep.Err = lost
	}
	r.log(rep)
	return rep
}

func (r *Reconciler) log(rep SchemaReport) {
	if r.Logger == nil {
		return
	}
	fields := []zap.Field{
		zap.Int16("schema_id", rep.SchemaID), zap.String("state", string(rep.State)),
		zap.Int64("epoch", rep.Epoch), zap.Int64("generation", rep.Generation),
		zap.Int("findings", len(rep.Findings)), zap.String("gc_refused", rep.Refused),
	}
	if rep.Collected != nil {
		fields = append(fields, zap.Int("deleted_objects", len(rep.Collected.Objects)),
			zap.Int("deleted_manifests", len(rep.Collected.Manifests)), zap.Int("forgotten", len(rep.Collected.Forgotten)))
	}
	if rep.Err != nil {
		r.Logger.Error("manifest-reconcile schema pass failed", append(fields, zap.Error(rep.Err))...)
		return
	}
	r.Logger.Info("manifest-reconcile schema pass", fields...)
}
