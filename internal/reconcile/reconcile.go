// Package reconcile is manifest-reconcile under Protocol V2
// (docs/publication-protocol-v2.md, "Garbage collection and reader
// lifetime"; docs/manifest-reconcile.md): a read-only diagnosis of each
// schema's publication, and, under --gc, the collector that deletes what no
// reader can still scan.
//
// Both read the same thing in the protocol's collection order: the schema's
// objects under its prefix, then, in one snapshot, its record and its
// retained and expired generations, then every manifest those generations
// name, each verified against its history row before a key it lists is
// trusted. The diagnosis writes nothing, to PostgreSQL or S3. The collector
// raises the record's horizon before it lists, retires the superseded
// generations it saw after that snapshot ended, and deletes exactly what
// publication.PlanCollection plans, step by step, so that a crash at any
// point leaves a state the next pass plans from again.
//
// There is no repair. Under V2 an object no generation lists is staging that
// never committed or garbage, never lost data, and a manifest or an object a
// committed generation lists cannot be rebuilt from S3: the recovery is a
// re-init (cdc-init), which publishes a new generation from PostgreSQL.
package reconcile

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
	// GC runs garbage collection. Without it a run is the read-only
	// diagnosis: it reports what a collection would delete and changes
	// nothing.
	GC bool
	// VerifyChecksums hashes every object a retained generation lists and
	// compares it with the digest and size its manifest lists.
	VerifyChecksums bool
	// RetentionGrace is G: how long after a collector retired a superseded
	// generation its manifest and objects stay.
	RetentionGrace time.Duration
	// ReaderDeadline is R_max, the federated reader's pass deadline
	// (federated.DefaultPublishedPassDeadline).
	ReaderDeadline time.Duration
	// ClockMargin is C: the drift between the readers' monotonic clocks,
	// the database clock and S3's LastModified.
	ClockMargin time.Duration
	// OrphanAge is A: how far behind the database clock the collector
	// raises the horizon. Any value is safe; one below the writers' staging
	// budget plus C refuses publications still inside their budget.
	OrphanAge time.Duration
}

// Validate refuses options under which a reader could lose an object it
// scans: G must be at least R_max + C (the retention rule's premise).
func (o Options) Validate() error {
	switch {
	case o.ReaderDeadline <= 0:
		return fmt.Errorf("reader pass deadline %s must be positive: %w", o.ReaderDeadline, ErrRetentionUnsafe)
	case o.ClockMargin < 0:
		return fmt.Errorf("clock margin %s must not be negative", o.ClockMargin)
	case o.RetentionGrace < o.ReaderDeadline+o.ClockMargin:
		return fmt.Errorf("retention grace %s is below the reader pass deadline %s plus the clock margin %s: %w",
			o.RetentionGrace, o.ReaderDeadline, o.ClockMargin, ErrRetentionUnsafe)
	case o.OrphanAge <= 0:
		return fmt.Errorf("orphan age %s must be positive: a horizon at the database clock would refuse every publication in flight", o.OrphanAge)
	}
	return nil
}

// SchemaEnumerator answers the schemas a run visits.
type SchemaEnumerator interface {
	SchemaIDs(ctx context.Context) ([]int16, error)
}

// Reconciler runs manifest-reconcile over every schema its enumerator
// answers.
type Reconciler struct {
	// DB holds the publication tables Tables names.
	DB      *sql.DB
	Tables  publication.Tables
	Layout  manifestv2.Layout
	Objects ObjectStore
	// Locker is the schema lock cdc-flush and cdc-init take. It keeps a
	// pass apart from them for liveness only: safety comes from the
	// horizon and the retention grace.
	Locker  cdc.SchemaLocker
	Schemas SchemaEnumerator
	Logger  *zap.Logger
	Opts    Options
}

// Run visits every schema under its lock. A schema whose pass fails reports
// the failure in its SchemaReport, and the others still run; only options
// that fail Validate or an enumeration that fails stop the run.
func (r *Reconciler) Run(ctx context.Context) (Report, error) {
	if err := r.Opts.Validate(); err != nil {
		return Report{}, fmt.Errorf("manifest-reconcile options: %w", err)
	}
	ids, err := r.Schemas.SchemaIDs(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("enumerate schemas: %w", err)
	}
	var report Report
	for _, id := range ids {
		report.Schemas = append(report.Schemas, r.underLock(ctx, id))
	}
	return report, nil
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
