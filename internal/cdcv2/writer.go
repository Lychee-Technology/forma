// Package cdcv2 is the writer half of Protocol V2 for cdc-flush, cdc-init
// and the compactor (docs/publication-protocol-v2.md, "The commit boundary"
// and "Writers"): it stages parquet objects and an immutable manifest
// (internal/manifestv2) before any transaction, then moves the schema's
// publication record (internal/publication) with compare-and-set and, in the
// same PostgreSQL transaction, marks the change_log entries the generation
// holds. A cdc-init publishes an init generation under a new lineage with no
// marks, then hands its exported entries off in transactions fenced on that
// lineage. A compaction publishes a compact generation, which replaces the
// files it merged and marks nothing.
//
// It is the one production package that moves a publication record by
// publishing (TestOnlyTheProtocolOwnersMoveAPublicationRecord in
// internal/publication).
//
// What turns rows into parquet is the caller's: a DeltaStager for a flush, a
// BaseExporter for an init and a CompactStager for a compaction. They write
// fresh write-once keys of the layout and answer the manifest entries; this
// package owns everything from the record read to the last handoff commit.
package cdcv2

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// Defaults of the Config fields left zero. The protocol leaves T_pub and W to
// the writer child (docs/publication-protocol-v2.md, "Open questions"): T_pub
// is the proposed 30 s, well under DSQL's five-minute transaction limit, and
// W one hour, which has to cover the longest staging, a cdc-init export.
const (
	DefaultPublishTimeout = 30 * time.Second
	DefaultStagingBudget  = time.Hour
	DefaultAttempts       = 5
	DefaultRestarts       = 3
	DefaultBatchSize      = 10000
	DefaultInitBatchSize  = 10000
)

// DSQLGenerationLimit is the most change_log entries one publication
// transaction marks on Aurora DSQL, and HandoffChunk the most one handoff
// transaction marks on either engine. DSQL caps a transaction at 3,000
// modified rows; a publication modifies three rows of its own besides its
// marks, so even if an update of flushed_at, a primary-key column, counted
// twice, a generation stays at 2,003 (docs/publication-protocol-v2.md, "DSQL
// budgets"). On PostgreSQL a flush generation keeps the whole batch.
const (
	DSQLGenerationLimit = 1000
	HandoffChunk        = 1000
)

// MaxMarkEntries is the most entries one mark statement binds: it binds
// flushed_at and the schema id, then a row id and a version per entry, and a
// PostgreSQL statement takes at most 65,535 bind parameters. A PostgreSQL
// flush generation is the whole batch, marked in its publication's one
// statement, so New refuses a larger PostgreSQL BatchSize: such a batch would
// stage, then roll back at every attempt. A DSQL generation and a handoff
// chunk stay far below it.
const MaxMarkEntries = (65535 - 2) / 2

// ErrAbandoned reports a publication the writer gave up on as the protocol
// says it must: its staging expired, the record moved in a way it may not
// rebase over (a cdc-init or another epoch for a flush, a flush for an init),
// or a lost commit answer can no longer be resolved. Nothing it staged is
// visible, and the entries it would have marked stay dirty. A flush selects
// the unflushed entries again by itself; an init is run again by its caller.
// A publication whose COMMIT went without an answer is abandoned only once
// the writer has settled that the COMMIT can no longer land (ErrUnresolved).
var ErrAbandoned = errors.New("publication abandoned")

// ErrEpochChanged reports a record whose epoch is not the one the process
// pinned when it loaded the schema: the schema was dropped, and maybe
// re-created, since. The writer fails closed.
var ErrEpochChanged = errors.New("publication epoch changed since the schema was loaded")

// ErrAttemptsExhausted reports a publication still neither committed nor
// abandoned after Config.Attempts transactions. When a COMMIT of it went
// without an answer, the writer has settled first that it can no longer land
// (ErrUnresolved).
var ErrAttemptsExhausted = errors.New("publication attempts exhausted")

// Config is one deployment's V2 writer.
type Config struct {
	// DB is the PostgreSQL or Aurora DSQL pool the record, the history and
	// change_log live on.
	DB     *sql.DB
	Engine forma.DatabaseEngine
	// EntityMain names entity_main; the publication tables are named after
	// it (publication.TablesFor), and cdc-init exports its live rows.
	EntityMain string
	// ChangeLog names the change log a flush selects and marks, and an init
	// hands off in. A writer that only compacts marks nothing and may leave
	// it empty; Flush, Init and ResumeInit refuse to run without it.
	ChangeLog string
	// Layout builds the object keys, Store holds them write-once, and Cache
	// answers the verified parent manifest a flush appends to. A Store that
	// is a manifestv2.S3Store over an *s3.Client also names the deployment's
	// storage target, which the writer binds, or checks against the binding,
	// before its first staging (ensureStorage).
	Layout manifestv2.Layout
	Store  manifestv2.Store
	Cache  *manifestv2.Cache
	// PublishTimeout is T_pub, the deadline of one publication or handoff
	// transaction. It is for liveness only: a COMMIT already sent may land
	// after it, which is why a lost answer goes through Resolve.
	PublishTimeout time.Duration
	// StagingBudget is W: how long after its staging began a publication may
	// still commit (publication.Next.StagingBudget).
	StagingBudget time.Duration
	// Attempts bounds the transactions one generation is sent in (resends
	// and rebases), and Restarts how often a flush selects its entries again
	// after an abandoned publication.
	Attempts, Restarts int
	// BatchSize is how many unflushed entries a flush selects, and
	// InitBatchSize how many live rows one init export snapshot reads.
	BatchSize, InitBatchSize int
	// Failpoint, when set, is called at every boundary a crash or a fault
	// can hit (Point). An error it answers fails the step there, as the
	// fault would. Tests inject faults through it; production leaves it nil.
	Failpoint func(context.Context, Point) error
}

// Writer publishes the generations of one deployment's schemas.
type Writer struct {
	cfg       Config
	tables    publication.Tables
	changeLog string
	main      string
	dsql      bool
	storage   storageGate
}

// New checks cfg, fills its defaults and answers its writer.
func New(cfg Config) (*Writer, error) {
	switch {
	case cfg.DB == nil:
		return nil, errors.New("a V2 writer needs a database")
	case cfg.Store == nil || cfg.Cache == nil:
		return nil, errors.New("a V2 writer needs a manifest store and a manifest cache")
	case cfg.EntityMain == "":
		return nil, errors.New("a V2 writer needs the entity_main table name")
	}
	cfg.PublishTimeout = orDefault(cfg.PublishTimeout, DefaultPublishTimeout)
	cfg.StagingBudget = orDefault(cfg.StagingBudget, DefaultStagingBudget)
	cfg.Attempts = orDefault(cfg.Attempts, DefaultAttempts)
	cfg.Restarts = orDefault(cfg.Restarts, DefaultRestarts)
	cfg.BatchSize = orDefault(cfg.BatchSize, DefaultBatchSize)
	cfg.InitBatchSize = orDefault(cfg.InitBatchSize, DefaultInitBatchSize)
	w := &Writer{
		cfg:       cfg,
		tables:    publication.TablesFor(cfg.EntityMain),
		changeLog: sqlutil.SanitizeIdentifier(cfg.ChangeLog),
		main:      sqlutil.SanitizeIdentifier(cfg.EntityMain),
		dsql:      cfg.Engine.Resolved() == forma.EngineAuroraDSQL,
	}
	if limit := w.generationLimit(); limit > MaxMarkEntries {
		return nil, fmt.Errorf("a V2 writer on PostgreSQL marks a flush batch in one statement, so BatchSize %d must be at most %d (MaxMarkEntries)", limit, MaxMarkEntries)
	}
	return w, nil
}

func orDefault[T int | time.Duration](v, def T) T {
	if v <= 0 {
		return def
	}
	return v
}

// generationLimit is the most entries one flush generation marks.
func (w *Writer) generationLimit() int {
	if w.dsql {
		return min(w.cfg.BatchSize, DSQLGenerationLimit)
	}
	return w.cfg.BatchSize
}

// readRecord reads schemaID's record and refuses one of another epoch than
// pin (0 pins nothing).
func (w *Writer) readRecord(ctx context.Context, schemaID int16, pin int64) (publication.Record, error) {
	snap, err := publication.Read(ctx, publication.DB(w.cfg.DB), w.tables, schemaID)
	if err != nil {
		return publication.Record{}, fmt.Errorf("read the publication record of schema %d: %w", schemaID, err)
	}
	if pin != 0 && snap.Epoch != pin {
		return publication.Record{}, fmt.Errorf("schema %d: the record is at epoch %d, the process loaded epoch %d: %w", schemaID, snap.Epoch, pin, ErrEpochChanged)
	}
	return snap.Record, nil
}

// now reads the database clock, the clock staged_at and the marks use.
func (w *Writer) now(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) (int64, error) {
	var ms int64
	if err := q.QueryRowContext(ctx, "SELECT "+sqlgen.ServerTimeMillisSQL).Scan(&ms); err != nil {
		return 0, fmt.Errorf("read the database clock: %w", err)
	}
	return ms, nil
}

// needChangeLog refuses op on a writer configured without a change log.
func (w *Writer) needChangeLog(op string) error {
	if w.cfg.ChangeLog == "" {
		return fmt.Errorf("a V2 %s needs the change_log table name", op)
	}
	return nil
}

// fail runs the failpoint at p, if one is set.
func (w *Writer) fail(ctx context.Context, p Point) error {
	if w.cfg.Failpoint == nil {
		return nil
	}
	if err := w.cfg.Failpoint(ctx, p); err != nil {
		return fmt.Errorf("failpoint %s: %w", p, err)
	}
	return nil
}
