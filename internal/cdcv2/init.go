package cdcv2

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// BaseExporter writes a cdc-init export's parquet objects.
type BaseExporter interface {
	// ExportBase writes the live rows req.RowIDs, reading their content on
	// snap, to the base object req.Key, once, and answers its manifest
	// entry. snap is the REPEATABLE READ READ ONLY transaction whose first
	// statement read the rows' unflushed entry versions, so each row's
	// exported content and its listed version come from one snapshot. The
	// exporter neither commits nor rolls snap back.
	ExportBase(ctx context.Context, snap *sql.Tx, req BaseRequest) (manifestv2.File, error)
}

// BaseRequest is one snapshot's worth of an init export.
type BaseRequest struct {
	SchemaID int16
	Epoch    int64
	Key      string
	RowIDs   []uuid.UUID
}

// InitReport is what an Init published and handed off.
type InitReport struct {
	// Generation is the init generation's record.
	Generation publication.Record
	// Rows counts the live rows exported, Files the base objects, and
	// Entries the exported rows that had an unflushed entry, the handoff's
	// candidates.
	Rows, Files, Entries int
	// Marked counts the entries the handoff's answered transactions marked.
	// One whose row moved past its listed version since the export stays
	// dirty.
	Marked int
	// Unconfirmed counts the entries of handoff chunks a COMMIT without an
	// answer may have marked: the chunk was sent again, and the answered
	// transaction did not mark them, because that COMMIT had landed, a
	// flush had marked them, or their rows had moved. They are flushed or
	// dirty as change_log says, and Marked does not count them: the handoff
	// marked at least Marked entries and at most Marked + Unconfirmed.
	Unconfirmed int
	// HandoffStopped says the handoff stopped at ErrLineageReplaced: a later
	// cdc-init or a drop replaced the lineage, and the entries it had not
	// marked stay dirty for the next flush.
	HandoffStopped bool
	// Resends counts publications sent again with the same token, and
	// Rebases those restaged onto a new head.
	Resends, Rebases int
}

// Init re-onboards schemaID: it exports every live row with the version of
// its unflushed entry, read in the row's snapshot, publishes an init
// generation of those base files under a new lineage with no marks, then
// hands the exported entries off at exactly those versions in transactions
// of at most HandoffChunk entries, each fenced by HoldLineage. It never
// clears change_log.
//
// It runs from UNPUBLISHED, AWAITING_INIT or PUBLISHED (a re-initialization),
// and refuses a DROPPED schema. pin is the epoch the process loaded the
// schema at (0 pins nothing). A publication the rebase rules abandon (a
// flush committed since the export began, a drop, an expired staging) leaves
// nothing visible and returns ErrAbandoned; running Init again starts a fresh
// export. A COMMIT of the publication that went without an answer is settled
// before Init gives up: one that landed, however late, is taken as committed
// and handed off. One the writer could not settle returns ErrUnresolved before
// any handoff: its UnresolvedError keeps the exported entries, and ResumeInit
// settles it later and, if the init committed, runs the handoff.
func (w *Writer) Init(ctx context.Context, schemaID int16, pin int64, exp BaseExporter) (InitReport, error) {
	var rep InitReport
	if err := w.needChangeLog("init"); err != nil {
		return rep, err
	}
	cur, err := w.readRecord(ctx, schemaID, pin)
	if err != nil {
		return rep, fmt.Errorf("init: %w", err)
	}
	if cur.State == publication.Dropped {
		return rep, fmt.Errorf("init schema %d at epoch %d: %w", schemaID, cur.Epoch, publication.ErrDropped)
	}
	if err := w.checkStorage(ctx); err != nil {
		return rep, fmt.Errorf("init schema %d: %w", schemaID, err)
	}
	stagedAt, err := w.now(ctx, w.cfg.DB)
	if err != nil {
		return rep, fmt.Errorf("init schema %d: %w", schemaID, err)
	}
	files, entries, err := w.export(ctx, cur, exp, &rep)
	if err != nil {
		return rep, err
	}
	lineage := uuid.New()
	p := &pub{
		kind:    publication.KindInit,
		cur:     cur,
		handoff: entries,
		build: func(ctx context.Context, cur publication.Record) (publication.Next, error) {
			h, err := manifestv2.Stage(cur, publication.KindInit, lineage, uuid.New())
			if err != nil {
				return publication.Next{}, err
			}
			m, err := manifestv2.NewInit(h, files)
			if err != nil {
				return publication.Next{}, err
			}
			return w.stageManifest(ctx, m, stagedAt)
		},
	}
	if p.next, err = p.build(ctx, cur); err != nil {
		return rep, fmt.Errorf("init schema %d: %w", schemaID, err)
	}
	rep.Generation, err = w.publish(ctx, p)
	rep.Resends, rep.Rebases = p.resends, p.rebases
	if err != nil {
		return rep, fmt.Errorf("init: %w", err)
	}
	if err := w.handoff(ctx, rep.Generation, entries, &rep); err != nil {
		return rep, fmt.Errorf("init schema %d generation %d: %w", schemaID, rep.Generation.Generation, err)
	}
	return rep, nil
}

// versionsSQL is an export snapshot's first statement: the next live rows of
// the schema after the cursor, by row id, each with the version of its
// unflushed entry, NULL when every entry of the row is flushed. Keyset by row
// id, so a row deleted below the cursor cannot shift the window and drop a
// live row (#462). The change_log primary key (schema_id, row_id,
// flushed_at) gives a row at most one unflushed entry.
const versionsSQL = `SELECT m.ltbase_row_id, cl.changed_at
	FROM %s m
	LEFT JOIN %s cl ON cl.schema_id = m.ltbase_schema_id AND cl.row_id = m.ltbase_row_id AND cl.flushed_at = 0
	WHERE m.ltbase_schema_id = $1 AND m.ltbase_deleted_at IS NULL AND m.ltbase_row_id > $2
	ORDER BY m.ltbase_row_id
	LIMIT $3`

// export runs the export in snapshots of Config.InitBatchSize rows and
// answers the base files and the handoff's entries.
func (w *Writer) export(ctx context.Context, cur publication.Record, exp BaseExporter, rep *InitReport) ([]manifestv2.File, []Entry, error) {
	files := []manifestv2.File{}
	var entries []Entry
	after := uuid.Nil
	for {
		file, rows, listed, err := w.exportSnapshot(ctx, cur, exp, after)
		if err != nil {
			return nil, nil, fmt.Errorf("init schema %d: export the live rows after %s: %w", cur.SchemaID, after, err)
		}
		if len(rows) == 0 {
			return files, entries, nil
		}
		files = append(files, file)
		entries = append(entries, listed...)
		rep.Rows += len(rows)
		rep.Files++
		rep.Entries += len(listed)
		after = rows[len(rows)-1]
	}
}

// exportSnapshot exports the next batch after the cursor in one REPEATABLE
// READ READ ONLY snapshot: it reads the rows and their versions as the
// snapshot's first statement, then has exp write them from the same
// snapshot. It answers the file, the rows, and the entries to hand off.
func (w *Writer) exportSnapshot(ctx context.Context, cur publication.Record, exp BaseExporter, after uuid.UUID) (manifestv2.File, []uuid.UUID, []Entry, error) {
	snap, err := w.cfg.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return manifestv2.File{}, nil, nil, fmt.Errorf("begin the export snapshot: %w", err)
	}
	defer func() { _ = snap.Rollback() }()
	rows, entries, err := w.readVersions(ctx, snap, cur.SchemaID, after)
	if err != nil || len(rows) == 0 {
		return manifestv2.File{}, nil, nil, err
	}
	key, err := w.cfg.Layout.NewParquetKey(cur.SchemaID, cur.Epoch, manifestv2.TierBase)
	if err != nil {
		return manifestv2.File{}, nil, nil, err
	}
	if err := w.fail(ctx, PointStageObjects); err != nil {
		return manifestv2.File{}, nil, nil, err
	}
	file, err := exp.ExportBase(ctx, snap, BaseRequest{SchemaID: cur.SchemaID, Epoch: cur.Epoch, Key: key, RowIDs: rows})
	if err != nil {
		return manifestv2.File{}, nil, nil, fmt.Errorf("export %d rows to %s: %w", len(rows), key, err)
	}
	return file, rows, entries, nil
}

func (w *Writer) readVersions(ctx context.Context, snap *sql.Tx, schemaID int16, after uuid.UUID) ([]uuid.UUID, []Entry, error) {
	res, err := snap.QueryContext(ctx, fmt.Sprintf(versionsSQL, w.main, w.changeLog), schemaID, after, w.cfg.InitBatchSize)
	if err != nil {
		return nil, nil, fmt.Errorf("read the live rows and their unflushed versions: %w", err)
	}
	defer res.Close()
	var rows []uuid.UUID
	var entries []Entry
	for res.Next() {
		var id uuid.UUID
		var version sql.NullInt64
		if err := res.Scan(&id, &version); err != nil {
			return nil, nil, fmt.Errorf("scan a live row and its unflushed version: %w", err)
		}
		rows = append(rows, id)
		if version.Valid {
			entries = append(entries, Entry{RowID: id, Version: version.Int64})
		}
	}
	if err := res.Err(); err != nil {
		return nil, nil, fmt.Errorf("read the live rows and their unflushed versions: %w", err)
	}
	return rows, entries, nil
}
