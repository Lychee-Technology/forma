package cdcv2

import "errors"

// Point is a boundary of a publication that a crash or a fault can hit
// (Config.Failpoint). Each is passed before the step it names runs, except
// PointCommitAnswer, which follows a COMMIT that succeeded.
type Point string

const (
	// PointStageObjects precedes the DeltaStager or BaseExporter call that
	// writes a generation's parquet objects.
	PointStageObjects Point = "stage-objects"
	// PointStageManifest precedes the write-once put of a manifest object,
	// the first staging and every rebase's.
	PointStageManifest Point = "stage-manifest"
	// PointBegin precedes BEGIN of a publication transaction.
	PointBegin Point = "begin"
	// PointPublish precedes the transaction's first statements: the storage
	// binding's check (storage.go) and the compare-and-set.
	PointPublish Point = "publish"
	// PointMarks follows the compare-and-set and precedes the marks.
	PointMarks Point = "marks"
	// PointCommit precedes COMMIT. An error wrapping ErrCommitAnswerLost
	// here stands for a COMMIT whose answer was lost and which did not land:
	// the transaction is rolled back and the writer resolves the outcome.
	PointCommit Point = "commit"
	// PointCommitAnswer follows a COMMIT that landed, of a publication or
	// of a handoff transaction. Any error here stands for its lost answer:
	// the writer cannot tell that it committed, so it resolves a
	// publication's outcome and sends a handoff chunk again.
	PointCommitAnswer Point = "commit-answer"
	// PointHandoff precedes BEGIN of each cdc-init handoff transaction.
	PointHandoff Point = "handoff"
	// PointFence precedes BEGIN of each fence transaction, which settles a
	// COMMIT without an answer before the writer gives its publication up.
	// A COMMIT still on its way may land there, as late as it can.
	PointFence Point = "fence"
)

// ErrCommitAnswerLost is the fault a failpoint answers at PointCommit to
// stand for a COMMIT sent without an answer. A real one is a COMMIT error the
// server did not answer: a dropped connection or the T_pub deadline.
var ErrCommitAnswerLost = errors.New("commit answer lost")
