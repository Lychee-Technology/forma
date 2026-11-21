package reconcile

import "github.com/lychee-technology/forma/internal/publication"

// Safe operator recovery actions for each finding kind. None rebuilds an
// inventory from S3 or restores a stale publication record or history row from
// backup; recovery is either cdc-init (for S3 loss/corruption) or fresh-epoch
// recovery via delete-record + init-db + cdc-init (for DB metadata faults).
const (
	actionNoRecord = "run init-db under the schema lock to recover the record at a fresh epoch " +
		"(docs/publication-protocol-v2.md, \"A lost record\"); GC deletes nothing until it exists"
	actionRecordInvalid = "never restore an older record row from backup (stale pointers can silently serve old or deleted rows). " +
		"Delete the schema's row from the publication record table, run init-db to recover it at a fresh epoch (arming the recovery fence), " +
		"restart readers/writers, and run cdc-init (docs/manifest-reconcile.md)"
	actionMissingLifecycle = "stop readers and writers for the schema, insert its lifecycle epoch row at or above the highest epoch " +
		"in the record/history, then run fresh-epoch recovery (docs/publication-protocol-v2.md, \"A lost record\"); GC deletes nothing until fixed"
	actionStaleLifecycle = "do not restore a backup lifecycle row; verify the record's epoch and update the lifecycle epoch row " +
		"to match the record's epoch, or run fresh-epoch recovery if the record was restored (docs/manifest-reconcile.md)"
	actionRecordBehindLifecycle = "treat the record as invalid: " + actionRecordInvalid
	actionUnboundStorage        = "confirm the S3 client's effective bucket URL (--s3-endpoint or AWS_ENDPOINT_URL_S3/AWS_ENDPOINT_URL or --s3-region, " +
		"with --s3-use-path matching the writers), --s3-bucket and --data-prefix match the writers, then run manifest-reconcile --bind-storage once " +
		"(docs/manifest-reconcile.md); GC deletes nothing until bound"
	actionRebindStorage = "run manifest-reconcile with the bound writers' base --s3-endpoint (never the displayed bucket URL), --s3-use-path, " +
		"--s3-bucket, --data-prefix and environment (clear any stray AWS_ENDPOINT_URL_S3/AWS_ENDPOINT_URL). " +
		"Only if the writers genuinely moved, delete the binding row by hand and re-run --bind-storage"
	actionAwaitingInit      = "run cdc-init for the schema: readers fail closed until its first V2 generation publishes"
	actionMissingGeneration = "re-initialize the schema with cdc-init (the lost manifest cannot be rebuilt from S3); " +
		"GC deletes nothing until the lost generation expires after retirement and grace"
	actionHistoryMismatch = "the current generation's history row is missing or disagrees with the record. Do not restore a history row from backup " +
		"or hand-edit the record; Publish and Reset fail closed on this state. Recover at a fresh epoch: delete the schema's row from the " +
		"publication record table, run init-db (arms the monotonic recovery fence), restart readers/writers, and run cdc-init (docs/manifest-reconcile.md)"
	actionHistoryGap = "do not restore generation history rows from backup (a copy with superseded_at=0 would stay retained forever). " +
		"Recover at a fresh epoch (delete the record row, run init-db, then cdc-init) or wait until the gap is resolved; GC deletes nothing while a gap exists"
	actionMissingRetained = "no action for new reads (they scan the current generation); GC deletes nothing until this generation retires and expires"
	actionInvalidManifest = "restore the manifest object from a copy matching PostgreSQL's manifest_sha256; for the current generation, " +
		"re-initialize with cdc-init to restore reads. GC deletes nothing while any manifest fails verification"
	actionForeignManifest = "run manifest-reconcile with the --data-prefix the writers publish under; GC deletes nothing under a foreign prefix"
	actionLostCurrent     = "re-initialize the schema with cdc-init: reads fail closed while the current generation lists a missing object"
	actionLostSuperseded  = "no action for new reads (they scan the current generation); pinned readers fail closed and the generation expires after grace"
	actionCorrupt         = "integrity fault: federated readers do not hash parquet objects, so reads may return incorrect results or scan errors. " +
		"If the current generation lists the object, re-initialize with cdc-init (compaction refuses corrupt sources, #347); " +
		"otherwise it leaves when its superseded generation expires"
	actionUnrecognized = "GC never deletes keys no V2 writer stages (V1 leftovers or foreign objects); remove by hand once verified"
)

func recordFinding(kind FindingKind, err error) Finding {
	action := actionNoRecord
	if kind == RecordInvalid {
		action = actionRecordInvalid
	}
	return Finding{Kind: kind, Detail: err.Error(), Action: action}
}

func generationFinding(kind FindingKind, g publication.Generation, current bool, detail string) Finding {
	f := Finding{Kind: kind, Epoch: g.Epoch, Generation: g.Generation, Current: current, Key: g.ManifestKey, Detail: detail}
	switch kind {
	case MissingGeneration:
		f.Action = actionMissingGeneration
	case HistoryMismatch:
		f.Action = actionHistoryMismatch
	case HistoryGap:
		f.Action = actionHistoryGap
	case MissingRetainedManifest:
		f.Action = actionMissingRetained
	case InvalidManifest:
		f.Action = actionInvalidManifest
	case ForeignManifest:
		f.Action = actionForeignManifest
	}
	return f
}

func absentManifest(kind FindingKind, g publication.Generation, current bool) Finding {
	f := generationFinding(kind, g, current, "the manifest object is missing")
	f.absent = true
	return f
}

func objectFinding(kind FindingKind, g publication.Generation, current bool, key, detail string) Finding {
	f := Finding{
		Kind:       kind,
		Epoch:      g.Epoch,
		Generation: g.Generation,
		Current:    current,
		Key:        key,
		Detail:     detail,
		absent:     kind == LostObject,
	}
	switch {
	case kind == CorruptObject:
		f.Action = actionCorrupt
	case current:
		f.Action = actionLostCurrent
	default:
		f.Action = actionLostSuperseded
	}
	return f
}
