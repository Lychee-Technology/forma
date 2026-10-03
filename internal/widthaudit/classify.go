package widthaudit

import "github.com/lychee-technology/forma"

// Class says what a finding means for the tiers, given what change_log
// records about the row.
type Class int

const (
	// ClassConsistent: every tier reads the same value. The row is either
	// pending (served hot, and re-exported under DOUBLE by its next flush),
	// never exported, or last exported at or after the cutover. Its value is
	// still outside the declared width, but #384 reads such values the same
	// way on every tier.
	ClassConsistent Class = iota
	// ClassStaleCandidate: a smallint/integer row last exported at an unknown
	// point relative to the #384 export change, because no cutover was
	// given. If that export predates #384, its parquet copy is NULL or
	// rounded.
	ClassStaleCandidate
	// ClassStaleExport: a smallint/integer row whose winning parquet copy
	// was written before the cutover. That copy was cast at declared width,
	// so the federated route serves NULL or a rounded value. The row is not
	// dirty, so its Postgres state has not changed since that export.
	ClassStaleExport
	// ClassBigIntOutOfContract: a bigint value outside [-2^53, 2^53] (the
	// range the float64 image eav_data keeps exactly) or not integral. A
	// value past 2^53 was rounded on the write, so no route can recover the
	// caller's value; one past int64 or non-integral is projected by every
	// DuckDB leg, hot included, through TRY_CAST(... AS BIGINT) to NULL or
	// a rounded value and is refused by the OLTP read. A re-flush
	// reproduces the same image, so only rewriting the value repairs it.
	// The write funnel refuses every such value, judging bigint by the
	// float64 image eav_data stores (#612, #590), so a finding predates
	// that check or was written around the funnel.
	//
	// A bigint bound to a double_* column is the same class (#618): the
	// column stores the same image under the same contract, the OLTP read
	// refuses the same values, and the same rewrite repairs it. Its
	// Finding names the column.
	ClassBigIntOutOfContract
)

// Classify places f given the cutover, the epoch millisecond from which
// cdc-flush ran the #384 export. A cutover of 0 means it is unknown.
func (f Finding) Classify(cutoverMillis int64) Class {
	if f.Declared == forma.ValueTypeBigInt {
		return ClassBigIntOutOfContract
	}
	if f.Pending || f.LastFlushedAt == 0 {
		return ClassConsistent
	}
	if cutoverMillis == 0 {
		return ClassStaleCandidate
	}
	if f.LastFlushedAt < cutoverMillis {
		return ClassStaleExport
	}
	return ClassConsistent
}

// Requeueable reports whether RequeueForFlush repairs the class: a re-export
// under the storage-width projection replaces the stale copy. A candidate is
// requeueable too, because re-exporting a row whose copy was already correct
// only rewrites the same values at a newer version.
func (c Class) Requeueable() bool {
	return c == ClassStaleCandidate || c == ClassStaleExport
}
