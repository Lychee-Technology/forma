package widthaudit

import "github.com/lychee-technology/forma"

// Class says what a finding means and how it is repaired. Every class is a
// stored value the declared type cannot hold, which only rewriting the
// attribute repairs: a re-flush re-exports the same value.
type Class int

const (
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
	ClassBigIntOutOfContract Class = iota
	// ClassBoundIntegerOutOfContract: a smallint or integer bound to a column
	// wider than its declared type holds an image that type cannot: a
	// fraction, a value past its range, NaN or an infinity (#708). The OLTP
	// read refuses the row with a consistency error, so an unrelated update
	// is blocked. The federated route reads the column through
	// TRY_CAST(<column> AS <declared>), as the CDC export does (#627): a
	// value past the range reads as absent and a fraction rounds half to
	// even. A re-flush reproduces that, so only rewriting the attribute
	// repairs the row.
	ClassBoundIntegerOutOfContract
	// ClassEAVIntegerOutOfContract: an EAV-only smallint or integer, or a
	// list item of one, holds an eav_data image its declared type cannot: a
	// fraction, a value past its range, NaN or an infinity (#729). The OLTP
	// read refuses the row with a consistency error, so an unrelated update
	// is blocked, and so does the federated read of a copy exported since
	// #384, which carries the same value at storage width. A copy exported
	// before #384 (ExportPredates) holds NULL or a rounded value instead,
	// which the federated route serves until the row is rewritten. Either
	// way the Postgres value is the defect, so a re-flush repairs nothing:
	// rewriting the attribute repairs the row, and the flush that follows
	// replaces any parquet copy.
	ClassEAVIntegerOutOfContract
)

// Classify places f by its declared type and storage.
func (f Finding) Classify() Class {
	switch {
	case f.Declared == forma.ValueTypeBigInt:
		return ClassBigIntOutOfContract
	case f.Column != "":
		return ClassBoundIntegerOutOfContract
	}
	return ClassEAVIntegerOutOfContract
}

// ExportPredates reports whether the row's newest parquet copy was written
// before cutoverMillis, the epoch millisecond from which cdc-flush ran the
// #384 storage-width export. That export cast a smallint/integer at
// declared width through TRY_CAST, so the copy holds NULL for an
// out-of-range value and a rounded one for a fraction, and the federated
// route serves it while the row is not dirty. A cutover of 0 means it is
// unknown, and the answer is false.
func (f Finding) ExportPredates(cutoverMillis int64) bool {
	return cutoverMillis > 0 && !f.Pending && f.LastFlushedAt > 0 && f.LastFlushedAt < cutoverMillis
}
