package sqlgen

// BoolTruthiness renders the one read-side rule every SQL reader derives a
// bool from (#404): the stored numeric image is the nearest of 0/1, so float
// noise around either end does not flip the answer. It is the SQL image of
// transform.float64ToBool's threshold; the two must stay in lockstep. Shared
// by the DuckDB EAV pivot and main-column normalisation, the PG-EAV
// comparison, and the CDC export leg (cdc.castEAVValue / castMainValue).
//
// Any further shared SQL spelling of a value-type read rule belongs beside
// this one, not in the payload/IR files that consume it.
func BoolTruthiness(expr string) string {
	return "(" + expr + " > 0.5)"
}
