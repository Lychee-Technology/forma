package sqlgen

// The keyset renderers, exported to the external test package so its hot-leg
// parity tests can hold sortkey's Go order against the SQL these render
// (#853). sortkey imports sqlgen, so those tests cannot live in this package.
var (
	GenerateKeysetWhereClause = generateKeysetWhereClause
	BuildKeysetOrderBy        = buildKeysetOrderBy
	BuildNonKeysetOrderBy     = buildNonKeysetOrderBy
)
