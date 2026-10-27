package sqlutil

// UUIDTextPattern matches exactly the text uuid.Parse (google/uuid) accepts,
// so a SQL census can state the read path's uuid rule: a uuid attribute in
// eav_data.value_text, or in a parquet VARCHAR, reads through uuid.Parse,
// and a value it refuses fails every read of the row (#733). Neither
// engine's own cast is that rule: Postgres refuses the urn:uuid: form, and
// DuckDB both refuses it and admits misplaced hyphens.
//
// uuid.Parse decides by byte length: 36 is the hyphenated form, 45 adds a
// case-insensitive "urn:uuid:", 32 is bare hex, and 38 is the hyphenated
// form between any two bytes, which it never inspects (not only braces). In
// valid UTF-8 those two bytes are ASCII: a lead byte first would put a
// continuation byte where a hex digit must be, and a multi-byte last
// character would end in one. So the pattern, which counts characters,
// admits ASCII there. Postgres text cannot hold NUL, so the class starts at
// \x01. The syntax is common to Postgres AREs and RE2 (DuckDB, Go).
const UUIDTextPattern = `^(` +
	`[\x01-\x7f]` + uuidHyphenated + `[\x01-\x7f]` +
	`|[uU][rR][nN]:[uU][uU][iI][dD]:` + uuidHyphenated +
	`|` + uuidHyphenated +
	`|[0-9a-fA-F]{32}` +
	`)$`

const uuidHyphenated = `[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`
