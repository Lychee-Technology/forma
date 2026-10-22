package uniquekey

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// The two key tables and the key table's row index take their names from the
// entity_main table they guard, so every deployment and every test that names
// its entity_main table names them too. sqlutil bounds each derived name to
// PostgreSQL's identifier limit, so the key and state tables of a long
// entity_main name stay two tables.
const (
	KeyTableSuffix   = "_unique_key"
	StateTableSuffix = "_unique_key_state"
	RowIndexSuffix   = "_unique_key_row_idx"
)

// KeyTable names the key table of entityMain: one entry per (live row,
// built key) whose attributes are all present. A schema-qualified name keeps
// its qualifier, and a quoted one its quotes.
func KeyTable(entityMain string) string {
	return sqlutil.SuffixedIdentifier(entityMain, KeyTableSuffix)
}

// StateTable names the built-key registry of entityMain.
func StateTable(entityMain string) string {
	return sqlutil.SuffixedIdentifier(entityMain, StateTableSuffix)
}

// RowIndex names the key table's row index. An index lives in its table's
// schema, so the name carries no qualifier and no quotes: the caller quotes
// it if it needs quoting.
func RowIndex(entityMain string) string { return sqlutil.SuffixedName(entityMain, RowIndexSuffix) }

// keyTableDDL is the key table. Uniqueness is the primary key on an ordinary
// table, a guard-row design that needs no partial index (#651), so it holds
// on Aurora DSQL too. The table is derived state: it is never in change_log,
// never flushed to S3 and never read by DuckDB.
const keyTableDDL = `CREATE TABLE IF NOT EXISTS %s (
		ltbase_schema_id SMALLINT NOT NULL,
		key_id           SMALLINT NOT NULL,
		key_value        TEXT     NOT NULL,
		ltbase_row_id    UUID     NOT NULL,
		PRIMARY KEY (ltbase_schema_id, key_id, key_value)
	)`

// keyRowIndexDDL serves a write's lookup of the entries one row holds.
const keyRowIndexDDL = `CREATE INDEX IF NOT EXISTS %s ON %s (ltbase_schema_id, ltbase_row_id)`

// stateTableDDL records each built key, the attribute list it was built for
// and where those attributes were stored when it was built.
const stateTableDDL = `CREATE TABLE IF NOT EXISTS %s (
		ltbase_schema_id SMALLINT NOT NULL,
		key_id           SMALLINT NOT NULL,
		attributes_hash  TEXT     NOT NULL,
		storage_hash     TEXT     NOT NULL,
		built_at         BIGINT   NOT NULL,
		PRIMARY KEY (ltbase_schema_id, key_id)
	)`

// Statements answers the key tables' DDL, the one source init-db and both
// e2e harnesses create them from (#440). Each argument is an identifier as
// the statement should spell it: the caller quotes it if it needs quoting.
func Statements(keyTable, rowIndex, stateTable string) []string {
	return []string{
		fmt.Sprintf(keyTableDDL, keyTable),
		fmt.Sprintf(keyRowIndexDDL, rowIndex, keyTable),
		fmt.Sprintf(stateTableDDL, stateTable),
	}
}

// StatementsFor answers Statements for an entity_main name that needs no
// quoting, as both e2e harnesses name it.
func StatementsFor(entityMain string) []string {
	return Statements(KeyTable(entityMain), RowIndex(entityMain), StateTable(entityMain))
}

// Attribute is one attribute of a key as the attributes hash sees it.
type Attribute struct {
	Name      string
	ValueType forma.ValueType
}

// EncodingVersion names the canonical encoding Encode implements. It is part
// of every attributes hash, so a change to the encoding, which bumps it, can
// never leave entries built under the old one looking current.
const EncodingVersion = "1"

// AttributesHash identifies what a key is built for: the encoding version,
// and the attribute names in order with their value types, which together
// decide every key value. A key whose declaration or attribute types changed
// under its id has another hash, which the build and the startup check
// refuse.
func AttributesHash(attrs []Attribute) string {
	var b strings.Builder
	fmt.Fprintf(&b, "encoding %s;", EncodingVersion)
	for _, a := range attrs {
		fmt.Fprintf(&b, "%d:%s%d:%s", len(a.Name), a.Name, len(a.ValueType), a.ValueType)
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

// Storage is where the write path reads one key attribute's value: the
// entity_main column the attribute is bound to and that column's encoding,
// or, for an attribute stored in EAV, its attribute ID.
type Storage struct {
	Column      forma.MainColumn
	Encoding    forma.MainColumnEncoding
	AttributeID int16
}

// String names s for an error.
func (s Storage) String() string {
	if s.Column == "" {
		return fmt.Sprintf("EAV attribute id %d", s.AttributeID)
	}
	return fmt.Sprintf("column %s (encoding %s)", s.Column, s.encoding())
}

// encoding is s's column encoding, with the default spelled out: a binding
// that names no encoding reads its column exactly as one naming "default".
func (s Storage) encoding() forma.MainColumnEncoding {
	if s.Encoding == "" {
		return forma.MainColumnEncodingDefault
	}
	return s.Encoding
}

// StorageHash identifies where a key's attributes, in order, are stored. A
// key value encodes what a read of the row answers, not where the answer was
// stored, so moving an attribute without moving its values unchanged leaves
// built entries that no row's stored values produce any more. Such a key has
// its attributes hash still and another storage hash; the startup check
// treats it as unbuilt, and a build derives its entries again.
func StorageHash(storage []Storage) string {
	var b strings.Builder
	for _, s := range storage {
		if s.Column == "" {
			fmt.Fprintf(&b, "eav %d;", s.AttributeID)
			continue
		}
		fmt.Fprintf(&b, "column %d:%s%d:%s;", len(s.Column), s.Column, len(s.encoding()), s.encoding())
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}
