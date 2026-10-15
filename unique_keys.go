package forma

// UniqueKeyDeclaration is one unique key a schema declares under
// "x-forma-unique-keys" (#651): among the schema's live rows, no two share
// the tuple of its attributes' values. A row with any of the attributes
// absent or null is not subject to the key. Values compare as their typed
// canonical form, so "1" and 1 on an integer attribute are equal while text
// compares byte-exact. Every write maintains the keys its schema declares;
// the rows written before the declaration are covered once the key is built
// (forma-tools build-unique-keys), and the server refuses to start while a
// declared key is not built. See docs/unique-keys.md.
type UniqueKeyDeclaration struct {
	// ID is 1 to 32767 and unique within the schema. It identifies the key
	// in storage, so a deployed key's ID is never reused, and changing a
	// key's attributes needs a new ID.
	ID int16 `json:"id"`
	// Name matches ^[a-z][a-z0-9_]{0,30}$ and is unique within the schema. A
	// violation's error names the key by it.
	Name string `json:"name"`
	// Attributes is the ordered tuple: 1 to MaxUniqueKeyAttributes distinct
	// scalar attributes, column-bound or EAV. A list attribute, or one inside
	// an array, cannot be part of a key.
	Attributes []string `json:"attributes"`
}

// MaxUniqueKeyAttributes bounds the tuple of one unique key.
const MaxUniqueKeyAttributes = 4
