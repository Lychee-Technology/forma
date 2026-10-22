package uniquekey

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlutil"
	"github.com/stretchr/testify/assert"
)

// The hash a key is built under changes with anything that changes its
// values: the attribute names, their order and their types.
func TestAttributesHashIdentifiesTheAttributeList(t *testing.T) {
	attr := func(name string, vt forma.ValueType) Attribute { return Attribute{Name: name, ValueType: vt} }
	base := AttributesHash([]Attribute{attr("tenant", forma.ValueTypeText), attr("email", forma.ValueTypeText)})
	assert.Equal(t, base, AttributesHash([]Attribute{attr("tenant", forma.ValueTypeText), attr("email", forma.ValueTypeText)}))
	for name, other := range map[string][]Attribute{
		"order":    {attr("email", forma.ValueTypeText), attr("tenant", forma.ValueTypeText)},
		"type":     {attr("tenant", forma.ValueTypeText), attr("email", forma.ValueTypeUUID)},
		"name":     {attr("tenant", forma.ValueTypeText), attr("mail", forma.ValueTypeText)},
		"boundary": {attr("tenante", forma.ValueTypeText), attr("mail", forma.ValueTypeText)},
		"subset":   {attr("tenant", forma.ValueTypeText)},
	} {
		assert.NotEqual(t, base, AttributesHash(other), name)
	}
}

// The storage hash changes with where any attribute's value is read from,
// and with nothing else: the default encoding reads as itself however it is
// spelled.
func TestStorageHashIdentifiesWhereTheAttributesAreStored(t *testing.T) {
	text01 := Storage{Column: forma.MainColumnText01}
	eav2 := Storage{AttributeID: 2}
	base := StorageHash([]Storage{text01, eav2})
	assert.Equal(t, base, StorageHash([]Storage{{Column: forma.MainColumnText01, Encoding: forma.MainColumnEncodingDefault}, eav2}))
	assert.Equal(t, base, StorageHash([]Storage{{Column: forma.MainColumnText01, AttributeID: 9}, eav2}),
		"a bound attribute is read from its column, whatever its attribute id")
	for name, other := range map[string][]Storage{
		"column":   {{Column: forma.MainColumnText02}, eav2},
		"encoding": {{Column: forma.MainColumnText01, Encoding: forma.MainColumnEncodingISO8601}, eav2},
		"eav id":   {text01, {AttributeID: 3}},
		"to eav":   {{AttributeID: 1}, eav2},
		"order":    {eav2, text01},
		"subset":   {text01},
	} {
		assert.NotEqual(t, base, StorageHash(other), name)
	}
	assert.Equal(t, "column text_01 (encoding default)", text01.String())
	assert.Equal(t, "EAV attribute id 2", eav2.String())
}

func TestStatementsForDerivesEveryNameFromEntityMain(t *testing.T) {
	statements := StatementsFor("entity_main")
	assert.Len(t, statements, 3)
	assert.True(t, strings.HasPrefix(statements[0], "CREATE TABLE IF NOT EXISTS entity_main_unique_key ("))
	assert.Equal(t, "CREATE INDEX IF NOT EXISTS entity_main_unique_key_row_idx ON entity_main_unique_key (ltbase_schema_id, ltbase_row_id)", statements[1])
	assert.True(t, strings.HasPrefix(statements[2], "CREATE TABLE IF NOT EXISTS entity_main_unique_key_state ("))
}

func TestKeyTableNamesKeepQualifierAndQuotes(t *testing.T) {
	for entityMain, want := range map[string][3]string{
		"entity_main":         {"entity_main_unique_key", "entity_main_unique_key_state", "entity_main_unique_key_row_idx"},
		"public.entity_main":  {"public.entity_main_unique_key", "public.entity_main_unique_key_state", "entity_main_unique_key_row_idx"},
		`"entity_main"`:       {`"entity_main_unique_key"`, `"entity_main_unique_key_state"`, "entity_main_unique_key_row_idx"},
		`"app"."Entity Main"`: {`"app"."Entity Main_unique_key"`, `"app"."Entity Main_unique_key_state"`, "Entity Main_unique_key_row_idx"},
	} {
		assert.Equal(t, want[0], KeyTable(entityMain), entityMain)
		assert.Equal(t, want[1], StateTable(entityMain), entityMain)
		assert.Equal(t, want[2], RowIndex(entityMain), entityMain)
	}
}

// PostgreSQL truncates an identifier past 63 bytes, so a derived name must
// fit, or the key table and state table of a long entity_main name would be
// one table (#651). Names that already fit keep their plain form.
func TestDerivedNamesFitAnIdentifierAndStayApart(t *testing.T) {
	names := []string{strings.Repeat("é", 30)} // 60 bytes: a cut must not split a character
	for n := 40; n <= 80; n++ {
		names = append(names, strings.Repeat("m", n))
	}
	for _, entityMain := range names {
		derived := map[string]string{
			KeyTableSuffix:   KeyTable(entityMain),
			StateTableSuffix: StateTable(entityMain),
			RowIndexSuffix:   RowIndex(entityMain),
		}
		seen := map[string]bool{}
		for suffix, name := range derived {
			assert.LessOrEqual(t, len(name), sqlutil.MaxIdentifierLen, name)
			assert.True(t, utf8.ValidString(name), name)
			assert.True(t, strings.HasSuffix(name, suffix), name)
			if len(entityMain)+len(suffix) <= sqlutil.MaxIdentifierLen {
				assert.Equal(t, entityMain+suffix, name)
			}
			assert.False(t, seen[name], "%d-byte entity_main derives %s twice", len(entityMain), name)
			seen[name] = true
		}
	}
	// The first case the review found: the key table fits exactly, and the
	// state table, which used to truncate to the key table's name, does not.
	main52 := strings.Repeat("m", 52)
	assert.Equal(t, main52+KeyTableSuffix, KeyTable(main52))
	assert.NotEqual(t, KeyTable(main52), StateTable(main52)[:sqlutil.MaxIdentifierLen])
}

func TestShortenedNamesTellLongEntityMainNamesApart(t *testing.T) {
	a, b := strings.Repeat("m", 70)+"a", strings.Repeat("m", 70)+"b"
	assert.NotEqual(t, KeyTable(a), KeyTable(b))
	assert.NotEqual(t, StateTable(a), StateTable(b))
	assert.NotEqual(t, RowIndex(a), RowIndex(b))
}

// Every consumer derives the same physical name, however it spells
// entity_main: init-db may name it bare while the factory qualifies it.
func TestShortenedNamesIgnoreQualifierAndQuotes(t *testing.T) {
	long := strings.Repeat("m", 70)
	assert.Equal(t, "public."+KeyTable(long), KeyTable("public."+long))
	assert.Equal(t, `"app"."`+StateTable(long)+`"`, StateTable(`"app"."`+long+`"`))
	assert.Equal(t, RowIndex(long), RowIndex(`"app"."`+long+`"`))
}
