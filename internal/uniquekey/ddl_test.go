package uniquekey

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
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

func TestStatementsForDerivesEveryNameFromEntityMain(t *testing.T) {
	statements := StatementsFor("entity_main")
	assert.Len(t, statements, 3)
	assert.True(t, strings.HasPrefix(statements[0], "CREATE TABLE IF NOT EXISTS entity_main_unique_key ("))
	assert.Equal(t, "CREATE INDEX IF NOT EXISTS entity_main_unique_key_row_idx ON entity_main_unique_key (ltbase_schema_id, ltbase_row_id)", statements[1])
	assert.True(t, strings.HasPrefix(statements[2], "CREATE TABLE IF NOT EXISTS entity_main_unique_key_state ("))
}

func TestKeyTableNamesKeepQualifierAndQuotes(t *testing.T) {
	for entityMain, want := range map[string][2]string{
		"entity_main":         {"entity_main_unique_key", "entity_main_unique_key_state"},
		"public.entity_main":  {"public.entity_main_unique_key", "public.entity_main_unique_key_state"},
		`"entity_main"`:       {`"entity_main_unique_key"`, `"entity_main_unique_key_state"`},
		`"app"."Entity Main"`: {`"app"."Entity Main_unique_key"`, `"app"."Entity Main_unique_key_state"`},
	} {
		assert.Equal(t, want[0], KeyTable(entityMain), entityMain)
		assert.Equal(t, want[1], StateTable(entityMain), entityMain)
	}
}
