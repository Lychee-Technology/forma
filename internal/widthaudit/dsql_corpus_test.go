package widthaudit

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/dsqllint/corpus"
)

// TestDSQLCorpus exports validate-schema-consistency's width census reads
// for the DSQL lint gate (#740), each over two representative targets: an
// EAV-only one, and one bound to a wider entity_main column.
func TestDSQLCorpus(t *testing.T) {
	tables := Tables{EAV: "eav_data", EntityMain: "entity_main", ChangeLog: "change_log"}
	builders := []struct {
		label   string
		build   func(Tables, []Target) (string, []any)
		targets []Target
	}{
		{"internal/widthaudit/widthaudit.go:BuildCensusQuery", BuildCensusQuery, []Target{
			{SchemaID: 300, AttrID: 1, Declared: forma.ValueTypeSmallInt}, {SchemaID: 300, AttrID: 2, Declared: forma.ValueTypeBigInt}}},
		{"internal/widthaudit/column_census.go:BuildColumnCensusQuery", BuildColumnCensusQuery, []Target{
			{SchemaID: 300, AttrID: 3, Declared: forma.ValueTypeInteger, Column: "bigint_01"}, {SchemaID: 300, AttrID: 4, Declared: forma.ValueTypeBigInt, Column: "double_01"}}},
		{"internal/widthaudit/numeric_census.go:BuildNumericCensusQuery", BuildNumericCensusQuery, []Target{
			{SchemaID: 300, AttrID: 5, Declared: forma.ValueTypeNumeric}, {SchemaID: 300, AttrID: 6, Declared: forma.ValueTypeNumeric}}},
		{"internal/widthaudit/numeric_census.go:BuildNumericColumnCensusQuery", BuildNumericColumnCensusQuery, []Target{
			{SchemaID: 300, AttrID: 7, Declared: forma.ValueTypeNumeric, Column: "double_02"}, {SchemaID: 300, AttrID: 8, Declared: forma.ValueTypeNumeric, Column: "double_03"}}},
		{"internal/widthaudit/domain_census.go:BuildDomainCensusQuery", BuildDomainCensusQuery, []Target{
			{SchemaID: 300, AttrID: 9, Declared: forma.ValueTypeBool}, {SchemaID: 300, AttrID: 10, Declared: forma.ValueTypeDateTime}}},
		{"internal/widthaudit/domain_census.go:BuildDomainColumnCensusQuery", BuildDomainColumnCensusQuery, []Target{
			{SchemaID: 300, AttrID: 11, Declared: forma.ValueTypeBool, Column: "text_01"}, {SchemaID: 300, AttrID: 12, Declared: forma.ValueTypeDate, Column: "text_02"}}},
	}
	var statements []corpus.Statement
	for _, b := range builders {
		query, _ := b.build(tables, b.targets)
		statements = append(statements, corpus.Rendered(b.label, query))
	}
	corpus.Export(t, corpus.Unit{Name: "internal-widthaudit", Engine: forma.EngineAuroraDSQL, Statements: statements})
}
