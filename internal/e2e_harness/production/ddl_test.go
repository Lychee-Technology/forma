package production

import (
	"slices"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/rowguard"
	"github.com/lychee-technology/forma/internal/schemalease"
	"github.com/lychee-technology/forma/internal/uniquekey"
)

// #585: this harness's entity_main copy of the init-db DDL (#440) declares
// exactly the runtime column set, so the production suite exercises the table
// the writer, the read projection and the CDC column order are built for.
func TestProductionDDLEntityMainMatchesRuntimeColumns(t *testing.T) {
	var entityMain string
	for _, stmt := range productionDDL {
		if strings.Contains(stmt, "CREATE TABLE IF NOT EXISTS entity_main ") {
			entityMain = stmt
		}
	}
	if entityMain == "" {
		t.Fatal("productionDDL has no entity_main statement")
	}
	if drift := model.EntityMainDDLDrift(entityMain); drift != nil {
		t.Fatalf("production harness entity_main DDL drifts from the runtime column set:\n  %s", strings.Join(drift, "\n  "))
	}
}

// #651: the unique-key tables come from uniquekey, the source init-db's DDL
// is built from, so this harness cannot carry its own drifting copy.
func TestProductionDDLCreatesUniqueKeyTablesFromSharedSource(t *testing.T) {
	for _, want := range uniquekey.StatementsFor("entity_main") {
		if !slices.Contains(productionDDL, want) {
			t.Errorf("productionDDL lacks the shared unique key statement:\n%s", want)
		}
	}
}

// #633: the row-guard table, which the aurora-dsql engine locks rows with,
// comes from rowguard, the source init-db's DDL is built from.
func TestProductionDDLCreatesRowGuardTableFromSharedSource(t *testing.T) {
	if want := rowguard.StatementFor("entity_main"); !slices.Contains(productionDDL, want) {
		t.Errorf("productionDDL lacks the shared row guard statement:\n%s", want)
	}
}

func TestProductionDDLCreatesSchemaLeaseTableFromSharedSource(t *testing.T) {
	if want := schemalease.StatementFor("entity_main"); !slices.Contains(productionDDL, want) {
		t.Errorf("productionDDL lacks the shared schema lease statement:\n%s", want)
	}
}
