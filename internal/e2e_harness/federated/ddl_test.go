package federated

import (
	"slices"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/rowguard"
	"github.com/lychee-technology/forma/internal/uniquekey"
)

// #585: this harness's entity_main copy of the init-db DDL (#440) declares
// exactly the runtime column set, so the federated suite scans the table the
// writer, the read projection and the CDC/parquet column order are built for.
// No e2e build tag: the check needs no container and runs with make test.
func TestFederatedDDLEntityMainMatchesRuntimeColumns(t *testing.T) {
	var entityMain string
	for _, stmt := range federatedDDL {
		if strings.Contains(stmt, "CREATE TABLE IF NOT EXISTS entity_main ") {
			entityMain = stmt
		}
	}
	if entityMain == "" {
		t.Fatal("federatedDDL has no entity_main statement")
	}
	if drift := model.EntityMainDDLDrift(entityMain); drift != nil {
		t.Fatalf("federated harness entity_main DDL drifts from the runtime column set:\n  %s", strings.Join(drift, "\n  "))
	}
}

// #651: the unique-key tables come from uniquekey, the source init-db's DDL
// is built from, so this harness cannot carry its own drifting copy.
func TestFederatedDDLCreatesUniqueKeyTablesFromSharedSource(t *testing.T) {
	for _, want := range uniquekey.StatementsFor("entity_main") {
		if !slices.Contains(federatedDDL, want) {
			t.Errorf("federatedDDL lacks the shared unique key statement:\n%s", want)
		}
	}
}

// #633: the row-guard table, which the aurora-dsql engine locks rows with,
// comes from rowguard, the source init-db's DDL is built from.
func TestFederatedDDLCreatesRowGuardTableFromSharedSource(t *testing.T) {
	if want := rowguard.StatementFor("entity_main"); !slices.Contains(federatedDDL, want) {
		t.Errorf("federatedDDL lacks the shared row guard statement:\n%s", want)
	}
}
