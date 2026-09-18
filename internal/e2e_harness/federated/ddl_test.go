package federated

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
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
