package production

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
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
