package main

import (
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
)

// #585: the entity_main table init-db creates declares exactly the column set
// the runtime writes, projects and flushes (model.EntityMainColumnDescriptors),
// with matching types. A column here that the runtime lacks is dead storage
// that registration refuses to bind (#557); a runtime column missing here
// fails every write. The golden in init_db_ddl_test.go still guards the
// literal text; this guards the set, so a change to either side that forgets
// the other turns red.
func TestEntityMainDDLMatchesRuntimeColumns(t *testing.T) {
	if drift := model.EntityMainDDLDrift(entityMainDDL(`"entity_main"`)); drift != nil {
		t.Fatalf("entity_main DDL drifts from the runtime column set:\n  %s", strings.Join(drift, "\n  "))
	}
}
