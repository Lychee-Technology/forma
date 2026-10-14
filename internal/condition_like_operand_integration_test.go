package internal

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestConditionOperandConformance_Like: a starts_with / contains operand is
// literal text on both storage forms; starts_with:a_c used to match "abc".
func TestConditionOperandConformance_Like(t *testing.T) {
	f := newDSLFixtureWith(t, true, "operand_conformance", operandSchemaCache())
	values := []string{"abc", "a_c", "a%c", `a\c`, "xa_cx", "A_C", "a__c"}
	for _, attr := range []string{"t_main", "t_eav"} {
		stored := map[uuid.UUID]string{}
		for _, v := range values {
			var id uuid.UUID
			if attr == "t_main" {
				id = f.seed(t, map[string]any{"text_01": v})
			} else {
				id = f.seed(t, map[string]any{}, eavText(attr, v))
			}
			stored[id] = v
		}
		for _, operand := range []string{"a_c", "_", "%", `a\`, `\c`, "a", "a__", `%\`} {
			for _, op := range []string{"starts_with", "contains"} {
				var want []uuid.UUID
				for id, v := range stored {
					if (op == "starts_with" && strings.HasPrefix(v, operand)) || (op == "contains" && strings.Contains(v, operand)) {
						want = append(want, id)
					}
				}
				require.Equal(t, sortedIDs(want...), f.match(t, kv(attr, op+":"+operand)), "%s %s:%s", attr, op, operand)
			}
		}
	}
}
