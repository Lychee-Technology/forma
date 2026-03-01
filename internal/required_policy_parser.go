package internal

import (
	"fmt"
	"strings"

	"github.com/lychee-technology/forma"
)

func parseRequiredPolicy(attrName string, attrData map[string]any, source string) (forma.RequiredPolicy, bool, error) {
	requiredRaw, hasRequired := attrData["required"]
	requiredPolicyRaw, hasRequiredPolicy := attrData["required_policy"]

	if hasRequired && hasRequiredPolicy {
		return "", false, fmt.Errorf("attribute %s in %s cannot define both required and required_policy", attrName, source)
	}

	if hasRequiredPolicy {
		policyStr, ok := requiredPolicyRaw.(string)
		if !ok {
			return "", false, fmt.Errorf("invalid required_policy for attribute %s in %s", attrName, source)
		}
		policy := forma.RequiredPolicy(strings.TrimSpace(policyStr))
		switch policy {
		case forma.RequiredPolicyOptional, forma.RequiredPolicyAlways, forma.RequiredPolicyIfParentPresent:
			return policy, true, nil
		default:
			return "", false, fmt.Errorf("invalid required_policy %q for attribute %s in %s", policyStr, attrName, source)
		}
	}

	if hasRequired {
		required, ok := requiredRaw.(bool)
		if !ok {
			return "", false, fmt.Errorf("invalid required flag for attribute %s in %s", attrName, source)
		}
		if required {
			return forma.RequiredPolicyIfParentPresent, true, nil
		}
		return forma.RequiredPolicyOptional, true, nil
	}

	return forma.RequiredPolicyOptional, false, nil
}
