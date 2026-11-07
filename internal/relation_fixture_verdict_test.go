package internal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// relationFixtureVerdictFile is the sidecar every relation fixture directory
// carries beside its schema documents. It is not a .json file on purpose:
// readSchemaDocs serves every .json in the directory as a schema, and the
// validator resolves every schema the registry lists.
const relationFixtureVerdictFile = "verdict.txt"

// relationFixtureVerdict is what a fixture is recorded to do, measured against
// the post-strip payload (buildStrippedPayload). It is the single place a
// fixture's expectations live (#395): the fixture-driven tests enumerate the
// fixture directories from disk and select by these fields, rather than each
// keeping its own literal list.
//
// The file is a few "key: value" lines; "#" starts a comment line.
//
//	guard:     boots | refuses       what LoadRelationIndex does with the fixture
//	validator: accepts | rejects     what the resolved validator does with the payload
//	message:   names-root | omits-root
//	                                 whether that rejection names "contactSnapshot";
//	                                 required when the validator rejects, and
//	                                 forbidden when it accepts
type relationFixtureVerdict struct {
	GuardRefuses     bool
	ValidatorRejects bool
	// MessageNamesRoot is meaningful only when ValidatorRejects is true.
	MessageNamesRoot bool
}

// loadRelationFixtureVerdict reads one fixture's sidecar, and fails the test
// naming the file and what it lacks, so a fixture added without one stops every
// fixture-driven test instead of quietly dropping out of the subsets they draw.
func loadRelationFixtureVerdict(t *testing.T, fixture string) relationFixtureVerdict {
	t.Helper()
	path := filepath.Join(resolveRelationFixtureDir(fixture), relationFixtureVerdictFile)
	body, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		require.FailNowf(t, "relation fixture has no verdict file",
			"%s is missing: every relation fixture must state what the guard and the validator do "+
				"with it (guard: boots|refuses, validator: accepts|rejects, and message: "+
				"names-root|omits-root when the validator rejects)", path)
	}
	require.NoError(t, err, "read %s", path)

	verdict, err := parseRelationFixtureVerdict(string(body))
	require.NoError(t, err, "parse %s", path)
	return verdict
}

// parseRelationFixtureVerdict is strict: an unknown key, a repeated key, a value
// outside its set or a missing key is an error, and so is a message expectation
// that does not match the validator verdict.
func parseRelationFixtureVerdict(body string) (relationFixtureVerdict, error) {
	fields, err := splitVerdictFields(body)
	if err != nil {
		return relationFixtureVerdict{}, err
	}

	var v relationFixtureVerdict
	if v.GuardRefuses, err = verdictChoice(fields, "guard", "boots", "refuses"); err != nil {
		return relationFixtureVerdict{}, err
	}
	if v.ValidatorRejects, err = verdictChoice(fields, "validator", "accepts", "rejects"); err != nil {
		return relationFixtureVerdict{}, err
	}

	// The soundness property (TestGuardNeverRefusesASchemaTheValidatorAccepts)
	// rules this pair out, and a sidecar recording it would steer a rejecting
	// fixture out of the message-shape subset with no message expectation at all.
	if v.GuardRefuses && !v.ValidatorRejects {
		return relationFixtureVerdict{}, errors.New(
			"\"guard: refuses\" with \"validator: accepts\" is contradictory: the guard never refuses " +
				"a schema the validator accepts, so a fixture the guard refuses records validator: rejects")
	}

	_, hasMessage := fields["message"]
	switch {
	case v.ValidatorRejects && !hasMessage:
		return relationFixtureVerdict{}, errors.New(
			"missing key \"message\": the validator rejects this fixture, so state whether " +
				"its message names the relation root (names-root|omits-root)")
	case !v.ValidatorRejects && hasMessage:
		return relationFixtureVerdict{}, errors.New(
			"key \"message\" is set, but the validator accepts this fixture, so there is no message to describe")
	case hasMessage:
		if v.MessageNamesRoot, err = verdictChoice(fields, "message", "omits-root", "names-root"); err != nil {
			return relationFixtureVerdict{}, err
		}
	}
	return v, nil
}

// relationFixtureMessageRoot is the relation root every relation fixture strips,
// and so the name a "message: names-root" rejection carries.
const relationFixtureMessageRoot = "contactSnapshot"

// relationFixtureVerdictMismatch compares every fact a verdict records with what
// the real guard (guardErr, from LoadRelationIndex) and the real resolved
// validator (validatorErr, on the post-strip payload) answered, and describes
// each disagreement; nil when the sidecar is right. The fixture-driven tests
// select their subsets by these facts, so each of them is measured here for
// every fixture before any subset relies on it (#395, R1-F1): a recorded
// acceptance the validator does not give is a failure, not a skip.
func relationFixtureVerdictMismatch(fixture string, v relationFixtureVerdict, guardErr, validatorErr error) error {
	var errs []error
	if refuses := guardErr != nil; refuses != v.GuardRefuses {
		errs = append(errs, fmt.Errorf("%s records guard refuses=%t, but LoadRelationIndex answered %v",
			fixture, v.GuardRefuses, guardErr))
	}
	if rejects := validatorErr != nil; rejects != v.ValidatorRejects {
		errs = append(errs, fmt.Errorf("%s records validator rejects=%t, but the validator answered %v",
			fixture, v.ValidatorRejects, validatorErr))
	}
	if v.ValidatorRejects && validatorErr != nil {
		names := strings.Contains(validatorErr.Error(), relationFixtureMessageRoot)
		if names != v.MessageNamesRoot {
			errs = append(errs, fmt.Errorf("%s records message names-root=%t, but the rejection reads %q",
				fixture, v.MessageNamesRoot, validatorErr.Error()))
		}
	}
	return errors.Join(errs...)
}

// splitVerdictFields reads the "key: value" lines of a verdict file.
func splitVerdictFields(body string) (map[string]string, error) {
	fields := make(map[string]string)
	for i, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			return nil, fmt.Errorf("line %d is %q, expected \"key: value\"", i+1, line)
		}
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		switch key {
		case "guard", "validator", "message":
		default:
			return nil, fmt.Errorf("line %d has unknown key %q, expected guard, validator or message", i+1, key)
		}
		if _, dup := fields[key]; dup {
			return nil, fmt.Errorf("line %d repeats key %q", i+1, key)
		}
		fields[key] = value
	}
	return fields, nil
}

// verdictChoice reads a two-valued key, answering true for the second value.
func verdictChoice(fields map[string]string, key, no, yes string) (bool, error) {
	value, ok := fields[key]
	switch {
	case !ok:
		return false, fmt.Errorf("missing key %q (%s|%s)", key, no, yes)
	case value == no:
		return false, nil
	case value == yes:
		return true, nil
	}
	return false, fmt.Errorf("key %q is %q, expected %s or %s", key, value, no, yes)
}

// TestRelationFixtureVerdictParserFailsLoudly pins the failure half of #395's
// acceptance: a verdict file that leaves a fact out, or states one the tests
// cannot use, is an error naming the fact, never a fixture that silently falls
// out of a subset.
func TestRelationFixtureVerdictParserFailsLoudly(t *testing.T) {
	for name, tc := range map[string]struct {
		body    string
		wantErr string
	}{
		"empty":                       {"# nothing\n", `missing key "guard"`},
		"no validator":                {"guard: boots\n", `missing key "validator"`},
		"rejecting without message":   {"guard: boots\nvalidator: rejects\n", `missing key "message"`},
		"accepting with message":      {"guard: boots\nvalidator: accepts\nmessage: omits-root\n", `key "message" is set`},
		"unknown value":               {"guard: maybe\nvalidator: accepts\n", `key "guard" is "maybe"`},
		"unknown key":                 {"guard: boots\nvalidator: accepts\nnote: x\n", `unknown key "note"`},
		"repeated key":                {"guard: boots\nguard: refuses\nvalidator: accepts\n", `repeats key "guard"`},
		"not a key-value line":        {"guard boots\n", `expected "key: value"`},
		"unknown message expectation": {"guard: boots\nvalidator: rejects\nmessage: vague\n", `key "message" is "vague"`},
		// R1-F1: the pair the soundness property forbids, with and without a message.
		"refusing yet accepting":              {"guard: refuses\nvalidator: accepts\n", `is contradictory`},
		"refusing yet accepting with message": {"guard: refuses\nvalidator: accepts\nmessage: omits-root\n", `is contradictory`},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parseRelationFixtureVerdict(tc.body)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}

	v, err := parseRelationFixtureVerdict("# c\nguard: refuses\nvalidator: rejects\nmessage: names-root\n")
	require.NoError(t, err)
	require.Equal(t, relationFixtureVerdict{GuardRefuses: true, ValidatorRejects: true, MessageNamesRoot: true}, v)
}

// TestRelationFixtureVerdictMismatchChecksEveryFact pins that a sidecar is held
// to what the guard and the validator measurably do, fact by fact (R1-F1). The
// contradictory sidecar the review reproduced — relation_required_allof recorded
// as "guard: refuses, validator: accepts" with no message — must fail here even
// were it to get past the parser, because the validator rejects that fixture.
func TestRelationFixtureVerdictMismatchChecksEveryFact(t *testing.T) {
	guardRefusal := errors.New("requires relation root")
	namesRoot := errors.New(`missing properties ["contactSnapshot"]`)
	omitsRoot := errors.New("validation failed under oneOf")

	for name, tc := range map[string]struct {
		verdict                relationFixtureVerdict
		guardErr, validatorErr error
		wantErr                string
	}{
		"recorded acceptance the validator does not give": {
			relationFixtureVerdict{GuardRefuses: true}, guardRefusal, namesRoot, "validator rejects=false"},
		"recorded rejection the validator does not give": {
			relationFixtureVerdict{ValidatorRejects: true}, nil, nil, "validator rejects=true"},
		"recorded refusal the guard does not give": {
			relationFixtureVerdict{GuardRefuses: true, ValidatorRejects: true, MessageNamesRoot: true},
			nil, namesRoot, "guard refuses=true"},
		"recorded names-root on a message that omits it": {
			relationFixtureVerdict{ValidatorRejects: true, MessageNamesRoot: true}, nil, omitsRoot, "names-root=true"},
		"recorded omits-root on a message that names it": {
			relationFixtureVerdict{ValidatorRejects: true}, nil, namesRoot, "names-root=false"},
	} {
		t.Run(name, func(t *testing.T) {
			require.ErrorContains(t, relationFixtureVerdictMismatch("f", tc.verdict, tc.guardErr, tc.validatorErr), tc.wantErr)
		})
	}

	require.NoError(t, relationFixtureVerdictMismatch("f",
		relationFixtureVerdict{GuardRefuses: true, ValidatorRejects: true, MessageNamesRoot: true}, guardRefusal, namesRoot))
	require.NoError(t, relationFixtureVerdictMismatch("f", relationFixtureVerdict{}, nil, nil))

	// The reviewer's reproduction, against the real fixture.
	const fixture = "relation_required_allof"
	_, guardErr := LoadRelationIndex(serveRelationFixture(t, fixture))
	validatorErr := validateFixtureChild(t, fixture, buildStrippedPayload())
	require.ErrorContains(t, relationFixtureVerdictMismatch(fixture,
		relationFixtureVerdict{GuardRefuses: true}, guardErr, validatorErr), "validator rejects=false")
}
