Read:
- `tools/autoresearch/testing/program-testcov.md`
- one target brief from `tools/autoresearch/testing/targets/`
- the target production file
- neighboring test files

Task:
Add one small, high-value BDD-style test-only patch.
Prefer extending an existing `*_test.go` file.

Prioritize:
- scenario gaps from the target brief
- business rules and failure handling
- boundary conditions with observable outcomes
- regression-prone behaviors
- deterministic behavior

Avoid:
- trivial assertions
- flaky timing
- production code edits
- implementation-only assertions when a behavior-level check is possible
- unrelated package changes

Output:
- one-sentence given/when/then summary of the scenario covered
- short note on why the candidate is behaviorally valuable
