# Forma BDD Test Research

## Goal

Add behavior-driven tests for one target area at a time.
Do not optimize for total repository coverage alone.
Prefer user-visible or system-visible behaviors, failure scenarios, and regression guards.

## Scope

You may only edit test files:
- `internal/**/*_test.go`
- `tests/e2e/**/*`

You must not edit:
- production code
- module files
- CI workflows
- documentation outside `tools/autoresearch/testing`
- dependencies

## Working Style

- Work on exactly one target brief at a time.
- Prefer extending an existing test file over creating a new one.
- Keep each patch small and reviewable.
- Prefer table-driven tests and deterministic stubs.
- Prefer naming and structure that reads like a scenario: given, when, then.
- Assert observable outcomes, returned errors, persisted state, or emitted artifacts.
- Avoid sleeps, random behavior, and log-only assertions.

## Keep Criteria

A candidate is `keep` only when all are true:
- only test files changed
- the relevant fast gate passes
- the patch captures a real scenario or regression risk from the target brief
- assertions check observable behavior rather than implementation trivia
- the test is deterministic and non-trivial
- no unrelated scope expansion is introduced

Coverage and function-focus reports are supporting evidence, not the primary goal.

Otherwise mark it `discard`.

## Scoring Guidance

Use this as a lightweight decision aid:
- +4 meaningful scenario from the target brief is covered
- +3 regression-prone failure mode or business rule is asserted
- +2 observable given/when/then structure is clear in the test
- +1 coverage or function-focus evidence improves
- -2 trivial assertion or low-value path
- -3 implementation-coupled assertions with weak behavioral value
- -3 flaky timing or environment-sensitive logic

Treat scores below 5 as likely `discard`.

## Experiment Loop

1. Read this file.
2. Read one target brief from `targets/`.
3. Read the target production file and neighboring tests.
4. Add one small BDD-style test-only patch.
5. Run the matching local gate script.
6. Record the result in `results.tsv`.
7. Keep or discard the candidate.
8. Continue with the next missing scenario in the same target.

## Never Do

- Do not modify production files to make tests easier.
- Do not add new dependencies.
- Do not chase coverage in unrelated packages.
- Do not keep a patch only because it increases a number.
- Do not prefer white-box assertions when a behavior-level assertion is possible.
