# Forma BDD Single-Candidate Prompt

You are running in **single-candidate mode**.
The controller will handle git state, committing, and result logging.
You must **not** run any git commands.

## Context

Active target:
- target key: `{{TARGET}}`
- source file: `{{SOURCE_FILE}}`
- primary test file: `{{PRIMARY_TEST_FILE}}`
- target brief: `{{BRIEF_FILE}}`

Local runtime assumption:
- the controller starts Postgres and RustFS from `deploy/docker-compose.yml` before your candidate runs unless explicitly disabled

Decision output path: `{{DECISION_FILE}}`

## Your Job

Produce exactly **one** BDD-style test candidate, then write a decision artifact to `{{DECISION_FILE}}`.
After writing the decision artifact, print the same decision content to stdout wrapped in fallback markers.
Then stop. Do not produce multiple candidates.

## Rules

1. **Only edit test files** in `internal/**/*_test.go` or `tests/e2e/**/*`.
2. **Do not edit**: production code, CI, dependencies, configs, or anything outside `tools/autoresearch/testing/`.
3. **Do not run any git command**: no `git checkout`, `git restore`, `git reset`, `git commit`, `/undo`, or any git state-changing operation.
4. **Do not append to or modify `results.tsv`**.
5. **Do not run gate scripts yourself** — the controller will do it.
6. **Prefer extending existing test files** over creating new ones.
7. **Prefer BDD-style given/when/then** naming and structure.
8. **Assert observable outcomes**: returned errors, persisted state, emitted artifacts.
9. **Avoid**: sleeps, random behavior, log-only assertions, trivial getters.
10. **If S3 behavior is required**, prefer local RustFS docker over writing an S3 mock.
11. **Do not create fake `S3ObjectClient` implementations** for scenarios about copy/delete/object persistence; use RustFS for those.
12. **If production-code testability limits or a pre-existing bug blocks the work**, emit a structured issue in the decision artifact.

## Scenario Selection

1. Read `{{BRIEF_FILE}}` carefully — especially the **"Blocked Scenarios"** and **"Attempted & Blocked"** sections.
2. **Do not attempt any scenario listed in Blocked Scenarios** — discard it immediately and choose another.
3. **Do not retry any scenario listed in Attempted & Blocked**.
4. Choose a scenario that:
   - Is listed in Priority Scenarios but NOT in Blocked or Attempted & Blocked
   - Can be tested by creating a DuckDB in-memory database and calling the target function directly
   - Does not require mocking a non-S3 concrete type without an interface
   - May use local RustFS docker if real S3 behavior is needed
5. If the highest-priority scenarios are blocked, immediately fall back to the next unblocked scenario from the target brief.
6. Do not discard the run only because preferred scenarios are blocked.
7. Discard the run only if no meaningful unblocked scenario remains.

## Step-by-step

1. Read `{{BRIEF_FILE}}` — choose one scenario that is NOT blocked and NOT already attempted.
2. Read `{{SOURCE_FILE}}` and `{{PRIMARY_TEST_FILE}}`.
3. Write one small, high-value BDD-style test to cover that scenario.
4. If the scenario needs S3 behavior, prefer local RustFS docker instead of writing a fake S3 mock.
5. If the scenario is about object copy/delete/storage semantics, do not introduce a fake `S3ObjectClient`; use RustFS-backed behavior instead.
6. If the scenario still requires mocking a non-S3 concrete type (for example `DuckDBExecutor`) without an interface, write `status=discard` immediately — do not attempt it.
7. If that scenario is blocked, choose the next recommended fallback scenario instead of ending the run.
8. Edit only the target test file.
9. Write a decision artifact to `{{DECISION_FILE}}` with this exact format (all fields required):

```
status=keep
reason=<one line: why this test is worth keeping>
scenario=<the given/when/then scenario name>
description=<one sentence describing what the test asserts>
evidence=<coverage or behavior evidence if available, otherwise n/a>
```

Or, if the candidate should be discarded (e.g. gate failed, trivial, non-deterministic, or blocked):

```
status=discard
reason=<one line: why this candidate was discarded>
scenario=<the intended scenario or 'n/a'>
description=<what was attempted or 'n/a'>
evidence=<gate failure reason, 'blocked - requires production code changes', or 'n/a'>
```

If you hit a production-code testability limit, a pre-existing failing test, or a likely production bug, append these optional fields to the decision artifact:

```
issue_category=<testability|bug|environment|harness>
issue_file=<most relevant file path>
issue_title=<short actionable title>
issue_evidence=<one line summarizing the evidence>
issue_suggested_fix=<one line suggested fix>
```

10. After writing the decision file, print the exact same decision content to stdout using this wrapper:

```
AUTORESEARCH_DECISION_BEGIN
status=keep
reason=...
scenario=...
description=...
evidence=...
AUTORESEARCH_DECISION_END
```

Include any optional `issue_*` lines inside the same wrapper block.
11. Stop.

## Important

- The decision file is the primary artifact the controller will read to decide keep/discard.
- If the decision file is missing, the controller may recover the same decision from the stdout fallback marker block.
- If the test file has syntax errors or the gate would fail, write `status=discard`.
- If the scenario only needs S3 behavior, use RustFS docker instead of writing an S3 mock.
- If the scenario is about real object storage behavior, do not write a fake `S3ObjectClient`; use RustFS.
- If the scenario requires mocking a non-S3 concrete type without an interface, write `status=discard` with reason "blocked - requires production code changes".
- If a higher-priority scenario is blocked, fall back to another unblocked scenario in the brief before discarding the run.
- If you discover a production-code testability blocker or a pre-existing failing test, include the optional `issue_*` fields so the controller can add it to the backlog.
- If you are unsure, err on the side of `status=discard`.
- Print exactly one fallback marker block after writing the decision file, then stop.
