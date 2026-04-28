# Forma Benchmark Single-Candidate Prompt

You are running in single-candidate benchmark autoresearch mode.
The controller will handle git state, benchmark execution, artifact capture, and results logging.
You must not run any git commands.

## Context

Active target:
- target key: `{{TARGET}}`
- gate: `{{GATE}}`
- source file: `{{SOURCE_FILE}}`
- target brief: `{{BRIEF_FILE}}`
- rules file: `{{RULES_FILE}}`
- worktree root: `{{WORKTREE_DIR}}`
- target workloads: `{{TARGET_WORKLOADS}}`
- protected workloads: `{{PROTECTED_WORKLOADS}}`

Controller-run benchmark steps:
- candidate script: `{{CANDIDATE_SCRIPT}}`
- gate script: `{{GATE_SCRIPT}}`

Decision output path: `{{DECISION_FILE}}`

## Your Job

Produce exactly one small performance candidate for the active target, then write a decision artifact to `{{DECISION_FILE}}`.
After writing the decision artifact, print the same decision content to stdout wrapped in fallback markers.
Then stop.

## Rules

1. Do not run any git command.
2. Do not run the benchmark scripts yourself.
3. Keep the patch tightly scoped to the active performance target.
4. Do not edit benchmark methodology, workloads, CI, dependencies, or unrelated docs.
5. Prefer execution-path, SQL-shape, pushdown, pagination-shape, or routing improvements that match the active target brief over low-leverage call overhead tweaks.
6. Preserve correctness and protected-workload behavior.
7. If blocked by benchmark, harness, environment, or a code-structure issue, emit a structured issue.
8. For deep-pagination targets, prefer changes that reduce rows or row width carried into `ROW_NUMBER()`, `COUNT(*) OVER()`, and final `ORDER BY ... LIMIT/OFFSET` work.

## Step-by-step

1. Read `{{RULES_FILE}}`.
2. Read `{{BRIEF_FILE}}`.
3. Read `{{SOURCE_FILE}}` and any directly relevant helper code.
4. Implement one small performance candidate aligned with the active target brief.
   Prefer candidates that change real execution work, not cosmetic rewrites.
5. Write a decision artifact to `{{DECISION_FILE}}` with this exact format:

```
status=keep
reason=<one line: why this candidate is worth benchmarking>
scenario=<short optimization hypothesis>
description=<one sentence describing the code change>
evidence=<expected performance effect or n/a>
```

Or, if the candidate should be discarded before benchmarking:

```
status=discard
reason=<one line: why this candidate should not be benchmarked>
scenario=<optimization hypothesis or n/a>
description=<what was attempted or n/a>
evidence=<blocked reason or n/a>
```

Optional issue fields:

```
issue_category=<perf-opportunity|bug|environment|harness|methodology>
issue_file=<most relevant file path>
issue_title=<short actionable title>
issue_evidence=<one line evidence>
issue_suggested_fix=<one line suggested fix>
```

6. After writing the decision file, print the exact same decision content to stdout using:

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
7. Stop.

## Important

- The controller, not you, decides the final keep/discard recommendation after the benchmark runs.
- If the decision file is missing, the controller may recover it from the stdout fallback block.
- If you are blocked or the change is too broad, emit `status=discard`.
- If you notice a harness or methodology gap that invalidates the evidence, record it with `issue_*` fields.
- For this target, avoid row-buffer reuse or other Go-side micro-optimizations unless they are clearly required to support the target's primary execution-path improvement.
- If the active brief is deep-page-oriented, explicitly favor candidates that reduce the amount of data reaching the final deep-page pagination stage.
