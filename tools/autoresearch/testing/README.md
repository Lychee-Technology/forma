# Local BDD Test Autoresearch

A constrained, local-only adaptation of `karpathy/autoresearch` for improving BDD-style tests in Forma.

## Architecture

The system has two layers:

- **Controller** (`autoloop.sh`): manages worktree, git branch, snapshots, and the keep/discard decision.
- **Agent** (`opencode_autoresearch.sh`): generates exactly one BDD-style test candidate and writes a decision artifact.

The key design principle is **commit-per-keep**: every accepted candidate is immediately committed, and discarding never uses git revert commands that could erase prior work.

## Layout

- `program-testcov.md`: master rules for the testing agent
- `targets/`: target-specific briefs with priority scenarios
- `prompts/`: prompt templates for OpenCode
  - `opencode-single-candidate.md`: single-candidate controller mode (default for autoloop)
  - `opencode-autoloop.md`: legacy multi-candidate mode
- `scripts/`
  - `autoloop.sh`: main controller — use this to run the loop
  - `opencode_autoresearch.sh`: OpenCode launcher
  - `common.sh`: shared shell utilities
  - `baseline.sh`, `run_candidate.sh`, `medium_gate.sh`, `heavy_gate.sh`: gate scripts
- `results.tsv`: experiment log written by the controller (not by the model)
- `issues.tsv`: backlog of production-code blockers, pre-existing bugs, worthwhile production improvements, and harness issues discovered by autoresearch

## Quick start

```bash
# Create a local branch for this session
git checkout -b autoresearch/my-session

# Run the loop in a dedicated worktree
./tools/autoresearch/testing/scripts/autoloop.sh \
  --model github-copilot/claude-sonnet-4.5 \
  --target flusher \
  --baseline \
  --iterations 20
```

## autoloop.sh

This is the main entry point. It:

1. Creates/uses a dedicated git worktree at `.worktrees/autoresearch-<target>/`
2. Uses a research branch `autoresearch/<target>-<date>`
3. Starts local Postgres and RustFS from `deploy/docker-compose.yml` unless disabled
4. Runs one `opencode run` per iteration
5. Reads the decision artifact from the agent
6. If `keep`: commits the candidate immediately
7. If `discard`: reverts to pre-run state (controller, not model, does this)
8. Appends one row to `results.tsv` per iteration
9. Appends structured issues to `issues.tsv` when the agent reports a blocker, bug, or worthwhile production improvement

Safety checks:
- Refuses to run on `main` or any non-research branch unless `--force` is set
- Verifies the worktree is clean before each iteration
- Model is forbidden from running any git state-changing commands

Key options:

```bash
--model MODEL           OpenCode model (required)
--target TARGET         flusher | postgres_duckdb_query | ...
--iterations N          Number of candidates (default: 20)
--baseline             Run coverage baseline before the loop
--skip-local-infra     Do not auto-start Postgres and RustFS
--session ID           Continue a specific OpenCode session across all runs
-c, --continue          Continue the last session across all runs
--force                Skip branch/safety checks
--dry-run              Show what would run without executing
```

## Single-candidate mode

Every `opencode run` produces exactly **one** candidate, then stops.
The controller then:
- Runs the fast gate
- Commits if keep, reverts if discard
- Moves to the next iteration

This prevents the previous problem where a discard in the middle of a multi-candidate run would erase all prior uncommitted keeps.

## Decision artifact

The agent writes a `decision.txt` file with this format:

```
status=keep
reason=why this is worth keeping
scenario=given/when/then summary
description=what the test asserts
evidence=coverage or behavior evidence
```

Or for discard:

```
status=discard
reason=why it was rejected
scenario=intended scenario or n/a
description=what was attempted or n/a
evidence=gate failure reason or n/a
```

Optional structured issue fields:

```
issue_category=testability|bug|environment|harness
issue_file=internal/foo.go
issue_title=Short actionable title
issue_evidence=One-line evidence summary
issue_suggested_fix=One-line suggested fix
```

These fields are also the mechanism for recording non-blocking but worthwhile production-code improvements discovered during test iteration, such as extracting a narrow seam, tightening validation, or simplifying a brittle code path.

Record a production improvement only when it is specific, actionable, and likely to matter for correctness, testability, or maintenance.
Good examples include missing interfaces/seams, brittle validation paths, duplicated branching that obscures behavior, or misleading defaults that make tests or behavior hard to reason about.
Do not record cosmetic cleanup, naming preferences, formatting ideas, or broad refactor wishes without concrete evidence from the current iteration.

The controller reads this and decides commit vs. revert. The model never touches git state.
When `issue_*` fields are present, the controller also appends an entry to `issues.tsv` with deduplication.

For resilience, single-candidate runs also ask the agent to print the same decision block to stdout wrapped in `AUTORESEARCH_DECISION_BEGIN` / `AUTORESEARCH_DECISION_END`.
If the decision file is missing but the stdout fallback block is present, the controller reconstructs the decision artifact from stdout and continues the run.

## Gate scripts

- `baseline.sh`: capture starting coverage evidence for a target
- `run_candidate.sh`: run fast gate and collect evidence for a candidate
- `medium_gate.sh`: run Go E2E harness smoke
- `heavy_gate.sh`: run Go performance suite + k6 (expensive, manual)

## Local Infra

For S3-backed scenarios, prefer running against the repo's local RustFS docker setup instead of introducing S3 mocks.
`autoloop.sh` now starts local Postgres and RustFS automatically from `deploy/docker-compose.yml` by default.

Useful repo references:
- `internal/e2e_harness/harness.go`
- `internal/e2e_harness/README.md`
- `tests/e2e/README.md`
- `deploy/docker-compose.yml`

Common local defaults in this repo:
- endpoint: `http://localhost:19000`
- access key: `minio`
- secret: `minio` or `minio_password` depending on the path being reused

If a scenario only needs S3 semantics, RustFS is preferred.
Only mark a scenario blocked when it still needs a non-S3 concrete dependency with no seam.

If you already manage local services yourself, pass `--skip-local-infra`.

## Target briefs

See `targets/` for priority scenarios per target. Start with `flusher`.

## Notes

- Coverage reports are supporting evidence, not the primary goal.
- Heavy gate (Docker + k6) is intentionally not automated in every iteration.
- The model is forbidden from git rollback, results logging, or modifying non-test files.
