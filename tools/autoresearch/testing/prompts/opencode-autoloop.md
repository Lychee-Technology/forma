Read these files first:
- `{{WORKTREE_DIR}}/tools/autoresearch/testing/README.md`
- `{{WORKTREE_DIR}}/tools/autoresearch/testing/program-testcov.md`
- `{{BRIEF_FILE}}`
- `{{WORKTREE_DIR}}/tools/autoresearch/testing/prompts/test-gap.md`
- `{{WORKTREE_DIR}}/tools/autoresearch/testing/prompts/test-review.md`

Active target:
- target key: `{{TARGET}}`
- source file: `{{SOURCE_FILE}}`
- primary test file: `{{PRIMARY_TEST_FILE}}`
- worktree root: `{{WORKTREE_DIR}}`

Run configuration:
- max candidate attempts this run: `{{MAX_ITERATIONS}}`
- run `./tools/autoresearch/testing/scripts/medium_gate.sh` every `{{MEDIUM_GATE_EVERY}}` kept candidates

Rules:
- only modify test files and `tools/autoresearch/testing/results.tsv`
- never modify production code, CI, dependencies, or docs outside `tools/autoresearch/testing`
- never revert or disturb unrelated user changes already present in the worktree
- keep patches small and reviewable
- prefer extending existing tests
- prefer deterministic mocks, stubs, and table-driven tests
- prefer BDD-style scenario framing and observable assertions
- if you discover a blocker, bug, or worthwhile production-code improvement, record it via the structured `issue_*` fields in the decision artifact so it lands in `issues.tsv`

Loop until you hit the max candidate attempts or I manually stop you:

1. Inspect git status and identify any unrelated pre-existing changes.
2. Read the target source file and nearby tests before editing.
3. Add one small, high-value BDD-style test-only patch for the active target.
4. Run `./tools/autoresearch/testing/scripts/run_candidate.sh {{TARGET}}`.
5. Read:
   - `tools/autoresearch/testing/reports/candidates/{{TARGET}}.cover.txt`
   - `tools/autoresearch/testing/reports/candidates/{{TARGET}}.focus.txt`
   - `tools/autoresearch/testing/reports/candidates/{{TARGET}}.summary.txt`
6. Score the candidate using `program-testcov.md` and the review guidance in `test-review.md`.
7. Append one row to `tools/autoresearch/testing/results.tsv` with:
   - current short HEAD, append `+dirty` if uncommitted kept changes exist
   - target key
   - package name
   - score
   - `keep` or `discard`
   - one given/when/then scenario summary
   - supporting evidence from the reports or gate output
   - a short description of the assertions or regression being protected
8. Keep the candidate only if it is test-only, non-trivial, and protects a meaningful behavior or regression risk.
9. If the candidate should be discarded, revert only the test-file edits from the current attempt and confirm unrelated files remain untouched.
10. After every `{{MEDIUM_GATE_EVERY}}` kept candidates, run `./tools/autoresearch/testing/scripts/medium_gate.sh`.

Do not stop to ask for permission between attempts. Stay on the current target for this run.
