# AI Code Review Guideline (Go)

## Purpose
Use this guideline to run high-signal, engineering-grade code reviews for Go codebases.
The review should combine:
- Go idioms and conventions from Effective Go.
- structural risk detection from refactoring.guru code smells.

The goal is not stylistic perfection. The goal is to find defects, maintenance risks, and refactor opportunities with clear impact.

## Canonical References
- Effective Go: https://go.dev/doc/effective_go
- Refactoring Guru (Code Smells): https://refactoring.guru/refactoring/smells

---

## 1) Review Objectives and Priorities

### Primary objectives
1. Detect correctness and behavior risks.
2. Detect maintainability risks that are likely to cause future defects.
3. Propose targeted, low-regret refactors.
4. Identify missing tests for risky paths.

### Severity model
- `P0` Critical: likely production incident, data loss, security/compliance risk.
- `P1` High: behavior bug, strong regression risk, high-cost maintenance hotspot.
- `P2` Medium: clear design debt with moderate ongoing cost.
- `P3` Low: polish/readability improvement with low risk.

---

## 2) Mandatory Review Output Format

Always output findings first, sorted by severity (`P0` to `P3`).
Each finding must include:
1. `Title`
2. `Severity`
3. `Location` (`path:line`)
4. `Why it matters` (risk/impact)
5. `Evidence` (specific code behavior, not vague style claims)
6. `Suggested refactor` (concrete and scoped)
7. `Test impact` (what to add/update)

If no findings:
- explicitly say `No material findings`.
- still report residual risks and test gaps.

---

## 3) Effective Go Review Checklist

Use this checklist as hard gates.

### 3.1 Error handling and control flow
- Prefer `errors as values`; avoid swallowing errors after logging.
- Check all returned errors from IO/network/db/encode/decode operations.
- Avoid `panic` in library/business layers; reserve for unrecoverable program startup invariants.
- Ensure error messages include actionable context.
- Prefer early return; avoid unnecessary `else` after `return`.

### 3.2 API and interface design
- Keep interfaces small and use-case-driven.
- Flag “fat interfaces” that force broad dependencies.
- Check whether function signatures include repeated parameter clumps.
- Ensure constructor and function signatures match actual behavior (no ignored injected dependencies).

### 3.3 Naming and readability
- Enforce consistent naming for initialisms (`ID`, `URL`, `SQL`, `HTTP`).
- Prefer short, scoped variables; avoid leaking wide mutable state.
- Ensure comments explain intent/constraints, not obvious mechanics.

### 3.4 Initialization and zero-value behavior
- Avoid heavy side effects inside `init()` (network, db, remote auth) when explicit bootstrap can fail gracefully.
- Prefer types with useful zero values where feasible.

### 3.5 Concurrency and resource safety
- Verify context propagation and cancellation handling.
- Check lock usage patterns and lock release guarantees.
- Check `defer` ordering and resource lifecycle (`Close`, `Rollback`, cleanup).
- Check goroutine lifetimes and potential leaks.

### 3.6 Formatting and idiomatic structure
- Assume `gofmt` compliance.
- Flag deeply nested branches that should be decomposed.
- Flag methods that hide multiple responsibilities.

---

## 4) Refactoring.Guru Smell-to-Go Mapping

Use smells as a risk taxonomy, not as a cosmetic checklist.

### 4.1 Bloaters
- `Long Method`: large handlers/flows doing parse + validate + execute + map response in one function.
- `Large Class`: one type handles CRUD, query, orchestration, and transformation.
- `Long Parameter List`: repeated `(ctx, cfg, db, client, logger, ...)` signatures.
- `Data Clumps`: recurring argument groups suggest a context object.
- `Primitive Obsession`: repeated `string`-encoded operators/flags instead of typed abstractions.

### 4.2 Change Preventers
- `Divergent Change`: one module changes for many unrelated reasons.
- `Shotgun Surgery`: one behavior update requires edits across many files.

### 4.3 Dispensables
- `Duplicate Code`: mirrored handlers, repeated SQL/operator parsing, repeated conversion switches.
- `Speculative Generality`: interfaces/params that are never used.

### 4.4 Object-Orientation Abusers (adapted for Go)
- `Switch Statements`: repeated type/operator switches across modules that should be centralized.

---

## 5) High-Signal Heuristics for Go Reviews

Use these quick detectors:
1. Same business logic appears in multiple entrypoints (`server` vs `lambda`).
2. Error is logged but not returned or aggregated.
3. Constructor accepts dependency but discards it.
4. Multiple files parse the same DSL/operator format independently.
5. Same conversion logic exists in more than one package.
6. SQL string assembly duplicated with minor variants.
7. Public API behavior differs from comment/contract.
8. Any pointer branch dereferences without nil guard.
9. `if err != nil { return ... } else { ... }` repeated heavily.
10. Large orchestrator function with chunked and non-chunked duplicate branches.

---

## 6) Refactor Recommendation Rules

When suggesting refactors, follow these rules:
1. Prefer incremental refactors over rewrites.
2. Preserve external behavior unless explicitly required.
3. Suggest extraction boundaries that align with responsibilities.
4. Pair each refactor with test updates.
5. State migration risk (API change, data shape, query behavior, performance).

Suggested patterns:
- Extract Method
- Introduce Parameter Object
- Extract Class / split service
- Consolidate Duplicate Conditional Fragments
- Centralize parser/codec/mapping logic
- Replace panic path with error-returning path (except startup invariants)

---

## 7) Test Expectations in Review

For each `P0/P1` finding, require tests covering:
1. success path
2. failure path
3. edge case
4. regression case tied to the bug/risk

For refactors:
- require behavior-preserving tests before major movement.
- verify contract tests for public interfaces/endpoints.

---

## 8) AI Reviewer Prompt Template

Use this template directly:

```text
You are a senior Go code reviewer focused on software quality.
Review the provided diff/files using:
1) Effective Go principles (https://go.dev/doc/effective_go)
2) Refactoring.Guru code smells (https://refactoring.guru/refactoring/smells)

Review goals:
- prioritize correctness, reliability, maintainability, and testability
- identify concrete risks, not style-only nits

Output requirements:
- findings first, sorted by severity (P0, P1, P2, P3)
- each finding must include:
  - title
  - severity
  - file:line
  - why it matters
  - concrete evidence
  - suggested refactor
  - required tests
- if no material findings, explicitly say so and list residual risks/test gaps

Focus especially on:
- duplicate code
- long methods/classes
- long parameter lists/data clumps
- divergent change/shotgun surgery
- panic/init misuse
- error swallowing and missing context
- fat interfaces and contract mismatches
- inconsistent naming of initialisms (SQL/ID/URL/etc.)
```

---

## 9) What Not to Do

- Do not produce generic comments without file-level evidence.
- Do not block on purely personal style preferences.
- Do not recommend broad rewrites when extraction can solve the issue.
- Do not ignore test impact.
- Do not bury critical findings under long summaries.

---

## 10) Review Completion Checklist

Before finishing, verify:
1. Findings are evidence-based and severity-ranked.
2. High-risk items include concrete refactor and tests.
3. Effective Go and code smell checks were both applied.
4. Output is concise, actionable, and directly executable by engineers.
