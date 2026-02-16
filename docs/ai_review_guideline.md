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
- Refactoring Guru (Couplers): https://refactoring.guru/refactoring/smells/couplers
- SOLID Go Design (Dave Cheney): https://dave.cheney.net/2016/08/20/solid-go-design
- Effective Go (community checklist): https://github.com/pthethanh/effective-go
- Twelve Go Best Practices (slides): https://go.dev/talks/2013/bestpractices.slide

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
- Prefer “handle errors first” to minimize nesting.
- Flag in-band error signaling (`""`, `-1`, `nil` as hidden failure) when `(..., ok)` or `(..., error)` is clearer.
- Check error text quality: lowercase start (except acronyms/proper nouns), no trailing punctuation.
- Prefer modern wrapping (`fmt.Errorf("...: %w", err)`) so callers can still use `errors.Is/As`.

### 3.2 API and interface design
- Keep interfaces small and use-case-driven.
- Flag “fat interfaces” that force broad dependencies.
- Check whether function signatures include repeated parameter clumps.
- Ensure constructor and function signatures match actual behavior (no ignored injected dependencies).
- Ask only for what is needed in arguments (e.g., `io.Writer` instead of broader/concrete types).
- Prefer synchronous APIs at boundaries; let callers choose concurrency.
- Apply contract minimalism: “require no more, promise no less.”
- Prefer `accept interfaces, return structs` for extensibility and testability.
- Be cautious with constructors returning interfaces by default; return interface only when abstraction boundary is intentional.

### 3.3 Naming and readability
- Enforce consistent naming for initialisms (`ID`, `URL`, `SQL`, `HTTP`).
- Prefer short, scoped variables; avoid leaking wide mutable state.
- Ensure comments explain intent/constraints, not obvious mechanics.
- Avoid stutter across package and symbol names (`json.Encoder`, not `json.JSONEncoder`).
- Flag low-signal package names (`util`, `common`, `misc`, `types`, `interfaces`) when they hide ownership.

### 3.4 Package organization and documentation
- Important code goes first (package docs/imports/core types), helpers later.
- For multi-file packages, verify split improves discoverability (not random scattering).
- Package-level docs and exported symbol docs should be complete sentences and godoc-friendly.
- Prefer packages with a single, coherent purpose; avoid catch-all package designs.
- Explicitly flag low-signal package names like `common`, `utils`, `server`, `private` when they hide multiple responsibilities.

### 3.5 Initialization, dependencies, and zero-value behavior
- Avoid heavy side effects inside `init()` (network, db, remote auth) when explicit bootstrap can fail gracefully.
- Prefer types with useful zero values where feasible.
- Minimize package globals; prefer dependency injection for testability.

### 3.6 Concurrency and resource safety
- Verify context propagation and cancellation handling.
- Check lock usage patterns and lock release guarantees.
- Check `defer` ordering and resource lifecycle (`Close`, `Rollback`, cleanup).
- Check goroutine lifetimes and potential leaks.
- Require explicit goroutine termination strategy (`context`, `quit` channel, bounded worker lifecycle).
- Watch for channel operations that can block forever; consider buffered channels or cancellation paths.

### 3.7 Testing quality
- Tests must fail with helpful diagnostics (inputs, expected, actual), not opaque helper-only assertions.
- Ensure tests validate behavior, not implementation details only.
- Check concurrency tests for deterministic shutdown and no goroutine leaks.

### 3.8 Formatting and idiomatic structure
- Assume `gofmt` compliance.
- Flag deeply nested branches that should be decomposed.
- Flag methods that hide multiple responsibilities.

### 3.9 Tooling gates (review-level checks)
- Verify codebase expectations for `gofmt`/`goimports`.
- Verify `go vet` findings are addressed or justified.

### 3.10 Coupling/Cohesion and Import Graph (SOLID Go)
- Use objective design language in findings: `rigid`, `fragile`, `immobile`, `complex`, `verbose`.
- Check package cohesion first (not just type-level SRP): do functions/types in a package change for one reason?
- Check coupling via imports: each `import` is a source-level dependency.
- Prefer a wide, relatively flat, acyclic import graph over tall/narrow dependency stacks.
- Push concrete wiring/details upward (typically to `main` or top-level handlers); keep lower layers dependency-light and interface-oriented.

---

## 4) Refactoring.Guru Smell-to-Go Mapping

Use smells as a risk taxonomy, not as a cosmetic checklist.

### 4.1 Bloaters
- `Long Method`: large(more than 100 lines) handlers/flows doing parse + validate + execute + map response in one function.
- `Large Class`: one type handles CRUD, query, orchestration, and transformation.
- `Long Parameter List`: repeated `(ctx, cfg, db, client, logger, ...)` signatures.
- `Data Clumps`: recurring argument groups suggest a context object.
- `Primitive Obsession`: repeated `string`-encoded operators/flags instead of typed abstractions.

### 4.2 Change Preventers
- `Divergent Change`: one module changes for many unrelated reasons.
- `Shotgun Surgery`: one behavior update requires edits across many files.
- Cross-check with package independence: avoid tight coupling between otherwise independent packages.
- If a change tends to produce `rigid/fragile/immobile` behavior, raise severity.

### 4.3 Dispensables
- `Duplicate Code`: mirrored handlers, repeated SQL/operator parsing, repeated conversion switches.
- `Speculative Generality`: interfaces/params that are never used.

### 4.4 Object-Orientation Abusers (adapted for Go)
- `Switch Statements`: repeated type/operator switches across modules that should be centralized.
- Repeated concurrency orchestration in public APIs: prefer central sync contract + internal concurrency.
- Over-reliance on inheritance-style mental model with embedding should be flagged; embedding is composition, not subtype polymorphism.

### 4.5 Couplers Deep-Dive (from refactoring.guru/couplers)

#### Feature Envy
- Definition: a method accesses another object’s data more than its own.
- Go indicators:
  - a function in package/type `A` repeatedly reads fields/getters from `B` and barely touches `A`.
  - “mapper/service” methods that mostly navigate foreign structs.
- Risk:
  - wrong ownership and change coupling; behavior changes in `B` force edits in `A`.
- Preferred refactors:
  - `Move Method`
  - `Extract Method` then move extracted part
- Review question:
  - “Which type/package changes when this logic changes? Is the logic currently located there?”
- When to ignore:
  - deliberate behavior/data separation (e.g., Strategy-like design for pluggable behavior).

#### Inappropriate Intimacy
- Definition: one class/module relies on internals of another.
- Go indicators:
  - direct usage of another package’s internal representation assumptions.
  - friend-like knowledge of lifecycle/order/invariants not encoded in API.
  - persistent two-way knowledge between packages/types.
- Risk:
  - high coupling, fragile internals, hard independent evolution.
- Preferred refactors:
  - `Move Method` / `Move Field`
  - `Extract Class`
  - `Hide Delegate`
  - change bidirectional dependency to unidirectional
- Review question:
  - “Is this caller using stable contract, or undocumented internals?”

#### Message Chains
- Definition: chained navigation (`a.B().C().D()` style).
- Go indicators:
  - long accessor chains across nested structs/interfaces.
  - callers repeatedly walk object graphs to reach one behavior point.
- Risk:
  - brittle code; any shape change in the chain breaks callers.
- Preferred refactors:
  - `Hide Delegate`
  - `Extract Method` + `Move Method` to the start of chain
- Review question:
  - “Can the first object expose intention-level behavior instead of structure traversal?”
- When to ignore:
  - avoid over-hiding delegates if it creates `Middle Man`.

#### Middle Man
- Definition: type exists mainly to delegate calls.
- Go indicators:
  - wrapper methods that forward almost 1:1 with no policy/validation/coordination value.
  - façade layer with no abstraction benefit and high churn.
- Risk:
  - unnecessary indirection and edit overhead.
- Preferred refactor:
  - `Remove Middle Man` where no boundary value exists.
- Review question:
  - “What concrete boundary value does this layer add (policy, security, caching, observability, compatibility)?”
- When to ignore:
  - intentional middle layers for dependency isolation, `Proxy`/`Decorator`, compatibility boundaries.

#### Incomplete Library Class
- Definition: library cannot be changed but lacks needed behavior.
- Go indicators:
  - repeated ad-hoc helpers around third-party types scattered in many files.
  - copy-paste adapters for same missing operation.
- Risk:
  - duplication and inconsistent semantics around one dependency.
- Preferred refactors:
  - `Introduce Foreign Method` (small extension)
  - `Introduce Local Extension` (larger wrapper/adapter)
- Review question:
  - “Should missing behavior be centralized as a local extension instead of repeated call-site patches?”
- Caution:
  - local extensions increase maintenance work when upstream libraries change.

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
11. Public API forces async/channel usage where a sync return would be simpler.
12. Potential goroutine leak: goroutine sends/receives without cancellation/exit contract.
13. Package-level mutable global used as hidden dependency.
14. Functions return ambiguous in-band “error values” instead of explicit status/error.
15. Repeated deep accessor chains (`a.b.c.d`) in business logic.
16. Thin pass-through wrappers with little/no policy value.
17. Scattered third-party library workarounds without a local extension layer.
18. Package name is generic (`common/utils/server/private`) and accumulates unrelated changes.
19. Import graph appears tall/narrow; low-level packages pull many concrete dependencies.
20. Lower-layer package knows runtime wiring details that should live in top-level composition code.

---

## 6) Refactor Recommendation Rules

When suggesting refactors, follow these rules:
1. Prefer incremental refactors over rewrites.
2. Preserve external behavior unless explicitly required.
3. Suggest extraction boundaries that align with responsibilities.
4. Pair each refactor with test updates.
5. State migration risk (API change, data shape, query behavior, performance).
6. Prefer converting async boundary APIs to sync forms unless async is a hard requirement.

Suggested patterns:
- Extract Method
- Introduce Parameter Object
- Extract Class / split service
- Consolidate Duplicate Conditional Fragments
- Centralize parser/codec/mapping logic
- Replace panic path with error-returning path (except startup invariants)
- Replace in-band error signaling with explicit `(value, ok)` or `(value, error)`
- Replace hidden globals with explicit dependencies
- Push dependency wiring to composition root (`main`/handler bootstrap)
- Replace concrete parameter dependency with narrow behavior interface where it improves reuse/tests

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
- require informative failure messages with input + expected + actual where relevant.

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
- couplers: feature envy, inappropriate intimacy, message chains, middle man, incomplete library class
- panic/init misuse
- error swallowing and missing context
- in-band error signaling and weak error message conventions
- fat interfaces and contract mismatches
- inconsistent naming of initialisms (SQL/ID/URL/etc.)
- async-first API design where sync API is cleaner
- goroutine lifecycle/leak risks
- hidden global dependencies and package coupling
- package cohesion and dependency shape (`rigid/fragile/immobile` signals)
- import graph quality (acyclic, wide/flat) and concrete dependency placement
- embedding misuse as pseudo-inheritance
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

---

## 11) Source-Aligned Review Questions

Run these questions explicitly during review:
1. Did we handle errors first to keep happy path flat and obvious?
2. Are any failures only logged but never returned?
3. Is any non-startup code using `panic` where `error` should be returned?
4. Does this API ask only for what it needs (small interface, minimal args)?
5. Is concurrency an implementation detail, or is it leaking into API design?
6. Can any goroutine block forever due to missing cancellation or receiver?
7. Are package names and symbols clear, non-stuttering, and initialism-consistent?
8. Do package globals create hidden coupling/test fragility?
9. Do tests explain failures clearly enough to debug quickly?
10. Is this smell a real risk (correctness/maintainability), or just style preference?
11. Are package responsibilities cohesive, or is this becoming a dumping-ground package?
12. Does the import graph trend toward wide/flat decoupling, or tall/narrow dependency chains?
13. Should this concrete dependency be pushed up to `main` and replaced by a narrow interface here?
