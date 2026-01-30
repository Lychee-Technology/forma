# Go Coding Standards for This Project

## Auto-Applied Rules

When writing or modifying Go code in this project, ALWAYS follow these rules:

### 1. File & Function Size Limits
- REJECT any changes that result in:
  - Souce cod files >500 lines (excluding generated code)
  - Functions/methods >100 lines
- If a change would exceed limits, **automatically suggest refactoring** instead

### 2. Code Quality Gates (Auto-Check Before Implementation)
Before implementing any feature/fix, check:
- [ ] Will this create code duplication? (>15 similar lines)
- [ ] Does this function do multiple unrelated things? (SRP violation)
- [ ] Are variable/function names clear and descriptive?
- [ ] Is error handling present with proper context?

### 3. Mandatory Patterns

#### Error Handling
```go
// DON'T
if err != nil {
    return err
}

// DO
if err != nil {
    return fmt.Errorf("failed to process user %s: %w", userID, err)
}
```

#### Function Structure
```go
// Early returns, flat structure
func ProcessOrder(order Order) error {
    if err := order.Validate(); err != nil {
        return fmt.Errorf("invalid order: %w", err)
    }
    
    if order.Total == 0 {
        return ErrEmptyOrder
    }
    
    // main logic here
    return nil
}
```

### 4. Naming Conventions
- **Packages**: lowercase, single word (e.g., `user`, `auth`, not `userService`)
- **Interfaces**: `<Noun>er` pattern (e.g., `Reader`, `Handler`) or `<Noun>Service`
- **Exported types**: Clear, descriptive (e.g., `UserRepository`, not `UsrRepo`)
- **Private helpers**: verb-first (e.g., `buildQuery`, `validateInput`)

### 5. Project Structure (Layer Separation)
```
internal/
├── domain/          # Business entities, interfaces
├── handler/         # RESTFul handlers (thin entrypoints)
├── service/         # Business logic
├── repository/      # Data access
└── pkg/             # Shared utilities
```

**Rule**: Handlers should be <50 lines. If longer, extract to service layer.

### 6. When Adding New Code

**Before writing**, ask yourself:
1. Does this belong in a new file/package? (cohesion)
2. Can I extract common logic? (DRY)
3. Is this testable? (dependency injection)

**After writing**, verify:
1. Run `go fmt`, `go test ./...`
2. Check function/file line counts
3. Add godoc comments for exported items

### 7. Refactoring Triggers (Auto-Suggest)

If you detect these patterns, **proactively suggest refactoring**:
- Function >80 lines (approaching limit)
- Copy-pasted code blocks
- Deeply nested conditions (>3 levels)
- Missing error handling
- Global variables for application state

---

## Incremental Improvement Mode

When modifying existing code that doesn't meet standards:
- Fix the immediate area you're working on
- Suggest follow-up refactoring for the whole file
- Don't rewrite unrelated code without asking

