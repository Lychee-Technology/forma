## Summary

Capture benchmark baselines and codify CI execution policy for optimization work.

## Why

Autoresearch and manual tuning both need a clear answer to which benchmark subsets are safe for routine execution and which ones should be reserved for slower review environments. The repository also needs saved baseline guidance that matches the live benchmark path.

## Scope

- capture initial `small` and `medium` benchmark baselines with the richer artifact model
- define fast, medium, and heavy benchmark subsets for local, CI, and manual use
- document runtime envelopes and environment expectations for each subset
- align repository automation with the benchmark's live and validation execution modes

## Acceptance Criteria

- the repository documents stable baseline capture procedures for `small` and `medium`
- CI-safe and manual-only benchmark subsets are clearly separated
- benchmark commands used later by autoresearch are documented and stable enough for repeated use

## References

- `docs/federated-query/federated-query-benchmark-baseline-runbook.md`
- `docs/federated-query/federated-query-benchmark-ci-and-ops-guide.md`
- `Makefile`
- `.github/workflows/ci.yml`
