## Summary

Implement a deterministic benchmark generator with multiple distribution models.

## Why

The benchmark must evaluate hybrid query behavior under realistic data shapes rather than only uniform synthetic samples.

## Scope

- support `uniform`, `zipf`, `temporal`, `partition-skew`, and `hotspot-overlap`
- make generation reproducible by seed
- calibrate selectivity for common filter scenarios
- define scale presets for `small`, `medium`, and `large`
- add generator-level verification helpers

## Acceptance Criteria

- generator output is reproducible for a fixed seed
- distribution-specific datasets can be produced through a public benchmark API
- selectivity calibration helpers exist for high-, medium-, and low-selectivity filters
- small-scale generation is covered by tests

## References

- `docs/cn/federated-query-benchmark-hld.md`
- `docs/federated-query/federated-query-benchmark-implementation-plan.md`
