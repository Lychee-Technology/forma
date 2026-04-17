Review the candidate test patch as a strict maintainer.

Check:
- does it capture a real scenario, rule, or failure mode?
- is the scenario clear in given/when/then terms?
- is it deterministic?
- is it low-noise and reviewable?
- does it assert observable behavior instead of implementation details?
- is it better to extend an existing test instead?

Return:
- `keep` or `discard`
- one-sentence reason
- any flake or triviality concerns
