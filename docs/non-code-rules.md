# Non-code artifacts

Issues, PR descriptions, design docs, specs, plans, research notes, assessments, and reviews must be useful to a human who did not share the agent's working context.

## Write for the reader

The reader can inspect the code and diff. Use the artifact for the context and judgment those don't show, so an unfamiliar reviewer can build the right mental model without reconstructing prompts, tool calls, or implementation history. Cover whichever of these materially matter; don't fill them in mechanically:

- **Why:** What problem or constraint triggered this work, and why is it worth doing?
- **Outcome:** What behavior, decision, or capability should change?
- **Judgment:** What non-obvious choices or trade-offs were made, and why?
- **Risk and uncertainty:** What could go wrong or remains uncertain? Is anything urgent, sensitive, or hard to reverse?
- **Review focus:** What deserves attention, and what feedback or decision is needed?

## Record reasoning, not a narrated diff

Don't enumerate files changed, functions renamed, tests added, or code left untouched. Include an implementation detail only when it explains behavior, a design decision, compatibility, risk, validation, or something the reviewer should inspect. Prefer

> The retry policy stays at the transport boundary because moving it into callers would duplicate backoff behavior and make cancellation semantics inconsistent.

over

> Updated the transport layer, modified callers, added retry handling, and added tests.

Record a rationale or rejected alternative when it clarifies a trade-off; don't manufacture one to look complete. Distinguish facts, conclusions, assumptions, and open questions. Never invent measurements, requirements, consensus, user impact, motivations, or decisions; if context is missing, say what is unknown.

## Preserve the actual shape of the work

- Don't recast finished implementation as a proposal. A document written after the fact says so and doesn't imply its reconstructed reasoning came first.
- Keep uncertainty, disagreement, and changes of direction that affect how the result should be understood; don't smooth them into a clean narrative.
- When editing human-written text, keep their terminology, opinions, emphasis, and voice. Fix errors and simplify, but don't replace distinctive writing with generic prose.

## Keep the signal high

Every paragraph, bullet, and heading must earn its place.

- Prefer concrete facts, examples, numbers, and consequences over abstract claims, and short direct sentences over long ones.
- Use paragraphs for reasoning; use bullets only for genuinely parallel items.
- Cut repetition, throat-clearing, generic transitions, and summaries of what is already short and clear.
- No promotional language, canned enthusiasm, or stock phrases ("this comprehensive approach", "ensures a seamless experience", "overall, these changes improve...") unless they state a specific, supported fact.

Concise is not compressed: keep the context, evidence, caveats, and trade-offs the reader needs.

## By artifact type

- **Issue:** problem, evidence, impact, constraints, desired outcome, unknowns. Don't prescribe implementation unless it is the decision under discussion.
- **Design doc or spec:** problem and constraints, alternatives, the decision and why, trade-offs, migration or operational risks, open questions. Help people reason about the decision, not describe a finished implementation.
- **PR description:** why the change exists, meaningful behavior change, key implementation decisions, risk, validation, review focus.
- **Plan:** outcome, dependencies and sequencing, decision points, risks, validation. Don't expand a small task into a long checklist.
- **Research notes or assessment:** the question, evidence examined, the supported conclusion, counter-evidence or limitations, remaining uncertainty.

## Before publishing

Reread once as a reader who was not in the session. Can they tell why this exists and what matters? Is every implementation detail useful for a decision or risk call, every judgment backed by evidence or a stated constraint, and every uncertainty visible? Remove any sentence, bullet, or heading that can go without losing information. Don't mention this review in the artifact.

## Publishing

Every artifact must end up on GitHub; a copy on disk doesn't count. Issues, PR descriptions, comments, and reviews are written in English. A document committed under `docs/` may be written in Chinese with a `.cn.md` suffix; that file is the canonical version.

- An artifact that already lives on GitHub isn't repeated in a comment: an issue or PR description is the body of that issue or PR, and a file committed in the PR (such as a `features/` spec) is in the diff.
- Post any other artifact in full, not as a summary or file path, as a comment on the relevant PR if the work is under review, otherwise on the relevant issue (create one if none exists).
- Say in the comment which artifact it is (plan, spec, review, …) so a later reader knows what they are looking at.
- A gitignored local working copy is fine, but don't force-add it to share it. If one exists, give its path in the comment.
- Durable conventions (ADRs, runbooks, reference pages) still belong in `docs/`. The issue or PR records the discussion; `docs/` records the decision.
