---
description: description: Prepare the current work or reasoning for independent challenge
---

Create `.review/handoff.md`.

Include:

## Task
What was asked.

## Outcome
What you currently recommend or implemented.

## Evidence
Sources, measurements, commands, or inspected data supporting the outcome.

## Assumptions
Anything that has not been verified.

## Decisions
Important choices made and alternatives considered.

## Files changed
If implementation occurred.

## Verification
Checks actually performed.

## Open questions
Anything still uncertain.

## Challenge focus
What an independent senior engineer should challenge most aggressively.

## Fix requirement
Fixes for runtime / platform incompatabilities must include a `Verified against:` line with the relevant official source.

Do not hide uncertainty.
Do not present hypotheses as facts.

If the preceding work was research or design, emphasize:
- recommendation
- evidence
- alternatives
- assumptions
- unresolved questions

If the preceding work was implementation, emphasize:
- intended behavior
- architectural decisions
- changed files
- tests and verification
- known limitations

If no files were changed, omit implementation-specific sections.