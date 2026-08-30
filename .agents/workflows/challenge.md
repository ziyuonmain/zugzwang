---
description: Challenge a technical claim, architecture decision, or proposed fix.
---

## Procedure

1. Identify the claims and assumptions being challenged.
2. Verify disputed technical facts against primary or official sources.
3. Separate:
   - verified facts
   - assumptions
   - hypotheses
4. Look for plausible alternatives.
5. Try to falsify the current recommendation.
6. Compare alternatives by correctness, complexity, maintainability, and fit
   with the existing architecture.
7. Recommend a direction only after the evidence supports it.
8. Do not modify code unless explicitly requested.

## Output

- Claims challenged
- Evidence
- Incorrect or unsupported assumptions
- Alternatives considered
- Recommendation
- Remaining uncertainty

Write the output to the path requested by the user. Otherwise:

- challenge of `.review/handoff.md` -> `.review/challenge.md`
- challenge of another artifact -> `.review/<artifact-stem>-challenge.md`

Never overwrite an existing review artifact unless explicitly requested.
