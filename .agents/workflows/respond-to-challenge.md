---
description: Evaluate and respond to a persisted independent challenge.
---

Read the handoff and challenge artifacts identified by the user.

When the user does not identify them, default to:

- `.review/handoff.md`
- `.review/challenge.md`

Also read:

- applicable project rules and skills
- directly related repository files

Do not modify code or overwrite the handoff or challenge unless the user
explicitly requests implementation.

## Procedure

1. Preserve the meaning of every actionable finding.
2. Classify each finding:
   - Accept
   - Accept with modification
   - Reject
   - Defer
3. Support each disposition with repository evidence or primary sources.
4. State the correction required for accepted findings.
5. State the consequence and resolution condition for deferred findings.
6. Check that the resulting recommendation is internally consistent.
7. Write the response to the path requested by the user, or default to
   `.review/challenge-response.md`.

## Rules

- Do not silently omit or weaken findings.
- Do not accept or reject findings based on authority alone.
- Do not claim implementation or verification that did not occur.
- Do not invent railway semantics or Databricks behavior.
- Use `Verified against:` for disputed runtime or platform claims.
- Prefer the smallest correction that resolves the issue.
- Never overwrite an existing review artifact unless explicitly requested.

## Output

Include:

- Summary
- Finding dispositions with evidence and required action
- Revised recommendation
- Remaining uncertainty
- Next step:
  - Ready to revise design
  - Ready to implement
  - Ready for review
  - Blocked pending evidence
  - Blocked pending user decision
