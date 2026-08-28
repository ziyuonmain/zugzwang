---
description: description: Verify the current implementation before declaring the task complete
---

Before verifying:

- Apply relevant workspace skills automatically.
- Read applicable project rules.
- Confirm with accepted ADRs and existing architecture.

Inspect the current git diff.

Determine which checks are relevant and run them.

At minimum consider:

- `uv run ruff check .`
- `uv run pytest`
- `databricks bundle validate` when bundle resources changed

For data transformations:
- verify expected input/output grain;
- check null/key assumptions;
- inspect representative records where appropriate.

Report:

## Verified
- commands/checks that passed

## Not verified
- anything that could not be tested

## Risks
- remaining correctness or data-semantic concerns

Do not claim something is verified unless the corresponding check was run.