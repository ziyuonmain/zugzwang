---
description: Perform an independent senior engineering review of current changes
---

Act as @reviewer.

Inspect the current git diff and directly related code.

Use the architecture-review skill where relevant.

Prioritize:
1. correctness
2. data semantics
3. architecture
4. tests
5. Databricks-specific issues
6. maintainability

Ignore superficial formatting unless it causes a real problem.

Do not modify files.

Return findings ordered by severity.