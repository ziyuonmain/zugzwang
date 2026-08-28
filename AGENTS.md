# Zugzwang

Zugzwang is an open-source railway data platform built on Databricks.

Before making non-trivial changes:

1. Read `.agents/rules/project.md`.
2. Read `.agents/rules/project-context.md`.
3. Use relevant skills under `.agents/skills/`.
4. For architectural work, run the `/design` workflow first.
5. Do not invent Deutsche Bahn data semantics.
6. Prefer the smallest implementation that solves the current problem.
7. Run relevant tests and validation before considering work complete.

Important constraints:

- Use real public railway data.
- Keep reusable logic under `src/`.
- Keep notebooks thin.
- Do not commit credentials or generated datasets.
- Do not add technologies solely to showcase them.