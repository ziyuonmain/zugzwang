---
trigger: always_on
---

# Gleiszeit Project Rules

## Purpose

Gleiszeit is an open-source rail data platform built around real public
Deutsche Bahn data.

The project has two goals:

1. Explore interesting questions about German railway operations.
2. Demonstrate production-minded data engineering and solution architecture
   on Databricks.

The project must remain a believable open-source side project. Do not add
Databricks features solely to create a technology showcase.

## Scope

This is a small solo project with an approximate initial implementation
budget of 30 focused development hours.

Prefer a small, coherent working system over broad feature coverage.

Avoid premature additions such as:
- machine learning
- Kafka
- Terraform
- streaming infrastructure
- multiple dashboards
- unnecessary abstraction layers

unless an actual project requirement justifies them.

## Architecture principles

- Use real publicly available data. Do not generate synthetic railway data
  except for minimal unit-test fixtures.
- Preserve raw upstream data where practical.
- Separate ingestion, normalization, and analytical models.
- Make transformations deterministic and testable.
- Treat upstream schema changes and incomplete data as expected conditions.
- Prefer incremental processing where it provides real value.
- Avoid unnecessary medallion layers or tables.
- Explain important architectural decisions in docs/decisions/.

## Databricks

Databricks is the primary execution and analytical platform.

Prefer:
- PySpark for distributed transformations
- Delta tables for persisted analytical datasets
- Unity Catalog compatible resources
- Databricks Declarative Automation Bundles for deployment
- Databricks SQL / AI/BI for consumption where appropriate

Do not use Databricks-specific functionality when a simpler local solution
is clearly better for development or testing.

## Python

- Use Python 3.12 where supported.
- Manage the project with uv.
- Put reusable code under src/.
- Keep notebooks thin; business logic belongs in Python modules.
- Use type hints.
- Use single quotes for Python strings where practical.
- Functions and classes should have brief Google-style docstrings.
- Prefer straightforward functions over unnecessary class hierarchies.

## Quality

Before considering work complete:

- run formatting/linting
- run relevant tests
- verify changed Databricks bundle configuration where applicable
- avoid committing credentials, generated datasets, or workspace state

## AI-assisted development

Do not generate large implementations from vague requirements.

Before implementing non-trivial functionality:
1. inspect the relevant existing code and data;
2. state important assumptions;
3. propose the smallest appropriate design;
4. implement only after the design is understood.

When data semantics are uncertain, inspect actual source records rather than
guessing.

Do not invent Deutsche Bahn field semantics or Databricks behavior.
Check primary documentation or source data when necessary.

## Research and evidence

Research may contain hypotheses, assumptions, and tentative conclusions.

Committed project documentation must contain only verified facts. Before
writing factual claims into `docs/`, verify them against primary sources,
official documentation, or inspected source data.

Clearly label unresolved assumptions and open questions instead of presenting
them as facts.

## Engineering judgment

When several approaches work, prefer the one that:
1. is easiest to explain;
2. has the fewest moving parts;
3. remains extensible if the project grows;
4. resembles something that could reasonably be operated in production.

Record consequential decisions in docs/decisions/.

## Credentials

Never read, display, modify, or commit:
- ~/.databrickscfg
- .env
- Databricks OAuth tokens
- PATs
- GitHub credentials
- cloud credentials

Commands may use credentials indirectly through official CLI authentication.