# ADR 0003: Separate Silver and Gold serving schemas

## Status

Accepted

## Context

The June 2026 vertical slice initially published every materialized view to a
single `silver_gold` schema. This minimized configuration while the pipeline
was being proven, but it obscured the distinction between reusable normalized
datasets and the consumer-facing analytical mart in Unity Catalog.

The immutable upstream files already live in a managed Volume in the `raw`
schema. Creating Bronze Delta tables that merely duplicate those files would
add storage and processing without serving a current requirement.

Lakeflow Declarative Pipelines can publish a dataset outside the configured
default schema by using a fully qualified identifier.

## Decision

- Keep the immutable landing Volume in the `raw` schema. Treat it as the
  Bronze-equivalent boundary without renaming or duplicating its contents.
- Publish normalized materialized views to the `silver` schema.
- Publish consumer-facing analytical datasets to the `gold` schema.
- Let schema names express the medallion layer; do not repeat `silver_` or
  `gold_` in dataset names when the schema already supplies that context. For
  example, publish `silver.train_stops` and `gold.train_stop_weather`.
- Keep one Lakeflow pipeline. Configure `silver` as its default schema and use
  the two-part `gold.train_stop_weather` identifier for the Gold output. The
  pipeline's bundle-configured catalog resolves the environment-specific
  three-part name.

## Consequences

- Catalog browsing, grants, retention policies, and ownership can be managed
  independently by layer.
- Silver-to-Gold lineage remains in one pipeline graph.
- Existing objects in `silver_gold` are not migrated to the new schemas. Their
  lifecycle state must be checked after the first refresh, and any remaining
  objects should be retired only after the replacements have been verified.
- A future Bronze table is justified only when parsing, schema enforcement,
  replay performance, or source history requires a persisted raw Delta model.
